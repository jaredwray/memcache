import { Hookified } from "hookified";
import { MemcacheNode } from "./node.js";
import type {
	ClusterConfig,
	DiscoveredNode,
	SASLCredentials,
} from "./types.js";

export interface AutoDiscoveryOptions {
	configEndpoint: string;
	pollingInterval: number;
	useLegacyCommand: boolean;
	timeout: number;
	keepAlive: boolean;
	keepAliveDelay: number;
	sasl?: SASLCredentials;
}

/**
 * Handles AWS ElastiCache Auto Discovery for memcache clusters.
 * Connects to a configuration endpoint, periodically polls for cluster
 * topology changes, and emits events when nodes are added or removed.
 */
export class AutoDiscovery extends Hookified {
	private _configEndpoint: string;
	private _pollingInterval: number;
	private _useLegacyCommand: boolean;
	private _configVersion = -1;
	private _pollingTimer: ReturnType<typeof setInterval> | undefined;
	private _configNode: MemcacheNode | undefined;
	private _timeout: number;
	private _keepAlive: boolean;
	private _keepAliveDelay: number;
	private _sasl: SASLCredentials | undefined;
	private _isRunning = false;
	private _isPolling = false;

	constructor(options: AutoDiscoveryOptions) {
		super();
		this._configEndpoint = options.configEndpoint;
		this._pollingInterval = options.pollingInterval;
		this._useLegacyCommand = options.useLegacyCommand;
		this._timeout = options.timeout;
		this._keepAlive = options.keepAlive;
		this._keepAliveDelay = options.keepAliveDelay;
		this._sasl = options.sasl;
	}

	/** Current config version. -1 means no config has been fetched yet. */
	public get configVersion(): number {
		return this._configVersion;
	}

	/** Whether auto discovery is currently running. */
	public get isRunning(): boolean {
		return this._isRunning;
	}

	/** The configuration endpoint being used. */
	public get configEndpoint(): string {
		return this._configEndpoint;
	}

	/**
	 * Start the auto discovery process.
	 * Performs an initial discovery, then starts the polling timer.
	 */
	public async start(): Promise<ClusterConfig> {
		if (this._isRunning) {
			throw new Error("Auto discovery is already running");
		}

		this._isRunning = true;

		const configNode = await this.ensureConfigNode();
		const config = await this.fetchConfig(configNode);
		this._configVersion = config.version;
		this.emit("autoDiscover", config);

		this._pollingTimer = setInterval(() => {
			void this.poll();
		}, this._pollingInterval);

		// Don't prevent process exit
		if (
			this._pollingTimer &&
			typeof this._pollingTimer === "object" &&
			"unref" in this._pollingTimer
		) {
			this._pollingTimer.unref();
		}

		return config;
	}

	/**
	 * Stop the auto discovery process.
	 */
	public async stop(): Promise<void> {
		this._isRunning = false;

		if (this._pollingTimer) {
			clearInterval(this._pollingTimer);
			this._pollingTimer = undefined;
		}

		if (this._configNode) {
			await this._configNode.disconnect();
			this._configNode = undefined;
		}
	}

	/**
	 * Perform a single discovery cycle.
	 * Returns the ClusterConfig if the version has changed, or undefined if unchanged.
	 */
	public async discover(): Promise<ClusterConfig | undefined> {
		const configNode = await this.ensureConfigNode();
		const config = await this.fetchConfig(configNode);

		if (config.version === this._configVersion) {
			return undefined;
		}

		this._configVersion = config.version;
		return config;
	}

	/**
	 * Parse the raw response data from a config get cluster command.
	 * The raw data is the value content between the CONFIG/VALUE header and END.
	 * Format: "<version>\n<host1>|<ip1>|<port1> <host2>|<ip2>|<port2>\n"
	 */
	public static parseConfigResponse(rawData: string[]): ClusterConfig {
		if (!rawData || rawData.length === 0) {
			throw new Error("Empty config response");
		}

		const data = rawData.join("");
		const lines = data.split("\n").filter((line) => line.trim().length > 0);

		if (lines.length < 2) {
			throw new Error(
				"Invalid config response: expected version and node list",
			);
		}

		const version = Number.parseInt(lines[0].trim(), 10);
		if (Number.isNaN(version)) {
			throw new Error(`Invalid config version: ${lines[0]}`);
		}

		const nodeEntries = lines[1]
			.trim()
			.split(" ")
			.filter((e) => e.length > 0);
		const nodes: DiscoveredNode[] = nodeEntries.map((entry) =>
			AutoDiscovery.parseNodeEntry(entry),
		);

		return { version, nodes };
	}

	/**
	 * Parse a single node entry in the format "hostname|ip|port".
	 */
	public static parseNodeEntry(entry: string): DiscoveredNode {
		const parts = entry.split("|");
		if (parts.length !== 3) {
			throw new Error(`Invalid node entry format: ${entry}`);
		}

		const hostname = parts[0];
		const ip = parts[1]; // May be empty string
		const port = Number.parseInt(parts[2], 10);

		if (!hostname) {
			throw new Error(`Invalid node entry: missing hostname in ${entry}`);
		}

		if (Number.isNaN(port) || port <= 0 || port > 65535) {
			throw new Error(`Invalid port in node entry: ${entry}`);
		}

		return { hostname, ip, port };
	}

	/**
	 * Build a node ID from a DiscoveredNode.
	 * Prefers IP when available, falls back to hostname.
	 */
	public static nodeId(node: DiscoveredNode): string {
		const host = node.ip || node.hostname;
		const wrappedHost = host.includes(":") ? `[${host}]` : host;
		return `${wrappedHost}:${node.port}`;
	}

	private async ensureConfigNode(): Promise<MemcacheNode> {
		if (this._configNode?.isConnected()) {
			return this._configNode;
		}

		const { host, port } = this.parseEndpoint(this._configEndpoint);

		this._configNode = new MemcacheNode(host, port, {
			timeout: this._timeout,
			keepAlive: this._keepAlive,
			keepAliveDelay: this._keepAliveDelay,
			sasl: this._sasl,
		});

		await this._configNode.connect();
		return this._configNode;
	}

	private async fetchConfig(node: MemcacheNode): Promise<ClusterConfig> {
		if (!node.isConnected()) {
			await node.connect();
		}

		if (this._useLegacyCommand) {
			const result = await node.command("get AmazonElastiCache:cluster", {
				isMultiline: true,
				requestedKeys: ["AmazonElastiCache:cluster"],
			});

			if (!result?.values || result.values.length === 0) {
				throw new Error("No config data received from legacy command");
			}

			return AutoDiscovery.parseConfigResponse(result.values);
		}

		const result = await node.command("config get cluster", {
			isConfig: true,
		});

		if (!result || result.length === 0) {
			throw new Error("No config data received");
		}

		return AutoDiscovery.parseConfigResponse(result);
	}

	private async poll(): Promise<void> {
		if (this._isPolling) {
			return;
		}

		this._isPolling = true;

		try {
			const config = await this.discover();
			if (config) {
				this.emit("autoDiscoverUpdate", config);
			}
		} catch (error) {
			this.emit("autoDiscoverError", error);
			// On connection failure, try to reconnect the config node
			try {
				if (this._configNode && !this._configNode.isConnected()) {
					await this._configNode.reconnect();
				}
			} catch {
				// Reconnect failed; will retry on next poll
			}
		} finally {
			this._isPolling = false;
		}
	}

	private parseEndpoint(endpoint: string): { host: string; port: number } {
		// Handle IPv6 with brackets
		if (endpoint.startsWith("[")) {
			const bracketEnd = endpoint.indexOf("]");
			if (bracketEnd === -1) {
				throw new Error("Invalid IPv6 endpoint: missing closing bracket");
			}

			const host = endpoint.slice(1, bracketEnd);
			const remainder = endpoint.slice(bracketEnd + 1);
			if (remainder === "" || remainder === ":") {
				return { host, port: 11211 };
			}

			if (remainder.startsWith(":")) {
				const port = Number.parseInt(remainder.slice(1), 10);
				return { host, port: Number.isNaN(port) ? 11211 : port };
			}

			return { host, port: 11211 };
		}

		// Standard host:port format
		const colonIndex = endpoint.lastIndexOf(":");
		if (colonIndex === -1) {
			return { host: endpoint, port: 11211 };
		}

		const host = endpoint.slice(0, colonIndex);
		const port = Number.parseInt(endpoint.slice(colonIndex + 1), 10);
		return { host, port: Number.isNaN(port) ? 11211 : port };
	}
}
