import { Hookified } from "hookified";
import { HashRing } from "./ketama";
import { MemcacheNode } from "./node";

export enum MemcacheEvents {
	CONNECT = "connect",
	QUIT = "quit",
	HIT = "hit",
	MISS = "miss",
	ERROR = "error",
	WARN = "warn",
	INFO = "info",
	TIMEOUT = "timeout",
	CLOSE = "close",
}

export interface MemcacheOptions {
	/**
	 * Array of node URIs to add to the consistent hashing ring.
	 * Examples: ["localhost:11211", "memcache://192.168.1.100:11212", "server3:11213"]
	 */
	nodes?: string[];
	/**
	 * The timeout for Memcache operations.
	 * @default 5000
	 */
	timeout?: number;
	/**
	 * Whether to keep the connection alive.
	 * @default true
	 */
	keepAlive?: boolean;
	/**
	 * The delay before the connection is kept alive.
	 * @default 1000
	 */
	keepAliveDelay?: number;
}

export interface MemcacheStats {
	[key: string]: string;
}

export class Memcache extends Hookified {
	private _nodes: Map<string, MemcacheNode> = new Map();
	private _timeout: number;
	private _keepAlive: boolean;
	private _keepAliveDelay: number;
	private _ring: HashRing<string>;

	constructor(options?: MemcacheOptions) {
		super();
		this._timeout = options?.timeout || 5000;
		this._keepAlive = options?.keepAlive !== false;
		this._keepAliveDelay = options?.keepAliveDelay || 1000;
		this._ring = new HashRing<string>();

		// Add nodes to the ring if provided, otherwise add default node
		const nodeUris = options?.nodes || ["localhost:11211"];
		for (const nodeUri of nodeUris) {
			const { host, port } = this.parseUri(nodeUri);
			const nodeKey = port === 0 ? host : `${host}:${port}`;

			// Add to hash ring
			this._ring.addNode(nodeKey);

			// Create node instance
			const node = new MemcacheNode(host, port, {
				timeout: this._timeout,
				keepAlive: this._keepAlive,
				keepAliveDelay: this._keepAliveDelay,
			});

			// Forward node events to Memcache events
			this._forwardNodeEvents(node);

			this._nodes.set(nodeKey, node);
		}
	}

	/**
	 * Get the consistent hashing ring for distributing keys across nodes.
	 * @returns {HashRing<string>}
	 */
	public get ring(): HashRing<string> {
		return this._ring;
	}

	/**
	 * Get the list of nodes in the ring as URI strings (e.g., ["localhost:11211", "127.0.0.1:11212"]).
	 * @returns {string[]} Array of node URI strings
	 */
	public get nodes(): string[] {
		return Array.from(this._ring.nodes.values());
	}

	/**
	 * Get the timeout for Memcache operations.
	 * @returns {number}
	 * @default 5000
	 */
	public get timeout(): number {
		return this._timeout;
	}

	/**
	 * Set the timeout for Memcache operations.
	 * @param {number} value
	 * @default 5000
	 */
	public set timeout(value: number) {
		this._timeout = value;
	}

	/**
	 * Get the keepAlive setting for the Memcache connection.
	 * @returns {boolean}
	 * @default true
	 */
	public get keepAlive(): boolean {
		return this._keepAlive;
	}

	/**
	 * Set the keepAlive setting for the Memcache connection.
	 * @param {boolean} value
	 * @default true
	 */
	public set keepAlive(value: boolean) {
		this._keepAlive = value;
	}

	/**
	 * Get the delay before the connection is kept alive.
	 * @returns {number}
	 * @default 1000
	 */
	public get keepAliveDelay(): number {
		return this._keepAliveDelay;
	}

	/**
	 * Set the delay before the connection is kept alive.
	 * @param {number} value
	 * @default 1000
	 */
	public set keepAliveDelay(value: number) {
		this._keepAliveDelay = value;
	}

	/**
	 * Get a map of all MemcacheNode instances
	 * @returns {Map<string, MemcacheNode>}
	 */
	public getNodes(): Map<string, MemcacheNode> {
		return new Map(this._nodes);
	}

	/**
	 * Get the node responsible for a given key
	 * @param {string} key
	 * @returns {MemcacheNode | undefined}
	 */
	public getNode(key: string): MemcacheNode | undefined {
		const nodeKey = this._ring.getNode(key);
		if (!nodeKey) return undefined;
		return this._nodes.get(nodeKey);
	}

	/**
	 * Add a new node to the cluster
	 * @param {string} uri - Node URI (e.g., "localhost:11212")
	 * @param {number} weight - Optional weight for consistent hashing
	 */
	public async addNode(uri: string, weight?: number): Promise<void> {
		const { host, port } = this.parseUri(uri);
		const nodeKey = port === 0 ? host : `${host}:${port}`;

		if (this._nodes.has(nodeKey)) {
			throw new Error(`Node ${nodeKey} already exists`);
		}

		// Add to ring
		this._ring.addNode(nodeKey, weight);

		// Create and connect node
		const node = new MemcacheNode(host, port, {
			timeout: this._timeout,
			keepAlive: this._keepAlive,
			keepAliveDelay: this._keepAliveDelay,
		});

		this._forwardNodeEvents(node);
		this._nodes.set(nodeKey, node);
	}

	/**
	 * Remove a node from the cluster
	 * @param {string} uri - Node URI (e.g., "localhost:11212")
	 */
	public async removeNode(uri: string): Promise<void> {
		const { host, port } = this.parseUri(uri);
		const nodeKey = port === 0 ? host : `${host}:${port}`;

		const node = this._nodes.get(nodeKey);
		if (!node) return;

		// Disconnect and remove
		await node.disconnect();
		this._nodes.delete(nodeKey);
		this._ring.removeNode(nodeKey);
	}

	/**
	 * Parse a URI string into host and port.
	 * Supports multiple formats:
	 * - Simple: "localhost:11211" or "localhost"
	 * - Protocol: "memcache://localhost:11211", "memcached://localhost:11211", "tcp://localhost:11211"
	 * - IPv6: "[::1]:11211" or "memcache://[2001:db8::1]:11212"
	 * - Unix socket: "/var/run/memcached.sock" or "unix:///var/run/memcached.sock"
	 *
	 * @param {string} uri - URI string
	 * @returns {{ host: string; port: number }} Object containing host and port (port is 0 for Unix sockets)
	 * @throws {Error} If URI format is invalid
	 */
	public parseUri(uri: string): { host: string; port: number } {
		// Handle Unix domain sockets
		if (uri.startsWith("unix://")) {
			return { host: uri.slice(7), port: 0 };
		}
		if (uri.startsWith("/")) {
			return { host: uri, port: 0 };
		}

		// Remove protocol if present
		let cleanUri = uri;
		if (uri.includes("://")) {
			const protocolParts = uri.split("://");
			const protocol = protocolParts[0];
			if (!["memcache", "memcached", "tcp"].includes(protocol)) {
				throw new Error(
					`Invalid protocol '${protocol}'. Supported protocols: memcache://, memcached://, tcp://, unix://`,
				);
			}
			cleanUri = protocolParts[1];
		}

		// Handle IPv6 addresses with brackets [::1]:11211
		if (cleanUri.startsWith("[")) {
			const bracketEnd = cleanUri.indexOf("]");
			if (bracketEnd === -1) {
				throw new Error("Invalid IPv6 format: missing closing bracket");
			}

			const host = cleanUri.slice(1, bracketEnd);
			if (!host) {
				throw new Error("Invalid URI format: host is required");
			}

			// Check if there's a port after the bracket
			const remainder = cleanUri.slice(bracketEnd + 1);
			if (remainder === "") {
				return { host, port: 11211 };
			}
			if (!remainder.startsWith(":")) {
				throw new Error("Invalid IPv6 format: expected ':' after bracket");
			}

			const portStr = remainder.slice(1);
			const port = Number.parseInt(portStr, 10);
			if (Number.isNaN(port) || port <= 0 || port > 65535) {
				throw new Error("Invalid port number");
			}

			return { host, port };
		}

		// Parse host and port for regular format
		const parts = cleanUri.split(":");
		if (parts.length === 0 || parts.length > 2) {
			throw new Error("Invalid URI format");
		}

		const host = parts[0];
		if (!host) {
			throw new Error("Invalid URI format: host is required");
		}

		const port = parts.length === 2 ? Number.parseInt(parts[1], 10) : 11211;
		if (Number.isNaN(port) || port < 0 || port > 65535) {
			throw new Error("Invalid port number");
		}

		// Port 0 is only valid for Unix sockets (already handled above)
		if (port === 0) {
			throw new Error("Invalid port number");
		}

		return { host, port };
	}

	/**
	 * Connect to all Memcache servers or a specific node.
	 * @param {string} nodeId - Optional node ID to connect to (e.g., "localhost:11211")
	 * @returns {Promise<void>}
	 */
	public async connect(nodeId?: string): Promise<void> {
		if (nodeId) {
			const node = this._nodes.get(nodeId);
			if (!node) throw new Error(`Node ${nodeId} not found`);
			await node.connect();
			return;
		}

		// Connect to all nodes
		await Promise.all(
			Array.from(this._nodes.values()).map((node) => node.connect()),
		);
	}

	/**
	 * Get a value from the Memcache server.
	 * @param {string} key
	 * @returns {Promise<string | undefined>}
	 */
	public async get(key: string): Promise<string | undefined> {
		await this.beforeHook("get", { key });

		this.validateKey(key);

		const node = await this._getNodeForKey(key);
		const result = await node.command(`get ${key}`, {
			isMultiline: true,
			requestedKeys: [key],
		});

		let value: string | undefined;
		if (result?.values && result.values.length > 0) {
			value = result.values[0];
		}

		await this.afterHook("get", { key, value });

		return value;
	}

	/**
	 * Get multiple values from the Memcache server.
	 * @param keys {string[]}
	 * @returns {Promise<Map<string, string>>}
	 */
	public async gets(keys: string[]): Promise<Map<string, string>> {
		await this.beforeHook("gets", { keys });

		// Validate all keys
		for (const key of keys) {
			this.validateKey(key);
		}

		// Group keys by node
		const keysByNode = new Map<string, string[]>();

		for (const key of keys) {
			const nodeKey = this._ring.getNode(key);
			if (!nodeKey) {
				throw new Error(`No node available for key: ${key}`);
			}

			if (!keysByNode.has(nodeKey)) {
				keysByNode.set(nodeKey, []);
			}
			// biome-ignore lint/style/noNonNullAssertion: we just set it
			keysByNode.get(nodeKey)!.push(key);
		}

		// Execute commands in parallel across nodes
		const promises = Array.from(keysByNode.entries()).map(
			async ([nodeKey, nodeKeys]) => {
				const node = this._nodes.get(nodeKey);
				if (!node) throw new Error(`Node ${nodeKey} not found`);

				if (!node.isConnected()) await node.connect();

				const keysStr = nodeKeys.join(" ");
				const result = await node.command(`get ${keysStr}`, {
					isMultiline: true,
					requestedKeys: nodeKeys,
				});

				return result;
			},
		);

		const results = await Promise.all(promises);

		// Merge results into Map
		const map = new Map<string, string>();

		for (const result of results) {
			if (result?.foundKeys && result.values) {
				// Map found keys to their values
				for (let i = 0; i < result.foundKeys.length; i++) {
					if (result.values[i] !== undefined) {
						map.set(result.foundKeys[i], result.values[i]);
					}
				}
			}
		}

		await this.afterHook("gets", { keys, values: map });

		return map;
	}

	/**
	 * Check-And-Set: Store a value only if it hasn't been modified since last fetch.
	 * @param key {string}
	 * @param value {string}
	 * @param casToken {string}
	 * @param exptime {number}
	 * @param flags {number}
	 * @returns {Promise<boolean>}
	 */
	public async cas(
		key: string,
		value: string,
		casToken: string,
		exptime: number = 0,
		flags: number = 0,
	): Promise<boolean> {
		await this.beforeHook("cas", { key, value, casToken, exptime, flags });

		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `cas ${key} ${flags} ${exptime} ${bytes} ${casToken}\r\n${valueStr}`;

		const node = await this._getNodeForKey(key);
		const result = await node.command(command);
		const success = result === "STORED";

		await this.afterHook("cas", {
			key,
			value,
			casToken,
			exptime,
			flags,
			success,
		});

		return success;
	}

	/**
	 * Set a value in the Memcache server.
	 * @param key {string}
	 * @param value {string}
	 * @param exptime {number}
	 * @param flags {number}
	 * @returns {Promise<boolean>}
	 */
	public async set(
		key: string,
		value: string,
		exptime: number = 0,
		flags: number = 0,
	): Promise<boolean> {
		await this.beforeHook("set", { key, value, exptime, flags });

		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `set ${key} ${flags} ${exptime} ${bytes}\r\n${valueStr}`;

		const node = await this._getNodeForKey(key);
		const result = await node.command(command);
		const success = result === "STORED";

		await this.afterHook("set", { key, value, exptime, flags, success });

		return success;
	}

	/**
	 * Add a value to the Memcache server (only if key doesn't exist).
	 * @param key {string}
	 * @param value {string}
	 * @param exptime {number}
	 * @param flags {number}
	 * @returns {Promise<boolean>}
	 */
	public async add(
		key: string,
		value: string,
		exptime: number = 0,
		flags: number = 0,
	): Promise<boolean> {
		await this.beforeHook("add", { key, value, exptime, flags });

		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `add ${key} ${flags} ${exptime} ${bytes}\r\n${valueStr}`;

		const node = await this._getNodeForKey(key);
		const result = await node.command(command);
		const success = result === "STORED";

		await this.afterHook("add", { key, value, exptime, flags, success });

		return success;
	}

	/**
	 * Replace a value in the Memcache server (only if key exists).
	 * @param key {string}
	 * @param value {string}
	 * @param exptime {number}
	 * @param flags {number}
	 * @returns {Promise<boolean>}
	 */
	public async replace(
		key: string,
		value: string,
		exptime: number = 0,
		flags: number = 0,
	): Promise<boolean> {
		await this.beforeHook("replace", { key, value, exptime, flags });

		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `replace ${key} ${flags} ${exptime} ${bytes}\r\n${valueStr}`;

		const node = await this._getNodeForKey(key);
		const result = await node.command(command);
		const success = result === "STORED";

		await this.afterHook("replace", { key, value, exptime, flags, success });

		return success;
	}

	/**
	 * Append a value to an existing key in the Memcache server.
	 * @param key {string}
	 * @param value {string}
	 * @returns {Promise<boolean>}
	 */
	public async append(key: string, value: string): Promise<boolean> {
		await this.beforeHook("append", { key, value });

		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `append ${key} 0 0 ${bytes}\r\n${valueStr}`;

		const node = await this._getNodeForKey(key);
		const result = await node.command(command);
		const success = result === "STORED";

		await this.afterHook("append", { key, value, success });

		return success;
	}

	/**
	 * Prepend a value to an existing key in the Memcache server.
	 * @param key {string}
	 * @param value {string}
	 * @returns {Promise<boolean>}
	 */
	public async prepend(key: string, value: string): Promise<boolean> {
		await this.beforeHook("prepend", { key, value });

		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `prepend ${key} 0 0 ${bytes}\r\n${valueStr}`;

		const node = await this._getNodeForKey(key);
		const result = await node.command(command);
		const success = result === "STORED";

		await this.afterHook("prepend", { key, value, success });

		return success;
	}

	/**
	 * Delete a value from the Memcache server.
	 * @param key {string}
	 * @returns {Promise<boolean>}
	 */
	public async delete(key: string): Promise<boolean> {
		await this.beforeHook("delete", { key });

		this.validateKey(key);

		const node = await this._getNodeForKey(key);
		const result = await node.command(`delete ${key}`);
		const success = result === "DELETED";

		await this.afterHook("delete", { key, success });

		return success;
	}

	/**
	 * Increment a value in the Memcache server.
	 * @param key {string}
	 * @param value {number}
	 * @returns {Promise<number | undefined>}
	 */
	public async incr(
		key: string,
		value: number = 1,
	): Promise<number | undefined> {
		await this.beforeHook("incr", { key, value });

		this.validateKey(key);

		const node = await this._getNodeForKey(key);
		const result = await node.command(`incr ${key} ${value}`);
		const newValue = typeof result === "number" ? result : undefined;

		await this.afterHook("incr", { key, value, newValue });

		return newValue;
	}

	/**
	 * Decrement a value in the Memcache server.
	 * @param key {string}
	 * @param value {number}
	 * @returns {Promise<number | undefined>}
	 */
	public async decr(
		key: string,
		value: number = 1,
	): Promise<number | undefined> {
		await this.beforeHook("decr", { key, value });

		this.validateKey(key);

		const node = await this._getNodeForKey(key);
		const result = await node.command(`decr ${key} ${value}`);
		const newValue = typeof result === "number" ? result : undefined;

		await this.afterHook("decr", { key, value, newValue });

		return newValue;
	}

	/**
	 * Touch a value in the Memcache server (update expiration time).
	 * @param key {string}
	 * @param exptime {number}
	 * @returns {Promise<boolean>}
	 */
	public async touch(key: string, exptime: number): Promise<boolean> {
		await this.beforeHook("touch", { key, exptime });

		this.validateKey(key);

		const node = await this._getNodeForKey(key);
		const result = await node.command(`touch ${key} ${exptime}`);
		const success = result === "TOUCHED";

		await this.afterHook("touch", { key, exptime, success });

		return success;
	}

	/**
	 * Flush all values from all Memcache servers.
	 * @param delay {number}
	 * @returns {Promise<boolean>}
	 */
	public async flush(delay?: number): Promise<boolean> {
		let command = "flush_all";

		// If a delay is specified, use the delayed flush command
		if (delay !== undefined) {
			command += ` ${delay}`;
		}

		// Execute on ALL nodes
		const results = await Promise.all(
			Array.from(this._nodes.values()).map(async (node) => {
				if (!node.isConnected()) {
					await node.connect();
				}
				return node.command(command);
			}),
		);

		// All must return OK
		return results.every((r) => r === "OK");
	}

	/**
	 * Get statistics from all Memcache servers.
	 * @param type {string}
	 * @returns {Promise<Map<string, MemcacheStats>>}
	 */
	public async stats(type?: string): Promise<Map<string, MemcacheStats>> {
		const command = type ? `stats ${type}` : "stats";

		// Get stats from ALL nodes
		const results = new Map<string, MemcacheStats>();

		await Promise.all(
			Array.from(this._nodes.entries()).map(async ([nodeKey, node]) => {
				if (!node.isConnected()) {
					await node.connect();
				}

				const stats = await node.command(command, { isStats: true });
				results.set(nodeKey, stats as MemcacheStats);
			}),
		);

		return results;
	}

	/**
	 * Get the Memcache server version from the first available node.
	 * @returns {Promise<string>}
	 */
	public async version(): Promise<string> {
		// Get version from first node
		const node = Array.from(this._nodes.values())[0];
		if (!node) throw new Error("No nodes available");

		if (!node.isConnected()) {
			await node.connect();
		}

		const result = await node.command("version");
		return result;
	}

	/**
	 * Quit all connections gracefully.
	 * @returns {Promise<void>}
	 */
	public async quit(): Promise<void> {
		await Promise.all(
			Array.from(this._nodes.values()).map(async (node) => {
				if (node.isConnected()) {
					await node.quit();
				}
			}),
		);
	}

	/**
	 * Disconnect all connections.
	 * @returns {Promise<void>}
	 */
	public async disconnect(): Promise<void> {
		await Promise.all(
			Array.from(this._nodes.values()).map((node) => node.disconnect()),
		);
	}

	/**
	 * Check if any node is connected to a Memcache server.
	 * @returns {boolean}
	 */
	public isConnected(): boolean {
		return Array.from(this._nodes.values()).some((node) => node.isConnected());
	}

	// Private methods

	/**
	 * Get the node for a given key, with lazy connection
	 */
	private async _getNodeForKey(key: string): Promise<MemcacheNode> {
		const nodeKey = this._ring.getNode(key);
		if (!nodeKey) {
			throw new Error(`No node available for key: ${key}`);
		}

		const node = this._nodes.get(nodeKey);
		if (!node) throw new Error(`Node ${nodeKey} not found`);

		// Lazy connect if not connected
		if (!node.isConnected()) {
			await node.connect();
		}

		return node;
	}

	/**
	 * Forward events from a MemcacheNode to the Memcache instance
	 */
	private _forwardNodeEvents(node: MemcacheNode): void {
		node.on("connect", () => this.emit(MemcacheEvents.CONNECT, node.id));
		node.on("close", () => this.emit(MemcacheEvents.CLOSE, node.id));
		node.on("error", (err: Error) =>
			this.emit(MemcacheEvents.ERROR, node.id, err),
		);
		node.on("timeout", () => this.emit(MemcacheEvents.TIMEOUT, node.id));
		node.on("hit", (key: string, value: string) =>
			this.emit(MemcacheEvents.HIT, key, value),
		);
		node.on("miss", (key: string) => this.emit(MemcacheEvents.MISS, key));
	}

	private validateKey(key: string): void {
		if (!key || key.length === 0) {
			throw new Error("Key cannot be empty");
		}
		if (key.length > 250) {
			throw new Error("Key length cannot exceed 250 characters");
		}
		if (/[\s\r\n\0]/.test(key)) {
			throw new Error(
				"Key cannot contain spaces, newlines, or null characters",
			);
		}
	}
}

export default Memcache;
