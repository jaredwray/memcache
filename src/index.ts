import { Hookified } from "hookified";
import { KetamaHash } from "./ketama.js";
import { createNode, MemcacheNode } from "./node.js";

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

export interface HashProvider {
	name: string;
	nodes: Array<MemcacheNode>;
	addNode: (node: MemcacheNode) => void;
	removeNode: (id: string) => void;
	getNode: (id: string) => MemcacheNode | undefined;
	getNodesByKey: (key: string) => Array<MemcacheNode>;
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

	/**
	 * The hash provider used to determine the distribution on each item is placed based
	 * on the number of nodes and hashing. By default it uses KetamaHash as the provider
	 */
	hash?: HashProvider;
}

export interface MemcacheStats {
	[key: string]: string;
}

export class Memcache extends Hookified {
	private _nodes: Array<MemcacheNode> = [];
	private _timeout: number;
	private _keepAlive: boolean;
	private _keepAliveDelay: number;
	private _hash: HashProvider;

	constructor(options?: MemcacheOptions) {
		super();

		this._hash = new KetamaHash();

		this._timeout = options?.timeout || 5000;
		this._keepAlive = options?.keepAlive !== false;
		this._keepAliveDelay = options?.keepAliveDelay || 1000;

		// Add nodes if provided, otherwise add default node
		const nodeUris = options?.nodes || ["localhost:11211"];
		for (const nodeUri of nodeUris) {
			this.addNode(nodeUri);
		}
	}

	/**
	 * Get the list of nodes
	 * @returns {MemcacheNode[]} Array of MemcacheNode
	 */
	public get nodes(): MemcacheNode[] {
		return this._nodes;
	}

	/**
	 * Get the list of node IDs (e.g., ["localhost:11211", "127.0.0.1:11212"])
	 * @returns {string[]} Array of node ID strings
	 */
	public get nodeIds(): string[] {
		return this._nodes.map((node) => node.id);
	}

	/**
	 * Get the hash provider used for consistent hashing distribution.
	 * @returns {HashProvider} The current hash provider instance
	 * @default KetamaHash
	 *
	 * @example
	 * ```typescript
	 * const client = new Memcache();
	 * const hashProvider = client.hash;
	 * console.log(hashProvider.name); // "ketama"
	 * ```
	 */
	public get hash(): HashProvider {
		return this._hash;
	}

	/**
	 * Set the hash provider used for consistent hashing distribution.
	 * This allows you to customize the hashing strategy for distributing keys across nodes.
	 * @param {HashProvider} hash - The hash provider instance to use
	 *
	 * @example
	 * ```typescript
	 * const client = new Memcache();
	 * const customHashProvider = new KetamaHash();
	 * client.hash = customHashProvider;
	 * ```
	 */
	public set hash(hash: HashProvider) {
		this._hash = hash;
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
	 * Updates all existing nodes with the new value.
	 * Note: To apply the new value, you need to call reconnect() on the nodes.
	 * @param {boolean} value
	 * @default true
	 */
	public set keepAlive(value: boolean) {
		this._keepAlive = value;
		// Update all existing nodes
		this.updateNodes();
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
	 * Updates all existing nodes with the new value.
	 * Note: To apply the new value, you need to call reconnect() on the nodes.
	 * @param {number} value
	 * @default 1000
	 */
	public set keepAliveDelay(value: number) {
		this._keepAliveDelay = value;
		// Update all existing nodes
		this.updateNodes();
	}

	/**
	 * Get an array of all MemcacheNode instances
	 * @returns {MemcacheNode[]}
	 */
	public getNodes(): MemcacheNode[] {
		return [...this._nodes];
	}

	/**
	 * Get a specific node by its ID
	 * @param {string} id - The node ID (e.g., "localhost:11211")
	 * @returns {MemcacheNode | undefined}
	 */
	public getNode(id: string): MemcacheNode | undefined {
		return this._nodes.find((n) => n.id === id);
	}

	/**
	 * Add a new node to the cluster
	 * @param {string} uri - Node URI (e.g., "localhost:11212")
	 * @param {number} weight - Optional weight for consistent hashing
	 */
	public async addNode(uri: string, weight?: number): Promise<void> {
		const { host, port } = this.parseUri(uri);
		const nodeKey = port === 0 ? host : `${host}:${port}`;

		if (this._nodes.some((n) => n.id === nodeKey)) {
			throw new Error(`Node ${nodeKey} already exists`);
		}

		// Create and connect node
		const node = new MemcacheNode(host, port, {
			timeout: this._timeout,
			keepAlive: this._keepAlive,
			keepAliveDelay: this._keepAliveDelay,
			weight,
		});

		this.forwardNodeEvents(node);
		this._nodes.push(node);

		this._hash.addNode(node);
	}

	/**
	 * Remove a node from the cluster
	 * @param {string} uri - Node URI (e.g., "localhost:11212")
	 */
	public async removeNode(uri: string): Promise<void> {
		const { host, port } = this.parseUri(uri);
		const nodeKey = port === 0 ? host : `${host}:${port}`;

		const node = this._nodes.find((n) => n.id === nodeKey);
		if (!node) return;

		// Disconnect and remove
		await node.disconnect();
		this._nodes = this._nodes.filter((n) => n.id !== nodeKey);
		this._hash.removeNode(node.id);
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
			const node = this._nodes.find((n) => n.id === nodeId);
			/* v8 ignore next -- @preserve */
			if (!node) throw new Error(`Node ${nodeId} not found`);
			/* v8 ignore next -- @preserve */
			await node.connect();
			/* v8 ignore next -- @preserve */
			return;
		}

		// Connect to all nodes
		await Promise.all(this._nodes.map((node) => node.connect()));
	}

	/**
	 * Get a value from the Memcache server.
	 * When multiple nodes are returned by the hash provider (for replication),
	 * queries all nodes and returns the first successful result.
	 * @param {string} key
	 * @returns {Promise<string | undefined>}
	 */
	public async get(key: string): Promise<string | undefined> {
		await this.beforeHook("get", { key });

		this.validateKey(key);

		const nodes = await this.getNodesByKey(key);

		// Query all nodes (supports replication strategies)
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(`get ${key}`, {
					isMultiline: true,
					requestedKeys: [key],
				});

				if (result?.values && result.values.length > 0) {
					return result.values[0];
				}
				return undefined;
			} catch {
				// If one node fails, try the others
				return undefined;
			}
		});

		// Wait for all nodes to respond
		const results = await Promise.all(promises);

		// Return the first successful result
		const value = results.find((v) => v !== undefined);

		await this.afterHook("get", { key, value });

		return value;
	}

	/**
	 * Get multiple values from the Memcache server.
	 * When multiple nodes are returned by the hash provider (for replication),
	 * queries all replica nodes and returns the first successful result for each key.
	 * @param keys {string[]}
	 * @returns {Promise<Map<string, string>>}
	 */
	public async gets(keys: string[]): Promise<Map<string, string>> {
		await this.beforeHook("gets", { keys });

		// Validate all keys
		for (const key of keys) {
			this.validateKey(key);
		}

		// Group keys by all their replica nodes
		const keysByNode = new Map<MemcacheNode, string[]>();

		for (const key of keys) {
			const nodes = this._hash.getNodesByKey(key);
			if (nodes.length === 0) {
				/* v8 ignore next -- @preserve */
				throw new Error(`No node available for key: ${key}`);
			}

			// Add key to all replica nodes
			for (const node of nodes) {
				if (!keysByNode.has(node)) {
					keysByNode.set(node, []);
				}
				// biome-ignore lint/style/noNonNullAssertion: we just set it
				keysByNode.get(node)!.push(key);
			}
		}

		// Execute commands in parallel across all nodes (including replicas)
		const promises = Array.from(keysByNode.entries()).map(
			async ([node, nodeKeys]) => {
				try {
					if (!node.isConnected()) await node.connect();

					const keysStr = nodeKeys.join(" ");
					const result = await node.command(`get ${keysStr}`, {
						isMultiline: true,
						requestedKeys: nodeKeys,
					});

					return result;
				} catch {
					// If one node fails, continue with others
					/* v8 ignore next -- @preserve */
					return undefined;
				}
			},
		);

		const results = await Promise.all(promises);

		// Merge results into Map (first successful value for each key wins)
		const map = new Map<string, string>();

		for (const result of results) {
			if (result?.foundKeys && result.values) {
				// Map found keys to their values
				for (let i = 0; i < result.foundKeys.length; i++) {
					if (result.values[i] !== undefined) {
						// Only set if key doesn't exist yet (first successful result wins)
						if (!map.has(result.foundKeys[i])) {
							map.set(result.foundKeys[i], result.values[i]);
						}
					}
				}
			}
		}

		await this.afterHook("gets", { keys, values: map });

		return map;
	}

	/**
	 * Check-And-Set: Store a value only if it hasn't been modified since last fetch.
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns true only if all succeed.
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

		const nodes = await this.getNodesByKey(key);

		// Execute CAS on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(command);
				return result === "STORED";
			} catch {
				// If one node fails, the entire operation fails
				/* v8 ignore next -- @preserve */
				return false;
			}
		});

		const results = await Promise.all(promises);

		// All nodes must succeed for CAS to be considered successful
		const success = results.every((result) => result === true);

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
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns true only if all succeed.
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

		const nodes = await this.getNodesByKey(key);

		// Execute SET on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(command);
				return result === "STORED";
			} catch {
				// If one node fails, the entire operation fails
				/* v8 ignore next -- @preserve */
				return false;
			}
		});

		const results = await Promise.all(promises);

		// All nodes must succeed for SET to be considered successful
		const success = results.every((result) => result === true);

		await this.afterHook("set", { key, value, exptime, flags, success });

		return success;
	}

	/**
	 * Add a value to the Memcache server (only if key doesn't exist).
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns true only if all succeed.
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

		const nodes = await this.getNodesByKey(key);

		// Execute ADD on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(command);
				return result === "STORED";
			} catch {
				// If one node fails, the entire operation fails
				/* v8 ignore next -- @preserve */
				return false;
			}
		});

		const results = await Promise.all(promises);

		// All nodes must succeed for ADD to be considered successful
		const success = results.every((result) => result === true);

		await this.afterHook("add", { key, value, exptime, flags, success });

		return success;
	}

	/**
	 * Replace a value in the Memcache server (only if key exists).
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns true only if all succeed.
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

		const nodes = await this.getNodesByKey(key);

		// Execute REPLACE on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(command);
				return result === "STORED";
			} catch {
				// If one node fails, the entire operation fails
				/* v8 ignore next -- @preserve */
				return false;
			}
		});

		const results = await Promise.all(promises);

		// All nodes must succeed for REPLACE to be considered successful
		const success = results.every((result) => result === true);

		await this.afterHook("replace", { key, value, exptime, flags, success });

		return success;
	}

	/**
	 * Append a value to an existing key in the Memcache server.
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns true only if all succeed.
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

		const nodes = await this.getNodesByKey(key);

		// Execute APPEND on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(command);
				return result === "STORED";
			} catch {
				// If one node fails, the entire operation fails
				/* v8 ignore next -- @preserve */
				return false;
			}
		});

		const results = await Promise.all(promises);

		// All nodes must succeed for APPEND to be considered successful
		const success = results.every((result) => result === true);

		await this.afterHook("append", { key, value, success });

		return success;
	}

	/**
	 * Prepend a value to an existing key in the Memcache server.
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns true only if all succeed.
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

		const nodes = await this.getNodesByKey(key);

		// Execute PREPEND on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(command);
				return result === "STORED";
			} catch {
				// If one node fails, the entire operation fails
				/* v8 ignore next -- @preserve */
				return false;
			}
		});

		const results = await Promise.all(promises);

		// All nodes must succeed for PREPEND to be considered successful
		const success = results.every((result) => result === true);

		await this.afterHook("prepend", { key, value, success });

		return success;
	}

	/**
	 * Delete a value from the Memcache server.
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns true only if all succeed.
	 * @param key {string}
	 * @returns {Promise<boolean>}
	 */
	public async delete(key: string): Promise<boolean> {
		await this.beforeHook("delete", { key });

		this.validateKey(key);

		const nodes = await this.getNodesByKey(key);

		// Execute DELETE on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(`delete ${key}`);
				return result === "DELETED";
			} catch {
				// If one node fails, the entire operation fails
				/* v8 ignore next -- @preserve */
				return false;
			}
		});

		const results = await Promise.all(promises);

		// All nodes must succeed for DELETE to be considered successful
		const success = results.every((result) => result === true);

		await this.afterHook("delete", { key, success });

		return success;
	}

	/**
	 * Increment a value in the Memcache server.
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns the first successful result.
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

		const nodes = await this.getNodesByKey(key);

		// Execute INCR on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(`incr ${key} ${value}`);
				return typeof result === "number" ? result : undefined;
			} catch {
				// If one node fails, continue with others
				/* v8 ignore next -- @preserve */
				return undefined;
			}
		});

		const results = await Promise.all(promises);

		// Return the first successful result
		const newValue = results.find((v) => v !== undefined);

		await this.afterHook("incr", { key, value, newValue });

		return newValue;
	}

	/**
	 * Decrement a value in the Memcache server.
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns the first successful result.
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

		const nodes = await this.getNodesByKey(key);

		// Execute DECR on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(`decr ${key} ${value}`);
				return typeof result === "number" ? result : undefined;
			} catch {
				// If one node fails, continue with others
				/* v8 ignore next -- @preserve */
				return undefined;
			}
		});

		const results = await Promise.all(promises);

		// Return the first successful result
		const newValue = results.find((v) => v !== undefined);

		await this.afterHook("decr", { key, value, newValue });

		return newValue;
	}

	/**
	 * Touch a value in the Memcache server (update expiration time).
	 * When multiple nodes are returned by the hash provider (for replication),
	 * executes on all nodes and returns true only if all succeed.
	 * @param key {string}
	 * @param exptime {number}
	 * @returns {Promise<boolean>}
	 */
	public async touch(key: string, exptime: number): Promise<boolean> {
		await this.beforeHook("touch", { key, exptime });

		this.validateKey(key);

		const nodes = await this.getNodesByKey(key);

		// Execute TOUCH on all replica nodes
		const promises = nodes.map(async (node) => {
			try {
				const result = await node.command(`touch ${key} ${exptime}`);
				return result === "TOUCHED";
			} catch {
				// If one node fails, the entire operation fails
				/* v8 ignore next -- @preserve */
				return false;
			}
		});

		const results = await Promise.all(promises);

		// All nodes must succeed for TOUCH to be considered successful
		const success = results.every((result) => result === true);

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
			this._nodes.map(async (node) => {
				/* v8 ignore next -- @preserve */
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
			/* v8 ignore next -- @preserve */
			this._nodes.map(async (node) => {
				if (!node.isConnected()) {
					await node.connect();
				}

				const stats = await node.command(command, { isStats: true });
				results.set(node.id, stats as MemcacheStats);
			}),
		);

		return results;
	}

	/**
	 * Get the Memcache server version from all nodes.
	 * @returns {Promise<Map<string, string>>} Map of node IDs to version strings
	 */
	public async version(): Promise<Map<string, string>> {
		// Get version from all nodes
		const results = new Map<string, string>();

		await Promise.all(
			/* v8 ignore next -- @preserve */
			this._nodes.map(async (node) => {
				if (!node.isConnected()) {
					await node.connect();
				}

				const version = await node.command("version");
				results.set(node.id, version);
			}),
		);

		return results;
	}

	/**
	 * Quit all connections gracefully.
	 * @returns {Promise<void>}
	 */
	public async quit(): Promise<void> {
		await Promise.all(
			this._nodes.map(async (node) => {
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
		await Promise.all(this._nodes.map((node) => node.disconnect()));
	}

	/**
	 * Reconnect all nodes by disconnecting and connecting them again.
	 * @returns {Promise<void>}
	 */
	public async reconnect(): Promise<void> {
		await Promise.all(this._nodes.map((node) => node.reconnect()));
	}

	/**
	 * Check if any node is connected to a Memcache server.
	 * @returns {boolean}
	 */
	public isConnected(): boolean {
		return this._nodes.some((node) => node.isConnected());
	}

	/**
	 * Get the nodes for a given key using consistent hashing, with lazy connection.
	 * This method will automatically connect to the nodes if they're not already connected.
	 * Returns an array to support replication strategies.
	 * @param {string} key - The cache key
	 * @returns {Promise<Array<MemcacheNode>>} The nodes responsible for this key
	 * @throws {Error} If no nodes are available for the key
	 */
	public async getNodesByKey(key: string): Promise<Array<MemcacheNode>> {
		const nodes = this._hash.getNodesByKey(key);
		/* v8 ignore next -- @preserve */
		if (nodes.length === 0) {
			throw new Error(`No node available for key: ${key}`);
		}

		// Lazy connect if not connected
		for (const node of nodes) {
			if (!node.isConnected()) {
				await node.connect();
			}
		}

		return nodes;
	}

	/**
	 * Validates a Memcache key according to protocol requirements.
	 * @param {string} key - The key to validate
	 * @throws {Error} If the key is empty, exceeds 250 characters, or contains invalid characters
	 *
	 * @example
	 * ```typescript
	 * client.validateKey("valid-key"); // OK
	 * client.validateKey(""); // Throws: Key cannot be empty
	 * client.validateKey("a".repeat(251)); // Throws: Key length cannot exceed 250 characters
	 * client.validateKey("key with spaces"); // Throws: Key cannot contain spaces, newlines, or null characters
	 * ```
	 */
	public validateKey(key: string): void {
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

	// Private methods

	/**
	 * Update all nodes with current keepAlive settings
	 */
	private updateNodes(): void {
		// Update all nodes with the current keepAlive settings
		for (const node of this._nodes) {
			node.keepAlive = this._keepAlive;
			node.keepAliveDelay = this._keepAliveDelay;
		}
	}

	/**
	 * Forward events from a MemcacheNode to the Memcache instance
	 */
	private forwardNodeEvents(node: MemcacheNode): void {
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
}

export { createNode };
export default Memcache;
