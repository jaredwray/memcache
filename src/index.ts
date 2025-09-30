import { createConnection, type Socket } from "node:net";
import { Hookified } from "hookified";
import { HashRing } from "./ketama";

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

export type CommandQueueItem = {
	command: string;
	// biome-ignore lint/suspicious/noExplicitAny: expected
	resolve: (value: any) => void;
	// biome-ignore lint/suspicious/noExplicitAny: expected
	reject: (reason?: any) => void;
	isMultiline?: boolean;
	isStats?: boolean;
	requestedKeys?: string[];
	foundKeys?: string[];
};

export class Memcache extends Hookified {
	private _socket: Socket | undefined = undefined;
	private _timeout: number;
	private _keepAlive: boolean;
	private _keepAliveDelay: number;
	private _connected: boolean = false;
	private _commandQueue: CommandQueueItem[] = [];
	private _buffer: string = "";
	private _currentCommand: CommandQueueItem | undefined = undefined;
	private _multilineData: string[] = [];
	private _ring: HashRing<string>;

	constructor(options?: MemcacheOptions) {
		super();
		this._timeout = options?.timeout || 5000;
		this._keepAlive = options?.keepAlive !== false;
		this._keepAliveDelay = options?.keepAliveDelay || 1000;
		this._ring = new HashRing<string>();

		// Add nodes to the ring if provided, otherwise add default node
		if (options?.nodes && options.nodes.length > 0) {
			for (const nodeUri of options.nodes) {
				const { host, port } = this.parseUri(nodeUri);
				// Store as host:port format in the ring
				const nodeKey = port === 0 ? host : `${host}:${port}`;
				this._ring.addNode(nodeKey);
			}
		} else {
			// Add default node if no nodes provided
			this._ring.addNode("localhost:11211");
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
	 * Get the socket connection.
	 * @returns {Socket | undefined}
	 */
	public get socket(): Socket | undefined {
		return this._socket;
	}

	/**
	 * Set the socket connection.
	 * @param {Socket | undefined} value
	 */
	public set socket(value: Socket | undefined) {
		this._socket = value;
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
	 * Get the current command being processed.
	 * @returns {CommandQueueItem | undefined}
	 */
	public get currentCommand(): CommandQueueItem | undefined {
		return this._currentCommand;
	}

	/**
	 * Set the current command being processed. This is for internal use
	 * @param {CommandQueueItem | undefined} value
	 */
	public set currentCommand(value: CommandQueueItem | undefined) {
		this._currentCommand = value;
	}

	/**
	 * Get the command queue for the Memcache client.
	 * @returns {CommandQueueItem[]}
	 */
	public get commandQueue(): CommandQueueItem[] {
		return this._commandQueue;
	}

	/**
	 * Set the command queue for the Memcache client.
	 * @param {CommandQueueItem[]} value
	 */
	public set commandQueue(value: CommandQueueItem[]) {
		this._commandQueue = value;
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
	 * Connect to a Memcache server.
	 * @param {string} host - The hostname of the Memcache server
	 * @param {number} port - The port of the Memcache server (default: 11211)
	 * @returns {Promise<void>}
	 */
	public async connect(
		host: string = "localhost",
		port: number = 11211,
	): Promise<void> {
		return new Promise((resolve, reject) => {
			if (this._connected) {
				resolve();
				return;
			}

			this._socket = createConnection({
				host,
				port,
				keepAlive: this._keepAlive,
				keepAliveInitialDelay: this._keepAliveDelay,
			});

			this._socket.setTimeout(this._timeout);
			this._socket.setEncoding("utf8");

			this._socket.on("connect", () => {
				this._connected = true;
				this.emit(MemcacheEvents.CONNECT);
				resolve();
			});

			this._socket.on("data", (data: string) => {
				this.handleData(data);
			});

			this._socket.on("error", (error: Error) => {
				this.emit(MemcacheEvents.ERROR, error);
				if (!this._connected) {
					reject(error);
				}
			});

			this._socket.on("close", () => {
				this._connected = false;
				this.emit(MemcacheEvents.CLOSE);
				this.rejectPendingCommands(new Error("Connection closed"));
			});

			this._socket.on("timeout", () => {
				this.emit(MemcacheEvents.TIMEOUT);
				this._socket?.destroy();
				reject(new Error("Connection timeout"));
			});
		});
	}

	/**
	 * Get a value from the Memcache server.
	 * @param {string} key
	 * @returns {Promise<string | undefined>}
	 */
	public async get(key: string): Promise<string | undefined> {
		await this.beforeHook("get", { key });

		this.validateKey(key);
		const result = await this.sendCommand(`get ${key}`, true, false, [key]);

		await this.afterHook("get", { key, value: result });

		if (result && result.length > 0) {
			return result[0];
		}
		return undefined;
	}

	/**
	 * Get multiple values from the Memcache server.
	 * @param keys {string[]}
	 * @returns {Promise<Map<string, string>>}
	 */
	public async gets(keys: string[]): Promise<Map<string, string>> {
		await this.beforeHook("gets", { keys });

		for (const key of keys) {
			this.validateKey(key);
		}
		const keysStr = keys.join(" ");
		const results = (await this.sendCommand(
			`get ${keysStr}`,
			true,
			false,
			keys,
		)) as string[] | undefined;
		const map = new Map<string, string>();

		if (results) {
			for (let i = 0; i < keys.length && i < results.length; i++) {
				map.set(keys[i], results[i]);
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
		const result = await this.sendCommand(command);
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
	 * Get a value from the Memcache server.
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
		const result = await this.sendCommand(command);
		const success = result === "STORED";

		await this.afterHook("set", { key, value, exptime, flags, success });

		return success;
	}

	/**
	 * Get a value from the Memcache server.
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
		const result = await this.sendCommand(command);
		const success = result === "STORED";

		await this.afterHook("add", { key, value, exptime, flags, success });

		return success;
	}

	/**
	 * Get a value from the Memcache server.
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
		const result = await this.sendCommand(command);
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
		const result = await this.sendCommand(command);
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
		const result = await this.sendCommand(command);
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
		const result = await this.sendCommand(`delete ${key}`);
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
		const result = await this.sendCommand(`incr ${key} ${value}`);
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
		const result = await this.sendCommand(`decr ${key} ${value}`);
		const newValue = typeof result === "number" ? result : undefined;

		await this.afterHook("decr", { key, value, newValue });

		return newValue;
	}

	/**
	 * Touch a value in the Memcache server.
	 * @param key {string}
	 * @param exptime {number}
	 * @returns {Promise<boolean>}
	 */
	public async touch(key: string, exptime: number): Promise<boolean> {
		await this.beforeHook("touch", { key, exptime });

		this.validateKey(key);
		const result = await this.sendCommand(`touch ${key} ${exptime}`);
		const success = result === "TOUCHED";

		await this.afterHook("touch", { key, exptime, success });

		return success;
	}

	/**
	 * Flush all values from the Memcache server.
	 * @param delay {number}
	 * @returns {Promise<boolean>}
	 */
	public async flush(delay?: number): Promise<boolean> {
		let command = "flush_all";

		// If a delay is specified, use the delayed flush command
		if (delay !== undefined) {
			command += ` ${delay}`;
		}

		const result = await this.sendCommand(command);
		return result === "OK";
	}

	/**
	 * Get statistics from the Memcache server.
	 * @param type {string}
	 * @returns {Promise<MemcacheStats>}
	 */
	public async stats(type?: string): Promise<MemcacheStats> {
		const command = type ? `stats ${type}` : "stats";
		return await this.sendCommand(command, false, true);
	}

	/**
	 * Get the Memcache server version.
	 * @returns {Promise<string>}
	 */
	public async version(): Promise<string> {
		const result = await this.sendCommand("version");
		return result;
	}

	/**
	 * Quit the Memcache server and disconnect the socket.
	 * @returns {Promise<void>}
	 */
	public async quit(): Promise<void> {
		if (this._connected && this._socket) {
			try {
				await this.sendCommand("quit");
				// biome-ignore lint/correctness/noUnusedVariables: expected
			} catch (error) {
				// Ignore errors from quit command as the server closes the connection
			}
			await this.disconnect();
		}
	}

	/**
	 * Disconnect the socket from the Memcache server. Use quit for graceful disconnection.
	 * @returns {Promise<void>}
	 */
	public async disconnect(): Promise<void> {
		if (this._socket) {
			this._socket.destroy();
			this._socket = undefined;
			this._connected = false;
		}
	}

	/**
	 * Check if the client is connected to the Memcache server.
	 * @returns {boolean}
	 */
	public isConnected(): boolean {
		return this._connected;
	}

	// Private methods
	private handleData(data: string): void {
		this._buffer += data;

		while (true) {
			const lineEnd = this._buffer.indexOf("\r\n");
			if (lineEnd === -1) break;

			const line = this._buffer.substring(0, lineEnd);
			this._buffer = this._buffer.substring(lineEnd + 2);

			this.processLine(line);
		}
	}

	private processLine(line: string): void {
		if (!this._currentCommand) {
			this._currentCommand = this._commandQueue.shift();
			if (!this._currentCommand) return;
		}

		if (this._currentCommand.isStats) {
			if (line === "END") {
				const stats: MemcacheStats = {};
				for (const statLine of this._multilineData) {
					const [, key, value] = statLine.split(" ");
					if (key && value) {
						stats[key] = value;
					}
				}
				this._currentCommand.resolve(stats);
				this._multilineData = [];
				this._currentCommand = undefined;
			} else if (line.startsWith("STAT ")) {
				this._multilineData.push(line);
			} else if (
				line.startsWith("ERROR") ||
				line.startsWith("CLIENT_ERROR") ||
				line.startsWith("SERVER_ERROR")
			) {
				this._currentCommand.reject(new Error(line));
				this._currentCommand = undefined;
			}
			return;
		}

		if (this._currentCommand.isMultiline) {
			// Track found keys locally for this command
			if (!this._currentCommand.foundKeys) {
				this._currentCommand.foundKeys = [];
			}

			if (line.startsWith("VALUE ")) {
				const parts = line.split(" ");
				const key = parts[1];
				const bytes = parseInt(parts[3], 10);
				this._currentCommand.foundKeys.push(key);
				this.readValue(bytes);
			} else if (line === "END") {
				const result =
					this._multilineData.length > 0 ? this._multilineData : undefined;

				// Emit hit/miss events if we have requested keys
				if (
					this._currentCommand.requestedKeys &&
					this._currentCommand.foundKeys
				) {
					for (let i = 0; i < this._currentCommand.foundKeys.length; i++) {
						this.emit(
							MemcacheEvents.HIT,
							this._currentCommand.foundKeys[i],
							this._multilineData[i],
						);
					}

					// Emit miss events for keys that weren't found
					const missedKeys = this._currentCommand.requestedKeys.filter(
						(key) => !this._currentCommand.foundKeys.includes(key),
					);
					for (const key of missedKeys) {
						this.emit(MemcacheEvents.MISS, key);
					}
				}

				this._currentCommand.resolve(result);
				this._multilineData = [];
				this._currentCommand = undefined;
			} else if (
				line.startsWith("ERROR") ||
				line.startsWith("CLIENT_ERROR") ||
				line.startsWith("SERVER_ERROR")
			) {
				this._currentCommand.reject(new Error(line));
				this._multilineData = [];
				this._currentCommand = undefined;
			}
		} else {
			if (
				line === "STORED" ||
				line === "DELETED" ||
				line === "OK" ||
				line === "TOUCHED" ||
				line === "EXISTS" ||
				line === "NOT_FOUND"
			) {
				this._currentCommand.resolve(line);
			} else if (line === "NOT_STORED") {
				this._currentCommand.resolve(false);
			} else if (
				line.startsWith("ERROR") ||
				line.startsWith("CLIENT_ERROR") ||
				line.startsWith("SERVER_ERROR")
			) {
				this._currentCommand.reject(new Error(line));
			} else if (/^\d+$/.test(line)) {
				this._currentCommand.resolve(parseInt(line, 10));
			} else {
				this._currentCommand.resolve(line);
			}
			this._currentCommand = undefined;
		}
	}

	private readValue(bytes: number): void {
		const valueEnd = this._buffer.indexOf("\r\n");
		if (valueEnd >= bytes) {
			const value = this._buffer.substring(0, bytes);
			this._buffer = this._buffer.substring(bytes + 2);
			this._multilineData.push(value);
		}
	}

	private async sendCommand(
		command: string,
		isMultiline: boolean = false,
		isStats: boolean = false,
		requestedKeys?: string[],
		// biome-ignore lint/suspicious/noExplicitAny: expected
	): Promise<any> {
		if (!this._connected || !this._socket) {
			throw new Error("Not connected to memcache server");
		}

		return new Promise((resolve, reject) => {
			this._commandQueue.push({
				command,
				resolve,
				reject,
				isMultiline,
				isStats,
				requestedKeys,
			});
			// biome-ignore lint/style/noNonNullAssertion: socket is checked
			this._socket!.write(`${command}\r\n`);
		});
	}

	private rejectPendingCommands(error: Error): void {
		if (this._currentCommand) {
			this._currentCommand.reject(error);
			this._currentCommand = undefined;
		}
		while (this._commandQueue.length > 0) {
			const cmd = this._commandQueue.shift();
			if (cmd) {
				cmd.reject(error);
			}
		}
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
