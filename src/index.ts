import { createConnection, type Socket } from "node:net";
import { Hookified } from "hookified";

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
	 * The hostname of the Memcache server.
	 * @default "localhost"
	 */
	host?: string;
	/**
	 * The port of the Memcache server.
	 * @default 11211
	 */
	port?: number;
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
};

export class Memcache extends Hookified {
	private _socket: Socket | undefined = undefined;
	private _host: string;
	private _port: number;
	private _timeout: number;
	private _keepAlive: boolean;
	private _keepAliveDelay: number;
	private _connected: boolean = false;
	private _commandQueue: CommandQueueItem[] = [];
	private _buffer: string = "";
	private _currentCommand: CommandQueueItem | undefined = undefined;
	private _multilineData: string[] = [];
	private _foundKeys: string[] = [];

	constructor(options: MemcacheOptions = {}) {
		super();
		this._host = options.host || "localhost";
		this._port = options.port || 11211;
		this._timeout = options.timeout || 5000;
		this._keepAlive = options.keepAlive !== false;
		this._keepAliveDelay = options.keepAliveDelay || 1000;
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
	 * Get the hostname of the Memcache server.
	 * @returns {string}
	 * @default "localhost"
	 */
	public get host(): string {
		return this._host;
	}

	/**
	 * Set the hostname of the Memcache server.
	 * @param {string} value
	 * @default "localhost"
	 */
	public set host(value: string) {
		this._host = value;
	}

	/**
	 * Get the port of the Memcache server.
	 * @returns {number}
	 * @default 11211
	 */
	public get port(): number {
		return this._port;
	}

	/**
	 * Set the port of the Memcache server.
	 * @param {number} value
	 * @default 11211
	 */
	public set port(value: number) {
		this._port = value;
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
	 * Connect to the Memcached server.
	 * @returns {Promise<void>}
	 */
	public async connect(): Promise<void> {
		return new Promise((resolve, reject) => {
			if (this._connected) {
				resolve();
				return;
			}

			this._socket = createConnection({
				host: this._host,
				port: this._port,
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
		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `prepend ${key} 0 0 ${bytes}\r\n${valueStr}`;
		const result = await this.sendCommand(command);
		return result === "STORED";
	}

	/**
	 * Delete a value from the Memcache server.
	 * @param key {string}
	 * @returns {Promise<boolean>}
	 */
	public async delete(key: string): Promise<boolean> {
		this.validateKey(key);
		const result = await this.sendCommand(`delete ${key}`);
		return result === "DELETED";
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
		this.validateKey(key);
		const result = await this.sendCommand(`incr ${key} ${value}`);
		return typeof result === "number" ? result : undefined;
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
		this.validateKey(key);
		const result = await this.sendCommand(`decr ${key} ${value}`);
		return typeof result === "number" ? result : undefined;
	}

	/**
	 * Touch a value in the Memcache server.
	 * @param key {string}
	 * @param exptime {number}
	 * @returns {Promise<boolean>}
	 */
	public async touch(key: string, exptime: number): Promise<boolean> {
		this.validateKey(key);
		const result = await this.sendCommand(`touch ${key} ${exptime}`);
		return result === "TOUCHED";
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
			if (line.startsWith("VALUE ")) {
				const parts = line.split(" ");
				const key = parts[1];
				const bytes = parseInt(parts[3], 10);
				this._foundKeys.push(key);
				this.readValue(bytes);
			} else if (line === "END") {
				const result =
					this._multilineData.length > 0 ? this._multilineData : undefined;

				// Emit hit/miss events if we have requested keys
				if (this._currentCommand.requestedKeys) {
					for (let i = 0; i < this._foundKeys.length; i++) {
						this.emit(
							MemcacheEvents.HIT,
							this._foundKeys[i],
							this._multilineData[i],
						);
					}

					// Emit miss events for keys that weren't found
					const missedKeys = this._currentCommand.requestedKeys.filter(
						(key) => !this._foundKeys.includes(key),
					);
					for (const key of missedKeys) {
						this.emit(MemcacheEvents.MISS, key);
					}
				}

				this._currentCommand.resolve(result);
				this._multilineData = [];
				this._foundKeys = [];
				this._currentCommand = undefined;
			} else if (
				line.startsWith("ERROR") ||
				line.startsWith("CLIENT_ERROR") ||
				line.startsWith("SERVER_ERROR")
			) {
				this._currentCommand.reject(new Error(line));
				this._multilineData = [];
				this._foundKeys = [];
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
