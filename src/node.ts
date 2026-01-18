import { createConnection, type Socket } from "node:net";
import { Hookified } from "hookified";
import {
	buildAddRequest,
	buildAppendRequest,
	buildDecrementRequest,
	buildDeleteRequest,
	buildFlushRequest,
	buildGetRequest,
	buildIncrementRequest,
	buildPrependRequest,
	buildQuitRequest,
	buildReplaceRequest,
	buildSaslPlainRequest,
	buildSetRequest,
	buildStatRequest,
	buildTouchRequest,
	buildVersionRequest,
	deserializeHeader,
	HEADER_SIZE,
	OPCODE_STAT,
	parseGetResponse,
	parseIncrDecrResponse,
	STATUS_AUTH_ERROR,
	STATUS_KEY_NOT_FOUND,
	STATUS_SUCCESS,
} from "./binary-protocol.js";
import type { SASLCredentials } from "./types.js";

export interface MemcacheNodeOptions {
	timeout?: number;
	keepAlive?: boolean;
	keepAliveDelay?: number;
	weight?: number;
	/** SASL authentication credentials */
	sasl?: SASLCredentials;
}

export interface CommandOptions {
	isMultiline?: boolean;
	isStats?: boolean;
	requestedKeys?: string[];
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

/**
 * MemcacheNode represents a single memcache server connection.
 * It handles the socket connection, command queue, and protocol parsing for one node.
 */
export class MemcacheNode extends Hookified {
	private _host: string;
	private _port: number;
	private _socket: Socket | undefined = undefined;
	private _timeout: number;
	private _keepAlive: boolean;
	private _keepAliveDelay: number;
	private _weight: number;
	private _connected: boolean = false;
	private _commandQueue: CommandQueueItem[] = [];
	private _buffer: string = "";
	private _currentCommand: CommandQueueItem | undefined = undefined;
	private _multilineData: string[] = [];
	private _pendingValueBytes: number = 0;
	private _sasl: SASLCredentials | undefined;
	private _authenticated: boolean = false;
	private _binaryBuffer: Buffer = Buffer.alloc(0);

	constructor(host: string, port: number, options?: MemcacheNodeOptions) {
		super();
		this._host = host;
		this._port = port;
		this._timeout = options?.timeout || 5000;
		this._keepAlive = options?.keepAlive !== false;
		this._keepAliveDelay = options?.keepAliveDelay || 1000;
		this._weight = options?.weight || 1;
		this._sasl = options?.sasl;
	}

	/**
	 * Get the host of this node
	 */
	public get host(): string {
		return this._host;
	}

	/**
	 * Get the port of this node
	 */
	public get port(): number {
		return this._port;
	}

	/**
	 * Get the unique identifier for this node (host:port format)
	 */
	public get id(): string {
		return this._port === 0 ? this._host : `${this._host}:${this._port}`;
	}

	/**
	 * Get the full uri like memcache://localhost:11211
	 */
	public get uri(): string {
		return `memcache://${this.id}`;
	}

	/**
	 * Get the socket connection
	 */
	public get socket(): Socket | undefined {
		return this._socket;
	}

	/**
	 * Get the weight of this node (used for consistent hashing distribution)
	 */
	public get weight(): number {
		return this._weight;
	}

	/**
	 * Set the weight of this node (used for consistent hashing distribution)
	 */
	public set weight(value: number) {
		this._weight = value;
	}

	/**
	 * Get the keepAlive setting for this node
	 */
	public get keepAlive(): boolean {
		return this._keepAlive;
	}

	/**
	 * Set the keepAlive setting for this node
	 */
	public set keepAlive(value: boolean) {
		this._keepAlive = value;
	}

	/**
	 * Get the keepAliveDelay setting for this node
	 */
	public get keepAliveDelay(): number {
		return this._keepAliveDelay;
	}

	/**
	 * Set the keepAliveDelay setting for this node
	 */
	public set keepAliveDelay(value: number) {
		this._keepAliveDelay = value;
	}

	/**
	 * Get the command queue
	 */
	public get commandQueue(): CommandQueueItem[] {
		return this._commandQueue;
	}

	/**
	 * Get whether SASL authentication is configured
	 */
	public get hasSaslCredentials(): boolean {
		return !!this._sasl?.username && !!this._sasl?.password;
	}

	/**
	 * Get whether the node is authenticated (only relevant if SASL is configured)
	 */
	public get isAuthenticated(): boolean {
		return this._authenticated;
	}

	/**
	 * Connect to the memcache server
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

			// Don't set encoding initially if SASL is configured (need raw buffers for binary protocol)
			if (!this._sasl) {
				this._socket.setEncoding("utf8");
			}

			this._socket.on("connect", async () => {
				this._connected = true;

				// If SASL credentials are configured, authenticate before resolving
				if (this._sasl) {
					try {
						await this.performSaslAuth();
						// Keep socket in binary mode - SASL servers require binary protocol
						// for all commands. Use binary* methods for operations.
						this.emit("connect");
						resolve();
					} catch (error) {
						this._socket?.destroy();
						this._connected = false;
						this._authenticated = false;
						reject(error);
					}
				} else {
					this.emit("connect");
					resolve();
				}
			});

			this._socket.on("data", (data: string | Buffer) => {
				// For non-SASL connections, data will be strings (utf8 encoding set)
				// For SASL connections, data handler is managed by binary* methods
				if (typeof data === "string") {
					this.handleData(data);
				}
			});

			this._socket.on("error", (error: Error) => {
				this.emit("error", error);
				if (!this._connected) {
					/* v8 ignore next -- @preserve */
					reject(error);
				}
			});

			this._socket.on("close", () => {
				this._connected = false;
				this._authenticated = false;
				this.emit("close");
				this.rejectPendingCommands(new Error("Connection closed"));
			});

			this._socket.on("timeout", () => {
				this.emit("timeout");
				this._socket?.destroy();
				reject(new Error("Connection timeout"));
			});
		});
	}

	/**
	 * Disconnect from the memcache server
	 */
	public async disconnect(): Promise<void> {
		/* v8 ignore next -- @preserve */
		if (this._socket) {
			this._socket.destroy();
			this._socket = undefined;
			this._connected = false;
		}
	}

	/**
	 * Reconnect to the memcache server by disconnecting and connecting again
	 */
	public async reconnect(): Promise<void> {
		// First disconnect if currently connected
		if (this._connected || this._socket) {
			await this.disconnect();
			// Clear any pending commands with a reconnection error
			this.rejectPendingCommands(
				new Error("Connection reset for reconnection"),
			);
			// Clear the buffer and current command state
			this._buffer = "";
			this._currentCommand = undefined;
			this._multilineData = [];
			this._pendingValueBytes = 0;
			this._authenticated = false;
			this._binaryBuffer = Buffer.alloc(0);
		}

		// Now establish a fresh connection
		await this.connect();
	}

	/**
	 * Perform SASL PLAIN authentication using the binary protocol
	 */
	private async performSaslAuth(): Promise<void> {
		/* v8 ignore next 3 -- @preserve */
		if (!this._sasl || !this._socket) {
			throw new Error("SASL credentials not configured");
		}

		// Capture references before entering the Promise to satisfy TypeScript
		const socket = this._socket;
		const sasl = this._sasl;

		return new Promise((resolve, reject) => {
			this._binaryBuffer = Buffer.alloc(0);

			const authPacket = buildSaslPlainRequest(sasl.username, sasl.password);

			// Temporary binary data handler for SASL authentication
			const binaryHandler = (data: Buffer) => {
				this._binaryBuffer = Buffer.concat([this._binaryBuffer, data]);

				// Need at least header size to parse response
				/* v8 ignore next 3 -- @preserve */
				if (this._binaryBuffer.length < HEADER_SIZE) {
					return;
				}

				const header = deserializeHeader(this._binaryBuffer);
				const totalLength = HEADER_SIZE + header.totalBodyLength;

				// Wait for complete packet
				/* v8 ignore next 3 -- @preserve */
				if (this._binaryBuffer.length < totalLength) {
					return;
				}

				// Remove this temporary handler
				socket.removeListener("data", binaryHandler);

				if (header.status === STATUS_SUCCESS) {
					this._authenticated = true;
					this.emit("authenticated");
					resolve();
				} else if (header.status === STATUS_AUTH_ERROR) {
					const body = this._binaryBuffer.subarray(HEADER_SIZE, totalLength);
					reject(
						new Error(
							`SASL authentication failed: ${body.toString() || "Invalid credentials"}`,
						),
					);
				} else {
					/* v8 ignore next -- @preserve */
					reject(
						new Error(
							`SASL authentication failed with status: 0x${header.status.toString(16)}`,
						),
					);
				}
			};

			socket.on("data", binaryHandler);
			socket.write(authPacket);
		});
	}

	/**
	 * Send a binary protocol request and wait for response.
	 * Used internally for SASL-authenticated connections.
	 */
	private async binaryRequest(packet: Buffer): Promise<Buffer> {
		/* v8 ignore next 3 -- @preserve */
		if (!this._socket) {
			throw new Error("Not connected");
		}

		const socket = this._socket;

		return new Promise((resolve) => {
			let buffer = Buffer.alloc(0);

			const dataHandler = (data: Buffer) => {
				buffer = Buffer.concat([buffer, data]);

				/* v8 ignore next 3 -- @preserve */
				if (buffer.length < HEADER_SIZE) {
					return;
				}

				const header = deserializeHeader(buffer);
				const totalLength = HEADER_SIZE + header.totalBodyLength;

				/* v8 ignore next 3 -- @preserve */
				if (buffer.length < totalLength) {
					return;
				}

				socket.removeListener("data", dataHandler);
				resolve(buffer.subarray(0, totalLength));
			};

			socket.on("data", dataHandler);
			socket.write(packet);
		});
	}

	/**
	 * Binary protocol GET operation
	 */
	public async binaryGet(key: string): Promise<string | undefined> {
		const response = await this.binaryRequest(buildGetRequest(key));
		const { header, value } = parseGetResponse(response);

		if (header.status === STATUS_KEY_NOT_FOUND) {
			this.emit("miss", key);
			return undefined;
		}

		/* v8 ignore next 3 -- @preserve */
		if (header.status !== STATUS_SUCCESS || !value) {
			return undefined;
		}

		const result = value.toString("utf8");
		this.emit("hit", key, result);
		return result;
	}

	/**
	 * Binary protocol SET operation
	 */
	public async binarySet(
		key: string,
		value: string,
		exptime = 0,
		flags = 0,
	): Promise<boolean> {
		const response = await this.binaryRequest(
			buildSetRequest(key, value, flags, exptime),
		);
		const header = deserializeHeader(response);
		return header.status === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol ADD operation
	 */
	public async binaryAdd(
		key: string,
		value: string,
		exptime = 0,
		flags = 0,
	): Promise<boolean> {
		const response = await this.binaryRequest(
			buildAddRequest(key, value, flags, exptime),
		);
		const header = deserializeHeader(response);
		return header.status === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol REPLACE operation
	 */
	public async binaryReplace(
		key: string,
		value: string,
		exptime = 0,
		flags = 0,
	): Promise<boolean> {
		const response = await this.binaryRequest(
			buildReplaceRequest(key, value, flags, exptime),
		);
		const header = deserializeHeader(response);
		return header.status === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol DELETE operation
	 */
	public async binaryDelete(key: string): Promise<boolean> {
		const response = await this.binaryRequest(buildDeleteRequest(key));
		const header = deserializeHeader(response);
		return (
			header.status === STATUS_SUCCESS || header.status === STATUS_KEY_NOT_FOUND
		);
	}

	/**
	 * Binary protocol INCREMENT operation
	 */
	public async binaryIncr(
		key: string,
		delta = 1,
		initial = 0,
		exptime = 0,
	): Promise<number | undefined> {
		const response = await this.binaryRequest(
			buildIncrementRequest(key, delta, initial, exptime),
		);
		const { header, value } = parseIncrDecrResponse(response);

		/* v8 ignore next 3 -- @preserve */
		if (header.status !== STATUS_SUCCESS) {
			return undefined;
		}

		return value;
	}

	/**
	 * Binary protocol DECREMENT operation
	 */
	public async binaryDecr(
		key: string,
		delta = 1,
		initial = 0,
		exptime = 0,
	): Promise<number | undefined> {
		const response = await this.binaryRequest(
			buildDecrementRequest(key, delta, initial, exptime),
		);
		const { header, value } = parseIncrDecrResponse(response);

		/* v8 ignore next 3 -- @preserve */
		if (header.status !== STATUS_SUCCESS) {
			return undefined;
		}

		return value;
	}

	/**
	 * Binary protocol APPEND operation
	 */
	public async binaryAppend(key: string, value: string): Promise<boolean> {
		const response = await this.binaryRequest(buildAppendRequest(key, value));
		const header = deserializeHeader(response);
		return header.status === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol PREPEND operation
	 */
	public async binaryPrepend(key: string, value: string): Promise<boolean> {
		const response = await this.binaryRequest(buildPrependRequest(key, value));
		const header = deserializeHeader(response);
		return header.status === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol TOUCH operation
	 */
	public async binaryTouch(key: string, exptime: number): Promise<boolean> {
		const response = await this.binaryRequest(buildTouchRequest(key, exptime));
		const header = deserializeHeader(response);
		return header.status === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol FLUSH operation
	 */
	public async binaryFlush(exptime = 0): Promise<boolean> {
		/* v8 ignore next -- @preserve */
		const response = await this.binaryRequest(buildFlushRequest(exptime));
		/* v8 ignore next -- @preserve */
		const header = deserializeHeader(response);
		/* v8 ignore next -- @preserve */
		return header.status === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol VERSION operation
	 */
	public async binaryVersion(): Promise<string | undefined> {
		const response = await this.binaryRequest(buildVersionRequest());
		const header = deserializeHeader(response);

		/* v8 ignore next -- @preserve */
		if (header.status !== STATUS_SUCCESS) {
			return undefined;
		}

		return response
			.subarray(HEADER_SIZE, HEADER_SIZE + header.totalBodyLength)
			.toString("utf8");
	}

	/**
	 * Binary protocol STATS operation
	 */
	public async binaryStats(): Promise<Record<string, string>> {
		/* v8 ignore next -- @preserve */
		if (!this._socket) {
			throw new Error("Not connected");
		}

		const socket = this._socket;
		const stats: Record<string, string> = {};

		return new Promise((resolve) => {
			let buffer = Buffer.alloc(0);

			const dataHandler = (data: Buffer) => {
				buffer = Buffer.concat([buffer, data]);

				while (buffer.length >= HEADER_SIZE) {
					const header = deserializeHeader(buffer);
					const totalLength = HEADER_SIZE + header.totalBodyLength;

					/* v8 ignore next -- @preserve */
					if (buffer.length < totalLength) {
						return;
					}

					// Empty key means end of stats
					if (header.keyLength === 0 && header.totalBodyLength === 0) {
						socket.removeListener("data", dataHandler);
						resolve(stats);
						return;
					}

					if (
						header.opcode === OPCODE_STAT &&
						header.status === STATUS_SUCCESS
					) {
						const keyStart = HEADER_SIZE;
						const keyEnd = keyStart + header.keyLength;
						const valueEnd = HEADER_SIZE + header.totalBodyLength;

						const key = buffer.subarray(keyStart, keyEnd).toString("utf8");
						const value = buffer.subarray(keyEnd, valueEnd).toString("utf8");
						stats[key] = value;
					}

					buffer = buffer.subarray(totalLength);
				}
			};

			socket.on("data", dataHandler);
			socket.write(buildStatRequest());
		});
	}

	/**
	 * Binary protocol QUIT operation
	 */
	public async binaryQuit(): Promise<void> {
		if (this._socket) {
			this._socket.write(buildQuitRequest());
		}
	}

	/**
	 * Gracefully quit the connection (send quit command then disconnect)
	 */
	public async quit(): Promise<void> {
		/* v8 ignore next -- @preserve */
		if (this._connected && this._socket) {
			try {
				await this.command("quit");
				// biome-ignore lint/correctness/noUnusedVariables: expected
			} catch (error) {
				// Ignore errors from quit command as the server closes the connection
			}
			await this.disconnect();
		}
	}

	/**
	 * Check if connected to the memcache server
	 */
	public isConnected(): boolean {
		return this._connected;
	}

	/**
	 * Send a generic command to the memcache server
	 * @param cmd The command string to send (without trailing \r\n)
	 * @param options Command options for response parsing
	 */
	public async command(
		cmd: string,
		options?: CommandOptions,
		// biome-ignore lint/suspicious/noExplicitAny: expected
	): Promise<any> {
		if (!this._connected || !this._socket) {
			throw new Error(`Not connected to memcache server ${this.id}`);
		}

		return new Promise((resolve, reject) => {
			this._commandQueue.push({
				command: cmd,
				resolve,
				reject,
				isMultiline: options?.isMultiline,
				isStats: options?.isStats,
				requestedKeys: options?.requestedKeys,
			});
			// biome-ignore lint/style/noNonNullAssertion: socket is checked
			this._socket!.write(`${cmd}\r\n`);
		});
	}

	private handleData(data: string): void {
		this._buffer += data;

		while (true) {
			// If we're waiting for value data, try to read it first
			if (this._pendingValueBytes > 0) {
				if (this._buffer.length >= this._pendingValueBytes + 2) {
					const value = this._buffer.substring(0, this._pendingValueBytes);
					this._buffer = this._buffer.substring(this._pendingValueBytes + 2);
					this._multilineData.push(value);
					this._pendingValueBytes = 0;
				} else {
					// Not enough data yet, wait for more
					break;
				}
			}

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
					/* v8 ignore next -- @preserve */
					if (key && value) {
						stats[key] = value;
					}
				}
				this._currentCommand.resolve(stats);
				this._multilineData = [];
				this._currentCommand = undefined;
				return;
			}

			if (line.startsWith("STAT ")) {
				this._multilineData.push(line);
				return;
			}

			if (
				line.startsWith("ERROR") ||
				line.startsWith("CLIENT_ERROR") ||
				line.startsWith("SERVER_ERROR")
			) {
				this._currentCommand.reject(new Error(line));
				this._currentCommand = undefined;
				return;
			}

			return;
		}

		if (this._currentCommand.isMultiline) {
			// Track found keys only if requestedKeys is provided
			if (
				this._currentCommand.requestedKeys &&
				!this._currentCommand.foundKeys
			) {
				this._currentCommand.foundKeys = [];
			}

			if (line.startsWith("VALUE ")) {
				const parts = line.split(" ");
				const key = parts[1];
				const bytes = parseInt(parts[3], 10);
				if (this._currentCommand.requestedKeys) {
					this._currentCommand.foundKeys?.push(key);
				}
				// Set pending bytes so handleData will read the value
				this._pendingValueBytes = bytes;
			} else if (line === "END") {
				let result:
					| string[]
					| { values: string[] | undefined; foundKeys: string[] }
					| undefined;

				// If requestedKeys is present, return object with keys and values
				if (
					this._currentCommand.requestedKeys &&
					this._currentCommand.foundKeys
				) {
					result = {
						values:
							this._multilineData.length > 0 ? this._multilineData : undefined,
						foundKeys: this._currentCommand.foundKeys,
					};
				} else {
					result =
						this._multilineData.length > 0 ? this._multilineData : undefined;
				}

				// Emit hit/miss events if we have requested keys
				/* v8 ignore next -- @preserve */
				if (
					this._currentCommand.requestedKeys &&
					this._currentCommand.foundKeys
				) {
					const foundKeys = this._currentCommand.foundKeys;
					for (let i = 0; i < foundKeys.length; i++) {
						this.emit("hit", foundKeys[i], this._multilineData[i]);
					}

					// Emit miss events for keys that weren't found
					const missedKeys = this._currentCommand.requestedKeys.filter(
						(key) => !foundKeys.includes(key),
					);
					for (const key of missedKeys) {
						this.emit("miss", key);
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

	private rejectPendingCommands(error: Error): void {
		if (this._currentCommand) {
			/* v8 ignore next -- @preserve */
			this._currentCommand.reject(error);
			/* v8 ignore next -- @preserve */
			this._currentCommand = undefined;
		}
		while (this._commandQueue.length > 0) {
			const cmd = this._commandQueue.shift();
			/* v8 ignore next -- @preserve */
			if (cmd) {
				cmd.reject(error);
			}
		}
	}
}

/**
 * Factory function to create a new MemcacheNode instance.
 * @param host - The hostname or IP address of the memcache server
 * @param port - The port number of the memcache server
 * @param options - Optional configuration for the node
 * @returns A new MemcacheNode instance
 *
 * @example
 * ```typescript
 * const node = createNode('localhost', 11211, {
 *   timeout: 5000,
 *   keepAlive: true,
 *   weight: 1
 * });
 * await node.connect();
 * ```
 */
export function createNode(
	host: string,
	port: number,
	options?: MemcacheNodeOptions,
): MemcacheNode {
	return new MemcacheNode(host, port, options);
}
