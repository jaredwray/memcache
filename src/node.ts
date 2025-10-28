import { EventEmitter } from "node:events";
import { createConnection, type Socket } from "node:net";

export interface MemcacheNodeOptions {
	timeout?: number;
	keepAlive?: boolean;
	keepAliveDelay?: number;
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
export class MemcacheNode extends EventEmitter {
	private _host: string;
	private _port: number;
	private _socket: Socket | undefined = undefined;
	private _timeout: number;
	private _keepAlive: boolean;
	private _keepAliveDelay: number;
	private _connected: boolean = false;
	private _commandQueue: CommandQueueItem[] = [];
	private _buffer: string = "";
	private _currentCommand: CommandQueueItem | undefined = undefined;
	private _multilineData: string[] = [];

	constructor(host: string, port: number, options?: MemcacheNodeOptions) {
		super();
		this._host = host;
		this._port = port;
		this._timeout = options?.timeout || 5000;
		this._keepAlive = options?.keepAlive !== false;
		this._keepAliveDelay = options?.keepAliveDelay || 1000;
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
	 * Get the socket connection
	 */
	public get socket(): Socket | undefined {
		return this._socket;
	}

	/**
	 * Get the command queue
	 */
	public get commandQueue(): CommandQueueItem[] {
		return this._commandQueue;
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
			this._socket.setEncoding("utf8");

			this._socket.on("connect", () => {
				this._connected = true;
				this.emit("connect");
				resolve();
			});

			this._socket.on("data", (data: string) => {
				this.handleData(data);
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
				this.readValue(bytes);
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

	private readValue(bytes: number): void {
		const valueEnd = this._buffer.indexOf("\r\n");
		/* v8 ignore next -- @preserve */
		if (valueEnd >= bytes) {
			const value = this._buffer.substring(0, bytes);
			this._buffer = this._buffer.substring(bytes + 2);
			this._multilineData.push(value);
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
