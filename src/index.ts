import { createConnection, type Socket } from "node:net";
import { Hookified } from "hookified";

export interface MemcacheOptions {
	host?: string;
	port?: number;
	timeout?: number;
	keepAlive?: boolean;
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

	constructor(options: MemcacheOptions = {}) {
		super();
		this._host = options.host || "localhost";
		this._port = options.port || 11211;
		this._timeout = options.timeout || 5000;
		this._keepAlive = options.keepAlive !== false;
		this._keepAliveDelay = options.keepAliveDelay || 1000;
	}

	// Getters and Setters
	public get socket(): Socket | undefined {
		return this._socket;
	}

	public set socket(value: Socket | undefined) {
		this._socket = value;
	}

	public get host(): string {
		return this._host;
	}

	public set host(value: string) {
		this._host = value;
	}

	public get port(): number {
		return this._port;
	}

	public set port(value: number) {
		this._port = value;
	}

	public get timeout(): number {
		return this._timeout;
	}

	public set timeout(value: number) {
		this._timeout = value;
	}

	public get keepAlive(): boolean {
		return this._keepAlive;
	}

	public set keepAlive(value: boolean) {
		this._keepAlive = value;
	}

	public get keepAliveDelay(): number {
		return this._keepAliveDelay;
	}

	public set keepAliveDelay(value: number) {
		this._keepAliveDelay = value;
	}

	public get connected(): boolean {
		return this._connected;
	}

	public set connected(value: boolean) {
		this._connected = value;
	}

	public get commandQueue(): CommandQueueItem[] {
		return this._commandQueue;
	}

	public set commandQueue(value: CommandQueueItem[]) {
		this._commandQueue = value;
	}

	public get buffer(): string {
		return this._buffer;
	}

	public set buffer(value: string) {
		this._buffer = value;
	}

	public get currentCommand(): CommandQueueItem | undefined {
		return this._currentCommand;
	}

	public set currentCommand(value: CommandQueueItem | undefined) {
		this._currentCommand = value;
	}

	public get multilineData(): string[] {
		return this._multilineData;
	}

	public set multilineData(value: string[]) {
		this._multilineData = value;
	}

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

	public async get(key: string): Promise<string | undefined> {
		this.validateKey(key);
		const result = await this.sendCommand(`get ${key}`, true);
		return result && result.length > 0 ? result[0] : undefined;
	}

	public async gets(keys: string[]): Promise<Map<string, string>> {
		for (const key of keys) {
			this.validateKey(key);
		}
		const keysStr = keys.join(" ");
		const results = (await this.sendCommand(`get ${keysStr}`, true)) as
			| string[]
			| undefined;
		const map = new Map<string, string>();

		if (results) {
			for (let i = 0; i < keys.length && i < results.length; i++) {
				map.set(keys[i], results[i]);
			}
		}

		return map;
	}

	public async set(
		key: string,
		value: string,
		exptime: number = 0,
		flags: number = 0,
	): Promise<boolean> {
		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `set ${key} ${flags} ${exptime} ${bytes}\r\n${valueStr}`;
		const result = await this.sendCommand(command);
		return result === "STORED";
	}

	public async add(
		key: string,
		value: string,
		exptime: number = 0,
		flags: number = 0,
	): Promise<boolean> {
		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `add ${key} ${flags} ${exptime} ${bytes}\r\n${valueStr}`;
		const result = await this.sendCommand(command);
		return result === "STORED";
	}

	public async replace(
		key: string,
		value: string,
		exptime: number = 0,
		flags: number = 0,
	): Promise<boolean> {
		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `replace ${key} ${flags} ${exptime} ${bytes}\r\n${valueStr}`;
		const result = await this.sendCommand(command);
		return result === "STORED";
	}

	public async append(key: string, value: string): Promise<boolean> {
		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `append ${key} 0 0 ${bytes}\r\n${valueStr}`;
		const result = await this.sendCommand(command);
		return result === "STORED";
	}

	public async prepend(key: string, value: string): Promise<boolean> {
		this.validateKey(key);
		const valueStr = String(value);
		const bytes = Buffer.byteLength(valueStr);
		const command = `prepend ${key} 0 0 ${bytes}\r\n${valueStr}`;
		const result = await this.sendCommand(command);
		return result === "STORED";
	}

	public async delete(key: string): Promise<boolean> {
		this.validateKey(key);
		const result = await this.sendCommand(`delete ${key}`);
		return result === "DELETED";
	}

	public async incr(
		key: string,
		value: number = 1,
	): Promise<number | undefined> {
		this.validateKey(key);
		const result = await this.sendCommand(`incr ${key} ${value}`);
		return typeof result === "number" ? result : undefined;
	}

	public async decr(
		key: string,
		value: number = 1,
	): Promise<number | undefined> {
		this.validateKey(key);
		const result = await this.sendCommand(`decr ${key} ${value}`);
		return typeof result === "number" ? result : undefined;
	}

	public async touch(key: string, exptime: number): Promise<boolean> {
		this.validateKey(key);
		const result = await this.sendCommand(`touch ${key} ${exptime}`);
		return result === "TOUCHED";
	}

	public async flush(): Promise<boolean> {
		const result = await this.sendCommand("flush_all");
		return result === "OK";
	}

	public async flushAll(delay?: number): Promise<boolean> {
		const command = delay !== undefined ? `flush_all ${delay}` : "flush_all";
		const result = await this.sendCommand(command);
		return result === "OK";
	}

	public async stats(type?: string): Promise<MemcacheStats> {
		const command = type ? `stats ${type}` : "stats";
		return await this.sendCommand(command, false, true);
	}

	public async version(): Promise<string> {
		const result = await this.sendCommand("version");
		return result;
	}

	public async quit(): Promise<void> {
		if (this._connected && this._socket) {
			try {
				await this.sendCommand("quit");
				// biome-ignore lint/correctness/noUnusedVariables: expected
			} catch (error) {
				// Ignore errors from quit command as the server closes the connection
			}
			this._socket.end();
			this._connected = false;
		}
	}

	public disconnect(): void {
		if (this._socket) {
			this._socket.destroy();
			this._socket = undefined;
			this._connected = false;
		}
	}

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
				for (const statLine of this.multilineData) {
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
				const bytes = parseInt(parts[3], 10);
				this.readValue(bytes);
			} else if (line === "END") {
				const result =
					this.multilineData.length > 0 ? this.multilineData : undefined;
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
		const valueEnd = this.buffer.indexOf("\r\n");
		if (valueEnd >= bytes) {
			const value = this.buffer.substring(0, bytes);
			this.buffer = this.buffer.substring(bytes + 2);
			this.multilineData.push(value);
		}
	}

	private async sendCommand(
		command: string,
		isMultiline: boolean = false,
		isStats: boolean = false,
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
