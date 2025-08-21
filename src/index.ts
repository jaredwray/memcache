import { Hookified } from "hookified";
import { createConnection, type Socket } from "net";

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

export class Memcache extends Hookified {
	private _socket: Socket | null = null;
	private _host: string;
	private _port: number;
	private _timeout: number;
	private _keepAlive: boolean;
	private _keepAliveDelay: number;
	private _connected: boolean = false;
	private _commandQueue: Array<{
		command: string;
		resolve: (value: any) => void;
		reject: (reason?: any) => void;
		isMultiline?: boolean;
		isStats?: boolean;
	}> = [];
	private _buffer: string = "";
	private _currentCommand: any = null;
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
	public get socket(): Socket | null {
		return this._socket;
	}

	public set socket(value: Socket | null) {
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

	public get commandQueue(): Array<{
		command: string;
		resolve: (value: any) => void;
		reject: (reason?: any) => void;
		isMultiline?: boolean;
		isStats?: boolean;
	}> {
		return this._commandQueue;
	}

	public set commandQueue(value: Array<{
		command: string;
		resolve: (value: any) => void;
		reject: (reason?: any) => void;
		isMultiline?: boolean;
		isStats?: boolean;
	}>) {
		this._commandQueue = value;
	}

	public get buffer(): string {
		return this._buffer;
	}

	public set buffer(value: string) {
		this._buffer = value;
	}

	public get currentCommand(): any {
		return this._currentCommand;
	}

	public set currentCommand(value: any) {
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
			if (this.connected) {
				resolve();
				return;
			}

			this.socket = createConnection({
				host: this.host,
				port: this.port,
				keepAlive: this.keepAlive,
				keepAliveInitialDelay: this.keepAliveDelay,
			});

			this.socket.setTimeout(this.timeout);
			this.socket.setEncoding("utf8");

			this.socket.on("connect", () => {
				this.connected = true;
				this.emit("connect");
				resolve();
			});

			this.socket.on("data", (data: string) => {
				this.handleData(data);
			});

			this.socket.on("error", (error: Error) => {
				this.emit("error", error);
				if (!this.connected) {
					reject(error);
				}
			});

			this.socket.on("close", () => {
				this.connected = false;
				this.emit("close");
				this.rejectPendingCommands(new Error("Connection closed"));
			});

			this.socket.on("timeout", () => {
				this.emit("timeout");
				this.socket?.destroy();
				reject(new Error("Connection timeout"));
			});
		});
	}

	public async get(key: string): Promise<string | null> {
		this.validateKey(key);
		const result = await this.sendCommand(`get ${key}`, true);
		return result && result.length > 0 ? result[0] : null;
	}

	public async gets(keys: string[]): Promise<Map<string, string>> {
		for (const key of keys) {
			this.validateKey(key);
		}
		const keysStr = keys.join(" ");
		const results = (await this.sendCommand(`get ${keysStr}`, true)) as
			| string[]
			| null;
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

	public async incr(key: string, value: number = 1): Promise<number | null> {
		this.validateKey(key);
		const result = await this.sendCommand(`incr ${key} ${value}`);
		return typeof result === "number" ? result : null;
	}

	public async decr(key: string, value: number = 1): Promise<number | null> {
		this.validateKey(key);
		const result = await this.sendCommand(`decr ${key} ${value}`);
		return typeof result === "number" ? result : null;
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
		if (this.connected && this.socket) {
			try {
				await this.sendCommand("quit");
				// biome-ignore lint/correctness/noUnusedVariables: expected to be used
			} catch (error) {
				// Ignore errors from quit command as the server closes the connection
			}
			this.socket.end();
			this.connected = false;
		}
	}

	public disconnect(): void {
		if (this.socket) {
			this.socket.destroy();
			this.socket = null;
			this.connected = false;
		}
	}

	public isConnected(): boolean {
		return this.connected;
	}

	// Private methods
	private handleData(data: string): void {
		this.buffer += data;

		while (true) {
			const lineEnd = this.buffer.indexOf("\r\n");
			if (lineEnd === -1) break;

			const line = this.buffer.substring(0, lineEnd);
			this.buffer = this.buffer.substring(lineEnd + 2);

			this.processLine(line);
		}
	}

	private processLine(line: string): void {
		if (!this.currentCommand) {
			this.currentCommand = this.commandQueue.shift();
			if (!this.currentCommand) return;
		}

		if (this.currentCommand.isStats) {
			if (line === "END") {
				const stats: MemcacheStats = {};
				for (const statLine of this.multilineData) {
					const [, key, value] = statLine.split(" ");
					if (key && value) {
						stats[key] = value;
					}
				}
				this.currentCommand.resolve(stats);
				this.multilineData = [];
				this.currentCommand = null;
			} else if (line.startsWith("STAT ")) {
				this.multilineData.push(line);
			} else if (
				line.startsWith("ERROR") ||
				line.startsWith("CLIENT_ERROR") ||
				line.startsWith("SERVER_ERROR")
			) {
				this.currentCommand.reject(new Error(line));
				this.currentCommand = null;
			}
			return;
		}

		if (this.currentCommand.isMultiline) {
			if (line.startsWith("VALUE ")) {
				const parts = line.split(" ");
				const bytes = parseInt(parts[3], 10);
				this.readValue(bytes);
			} else if (line === "END") {
				const result =
					this.multilineData.length > 0 ? this.multilineData : null;
				this.currentCommand.resolve(result);
				this.multilineData = [];
				this.currentCommand = null;
			} else if (
				line.startsWith("ERROR") ||
				line.startsWith("CLIENT_ERROR") ||
				line.startsWith("SERVER_ERROR")
			) {
				this.currentCommand.reject(new Error(line));
				this.multilineData = [];
				this.currentCommand = null;
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
				this.currentCommand.resolve(line);
			} else if (line === "NOT_STORED") {
				this.currentCommand.resolve(false);
			} else if (
				line.startsWith("ERROR") ||
				line.startsWith("CLIENT_ERROR") ||
				line.startsWith("SERVER_ERROR")
			) {
				this.currentCommand.reject(new Error(line));
			} else if (/^\d+$/.test(line)) {
				this.currentCommand.resolve(parseInt(line, 10));
			} else {
				this.currentCommand.resolve(line);
			}
			this.currentCommand = null;
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

	private sendCommand(
		command: string,
		isMultiline: boolean = false,
		isStats: boolean = false,
	): Promise<any> {
		return new Promise((resolve, reject) => {
			if (!this.connected || !this.socket) {
				reject(new Error("Not connected to memcache server"));
				return;
			}

			this.commandQueue.push({
				command,
				resolve,
				reject,
				isMultiline,
				isStats,
			});
			this.socket.write(command + "\r\n");
		});
	}

	private rejectPendingCommands(error: Error): void {
		if (this.currentCommand) {
			this.currentCommand.reject(error);
			this.currentCommand = null;
		}
		while (this.commandQueue.length > 0) {
			const cmd = this.commandQueue.shift();
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
