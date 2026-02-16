import * as net from "node:net";

export interface FakeConfigServerOptions {
	version?: number;
	nodes?: string[];
	respondEmpty?: boolean;
	errorResponse?: string;
	responseDelay?: number;
}

/**
 * A minimal TCP server that speaks enough of the memcache/ElastiCache protocol
 * to respond to `config get cluster` and `get AmazonElastiCache:cluster` commands.
 * Used for integration testing AutoDiscovery without mocks.
 */
export class FakeConfigServer {
	private _server: net.Server | undefined;
	private _port = 0;
	private _connections: net.Socket[] = [];

	public version: number;
	public nodes: string[];
	public respondEmpty: boolean;
	public errorResponse: string | undefined;
	public responseDelay: number;

	constructor(options?: FakeConfigServerOptions) {
		this.version = options?.version ?? 1;
		this.nodes = options?.nodes ?? [];
		this.respondEmpty = options?.respondEmpty ?? false;
		this.errorResponse = options?.errorResponse;
		this.responseDelay = options?.responseDelay ?? 0;
	}

	get port(): number {
		return this._port;
	}

	get endpoint(): string {
		return `127.0.0.1:${this._port}`;
	}

	async start(): Promise<void> {
		return new Promise((resolve) => {
			this._server = net.createServer((socket) => {
				this._connections.push(socket);
				socket.on("close", () => {
					this._connections = this._connections.filter((s) => s !== socket);
				});

				socket.setEncoding("utf8");
				let buffer = "";

				socket.on("data", (data: string) => {
					buffer += data;
					let lineEnd = buffer.indexOf("\r\n");
					while (lineEnd !== -1) {
						const line = buffer.substring(0, lineEnd);
						buffer = buffer.substring(lineEnd + 2);
						this.handleCommand(socket, line);
						lineEnd = buffer.indexOf("\r\n");
					}
				});
			});

			this._server.listen(0, "127.0.0.1", () => {
				const addr = this._server?.address() as net.AddressInfo;
				this._port = addr.port;
				resolve();
			});
		});
	}

	async stop(): Promise<void> {
		for (const socket of this._connections) {
			socket.destroy();
		}
		this._connections = [];

		return new Promise((resolve) => {
			if (this._server) {
				this._server.close(() => {
					this._server = undefined;
					resolve();
				});
			} else {
				resolve();
			}
		});
	}

	destroyAllConnections(): void {
		for (const socket of this._connections) {
			socket.destroy();
		}
		this._connections = [];
	}

	private handleCommand(socket: net.Socket, command: string): void {
		const respond = (data: string) => {
			if (this.responseDelay > 0) {
				setTimeout(() => {
					if (!socket.destroyed) {
						socket.write(data);
					}
				}, this.responseDelay);
			} else {
				if (!socket.destroyed) {
					socket.write(data);
				}
			}
		};

		if (this.errorResponse) {
			respond(`${this.errorResponse}\r\n`);
			return;
		}

		if (this.respondEmpty) {
			respond("END\r\n");
			return;
		}

		const payload = `${this.version}\n${this.nodes.join(" ")}\n`;
		const bytes = Buffer.byteLength(payload);

		if (command === "config get cluster") {
			respond(`CONFIG cluster 0 ${bytes}\r\n${payload}\r\nEND\r\n`);
		} else if (command === "get AmazonElastiCache:cluster") {
			respond(
				`VALUE AmazonElastiCache:cluster 0 ${bytes}\r\n${payload}\r\nEND\r\n`,
			);
		} else {
			respond("ERROR\r\n");
		}
	}
}
