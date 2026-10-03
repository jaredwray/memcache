import { createConnection, type Socket } from "node:net";
import {
	connect as createTlsConnection,
	type ConnectionOptions as TlsConnectionOptions,
} from "node:tls";
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
	OPCODE_QUIT,
	OPCODE_STAT,
	parseGetResponse,
	parseIncrDecrResponse,
	RESPONSE_MAGIC,
	readStatus,
	STATUS_AUTH_ERROR,
	STATUS_KEY_NOT_FOUND,
	STATUS_SUCCESS,
} from "./binary-protocol.js";
import { Queue } from "./queue.js";
import type { SASLCredentials } from "./types.js";

/**
 * TLS configuration for node connections.
 * - `true`: connect using TLS with Node's default trust store.
 * - a `tls.ConnectionOptions` object: connect using TLS with the provided
 *   options (e.g. `{ ca }` for a private certificate authority).
 * - `false` / `undefined`: connect using plain TCP.
 */
export type MemcacheTlsOption = boolean | TlsConnectionOptions;

export interface MemcacheNodeOptions {
	timeout?: number;
	/**
	 * The most requests the node keeps waiting for a response, or for its
	 * connection to open (see `connectForRequest()`). A request made while
	 * that many are pending fails at once instead of joining them. `0`, or
	 * anything below 1 or not finite, means no limit.
	 * @default 0
	 */
	maxPendingCommands?: number;
	keepAlive?: boolean;
	keepAliveDelay?: number;
	weight?: number;
	/** SASL authentication credentials */
	sasl?: SASLCredentials;
	/**
	 * Enable TLS for this node's connection.
	 * `true` uses Node's default trust store; a `tls.ConnectionOptions`
	 * object is passed through to `tls.connect()` (e.g. `{ ca }`).
	 * @default undefined (plain TCP)
	 */
	tls?: MemcacheTlsOption;
}

export interface CommandOptions {
	isMultiline?: boolean;
	isStats?: boolean;
	isConfig?: boolean;
	requestedKeys?: string[];
	/**
	 * The data block of a storage command (`set`, `cas`, ...), written after
	 * the command line and its \r\n, then followed by its own \r\n. Passing a
	 * large value here, instead of joining it to the command string, saves
	 * copying it before it is encoded.
	 */
	data?: string;
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
	isConfig?: boolean;
	requestedKeys?: string[];
	foundKeys?: string[];
};

/**
 * A binary protocol request waiting for its response. memcached answers
 * binary requests on a connection in the order they were sent, and copies
 * each request's `opaque` value into its response packets.
 */
type BinaryQueueItem = {
	opaque: number;
	/** Handles one response packet and returns `true` once the request is complete. */
	onPacket: (packet: Buffer) => boolean;
	// biome-ignore lint/suspicious/noExplicitAny: expected
	reject: (reason?: any) => void;
};

const CR = 13;
const LF = 10;

/** Held once every byte received has been parsed. */
const EMPTY_BUFFER = Buffer.alloc(0);

/**
 * The replies that are one fixed word, by length. A line that is one of
 * them gets the constant string instead of being decoded into a new one.
 */
const FIXED_LINES: Array<Array<[Buffer, string]>> = [];
for (const line of [
	"OK",
	"END",
	"ERROR",
	"EXISTS",
	"STORED",
	"DELETED",
	"TOUCHED",
	"NOT_FOUND",
	"NOT_STORED",
]) {
	const sameLength = FIXED_LINES[line.length] ?? [];
	sameLength.push([Buffer.from(line), line]);
	FIXED_LINES[line.length] = sameLength;
}

/**
 * Where the next line ends: the index of the CR of the first CRLF at or
 * after `from`, or -1 if no whole line is buffered yet. A CR only ends a
 * line when an LF follows it, so a CR that is the last byte buffered waits
 * for the next chunk.
 */
function findLineEnd(buffer: Buffer, from: number): number {
	let cr = buffer.indexOf(CR, from);
	while (cr !== -1 && cr + 1 < buffer.length) {
		if (buffer[cr + 1] === LF) {
			return cr;
		}

		cr = buffer.indexOf(CR, cr + 1);
	}

	return -1;
}

/**
 * The line from `start` to `end`, decoded, or the constant string when it
 * is one of the fixed replies.
 */
function readLine(buffer: Buffer, start: number, end: number): string {
	const fixed = FIXED_LINES[end - start];
	if (fixed !== undefined) {
		for (const [bytes, line] of fixed) {
			if (hasBytesAt(buffer, start, bytes)) {
				return line;
			}
		}
	}

	return buffer.toString("utf8", start, end);
}

/**
 * Whether `buffer` holds `bytes` at `start`. For words this short, a loop
 * is about 3x as fast as `Buffer.compare()` with offsets.
 */
function hasBytesAt(buffer: Buffer, start: number, bytes: Buffer): boolean {
	for (let i = 0; i < bytes.length; i++) {
		if (buffer[start + i] !== bytes[i]) {
			return false;
		}
	}

	return true;
}

/**
 * A pending request limit as a whole number of requests. Anything below 1,
 * or not a finite number, means no limit: 0.
 */
export function toPendingLimit(value: number | undefined): number {
	return Number.isFinite(value) ? Math.max(0, Math.floor(value as number)) : 0;
}

/**
 * The error a node gives a request it refuses because `maxPendingCommands`
 * requests are already pending. The client doesn't retry these.
 */
export class PendingLimitError extends Error {}

/**
 * MemcacheNode represents a single memcache server connection.
 * It handles the socket connection, command queue, and protocol parsing for one node.
 */
export class MemcacheNode extends Hookified {
	private _host: string;
	private _port: number;
	private _socket: Socket | undefined = undefined;
	private _connecting: Promise<void> | undefined = undefined;
	private _timeout: number;
	private _maxPendingCommands: number;
	private _overloadError: PendingLimitError | undefined;
	private _connectWaiters = 0;
	private _keepAlive: boolean;
	private _keepAliveDelay: number;
	private _weight: number;
	private _connected: boolean = false;
	private _commandQueue = new Queue<CommandQueueItem>();
	private _buffer: Buffer = Buffer.alloc(0);
	private _currentCommand: CommandQueueItem | undefined = undefined;
	private _multilineData: string[] = [];
	private _pendingValueBytes: number = 0;
	private _valueChunks: Buffer[] = [];
	private _valueLength: number = 0;
	private _sasl: SASLCredentials | undefined;
	private _tls: MemcacheTlsOption | undefined;
	private _authenticated: boolean = false;
	private _binaryQueue = new Queue<BinaryQueueItem>();
	private _binaryChunks: Buffer[] = [];
	private _binaryLength: number = 0;
	private _binaryOpaque: number = 0;
	private _deadline: ReturnType<typeof setTimeout> | undefined = undefined;
	private _waitingSince: number = 0;

	constructor(host: string, port: number, options?: MemcacheNodeOptions) {
		super({ throwOnEmptyListeners: false });
		this._host = host;
		this._port = port;
		this._timeout = options?.timeout || 5000;
		this._maxPendingCommands = toPendingLimit(options?.maxPendingCommands);
		this._keepAlive = options?.keepAlive !== false;
		this._keepAliveDelay = options?.keepAliveDelay || 1000;
		this._weight = options?.weight || 1;
		this._sasl = options?.sasl;
		this._tls = options?.tls;
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
		if (this._port === 0) {
			return this._host;
		}

		const host = this._host.includes(":") ? `[${this._host}]` : this._host;
		return `${host}:${this._port}`;
	}

	/**
	 * Get the full URI, e.g. `memcache://localhost:11211`.
	 * TLS-enabled nodes use `memcaches://` so the URI round-trips through
	 * `parseUri()` / `addNode()` without a client-level `tls` option.
	 */
	public get uri(): string {
		const scheme = this._tls ? "memcaches" : "memcache";
		return `${scheme}://${this.id}`;
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
	 * Get the timeout in milliseconds for opening a connection and for
	 * receiving a response while commands are pending
	 */
	public get timeout(): number {
		return this._timeout;
	}

	/**
	 * Set the timeout in milliseconds. Applies to the next connection attempt
	 * and to commands that are already waiting for a response.
	 */
	public set timeout(value: number) {
		this._timeout = value;
		if (this._deadline) {
			clearTimeout(this._deadline);
			this.scheduleDeadline(value - (performance.now() - this._waitingSince));
		}
	}

	/**
	 * Get the most requests the node keeps waiting for a response. `0` means
	 * no limit.
	 */
	public get maxPendingCommands(): number {
		return this._maxPendingCommands;
	}

	/**
	 * Set the most requests the node keeps waiting for a response. Requests
	 * made while that many are pending fail at once. `0`, or anything below
	 * 1 or not finite, means no limit.
	 */
	public set maxPendingCommands(value: number) {
		this._maxPendingCommands = toPendingLimit(value);
		this._overloadError = undefined;
	}

	/**
	 * Get the commands waiting for a response, oldest first. This is a copy,
	 * so changing it doesn't change the queue.
	 */
	public get commandQueue(): CommandQueueItem[] {
		return this._commandQueue.toArray();
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
	 * TLS option this node was constructed with (`true`, `false`, a
	 * `tls.ConnectionOptions` object, or `undefined` for plain TCP).
	 */
	public get tls(): MemcacheTlsOption | undefined {
		return this._tls;
	}

	/**
	 * Whether TLS is enabled for this node's connection.
	 */
	public get tlsEnabled(): boolean {
		return Boolean(this._tls);
	}

	/**
	 * Connect to the memcache server. Callers that arrive while a connection
	 * is being opened share it instead of opening another socket.
	 */
	public async connect(): Promise<void> {
		if (this._connecting) {
			return this._connecting;
		}

		if (this._connected) {
			return;
		}

		const connecting = this.openSocket().finally(() => {
			// disconnect() may have abandoned this attempt and started another
			if (this._connecting === connecting) {
				this._connecting = undefined;
			}
		});
		this._connecting = connecting;
		return connecting;
	}

	/**
	 * Connect for a request that is sent once the connection is open. With
	 * `maxPendingCommands` set, the requests waiting for the connection
	 * count toward it, so a server that can't be reached can't gather an
	 * unbounded backlog either: past the limit, this rejects at once with
	 * the error a refused command gets. The client's commands connect this
	 * way.
	 */
	public connectForRequest(): Promise<void> {
		if (this._maxPendingCommands === 0) {
			return this.connect();
		}

		const overload = this.overloadError(this._connectWaiters);
		if (overload) {
			return Promise.reject(overload);
		}

		this._connectWaiters++;
		return this.connect().finally(() => {
			this._connectWaiters--;
		});
	}

	/**
	 * Open a socket and wait until it is ready (and authenticated, when SASL is
	 * configured). The socket's handlers only change the node's state while it
	 * is still the node's socket, so a replaced socket can't close, time out or
	 * feed data into the one in use.
	 */
	private openSocket(): Promise<void> {
		return new Promise((resolve, reject) => {
			const socket = this._tls
				? createTlsConnection(this.buildTlsConnectOptions(this._tls))
				: createConnection({
						host: this._host,
						port: this._port,
						keepAlive: this._keepAlive,
						keepAliveInitialDelay: this._keepAliveDelay,
					});
			this._socket = socket;
			let ready = false;

			// The socket timeout only covers opening the connection
			socket.setTimeout(this._timeout);
			socket.setNoDelay(true);

			// For TLS connections readiness is "secureConnect" (handshake
			// complete). Resolving on "connect" would allow commands to be
			// written into an unfinished TLS handshake.
			const readyEvent = this._tls ? "secureConnect" : "connect";

			socket.on(readyEvent, async () => {
				ready = true;
				this._connected = true;
				// From here the command deadline covers waiting for responses,
				// including SASL authentication, and an idle connection stays open
				socket.setTimeout(0);

				// If SASL credentials are configured, authenticate before resolving
				if (this._sasl) {
					try {
						await this.performSaslAuth();
						// Keep socket in binary mode - SASL servers require binary protocol
						// for all commands. Use binary* methods for operations.
					} catch (error) {
						this.dropSocket(socket, error as Error);
						socket.destroy();
						reject(error);
						return;
					}
				}

				this.emit("connect");
				resolve();
			});

			socket.on("data", (data: Buffer) => {
				if (socket !== this._socket) {
					return;
				}

				// Response bytes are progress; restart the command deadline
				this._waitingSince = performance.now();

				// SASL connections only speak the binary protocol. Other
				// connections are text unless binary requests are waiting.
				if (this._sasl || this._binaryQueue.length > 0) {
					this.handleBinaryData(data);
				} else {
					this.handleData(data);
				}
			});

			socket.on("error", (error: Error) => {
				this.emit("error", error);
				if (!ready) {
					/* v8 ignore next -- @preserve */
					reject(error);
				}
			});

			socket.on("close", () => {
				this.dropSocket(socket, new Error("Connection closed"));
				this.emit("close");
				// Settles connect() if the socket closed before it was ready, for
				// example after disconnect(). Does nothing once it has resolved.
				reject(new Error("Connection closed"));
			});

			socket.on("timeout", () => {
				this.emit("timeout");
				socket.destroy();
				reject(new Error("Connection timeout"));
			});
		});
	}

	/**
	 * Whether any text command or binary request is waiting for a response.
	 */
	private hasPendingCommands(): boolean {
		return (
			this._currentCommand !== undefined ||
			this._commandQueue.length > 0 ||
			this._binaryQueue.length > 0
		);
	}

	/**
	 * The error for a request made while `maxPendingCommands` requests are
	 * already waiting, or undefined when there is room for it. `waiting`
	 * adds requests that are not in the queues, such as those waiting for
	 * the connection. Every refused request gets the same Error, built once
	 * per limit: an Error with its stack trace for each one made refusing a
	 * request cost 3x as much as queueing it.
	 */
	private overloadError(waiting = 0): PendingLimitError | undefined {
		const limit = this._maxPendingCommands;
		if (
			limit > 0 &&
			waiting +
				(this._currentCommand ? 1 : 0) +
				this._commandQueue.length +
				this._binaryQueue.length >=
				limit
		) {
			this._overloadError ??= new PendingLimitError(
				`Too many pending commands on memcache server ${this.id} (maxPendingCommands: ${limit})`,
			);
			return this._overloadError;
		}

		return undefined;
	}

	/**
	 * Start the command deadline after queueing a command. `idle` says nothing
	 * was pending before it, so the wait starts now. Only response bytes
	 * restart the wait, never writes, so a server that stops responding is
	 * detected even while more commands are being sent.
	 */
	private startDeadline(idle: boolean): void {
		if (idle) {
			this._waitingSince = performance.now();
		}

		if (!this._deadline) {
			this.scheduleDeadline(this._timeout);
		}
	}

	private scheduleDeadline(delay: number): void {
		this._deadline = setTimeout(() => this.checkDeadline(), Math.max(delay, 0));
		// Don't keep the process alive just to time out a command
		this._deadline.unref();
	}

	/**
	 * One timer per node checks the deadline instead of a timer per command.
	 * It isn't moved on every response: when it fires before the deadline, it
	 * is scheduled again for the time left.
	 */
	private checkDeadline(): void {
		this._deadline = undefined;
		if (!this.hasPendingCommands()) {
			return;
		}

		const waited = performance.now() - this._waitingSince;
		if (waited < this._timeout) {
			this.scheduleDeadline(this._timeout - waited);
			return;
		}

		// No response in time: fail everything pending and drop the connection,
		// so a late response can't be taken for a later command's
		const socket = this._socket as Socket;
		this.emit("timeout");
		this.dropSocket(
			socket,
			new Error(this._connecting ? "Connection timeout" : "Command timeout"),
		);
		socket.destroy();
	}

	/**
	 * Mark the node disconnected and fail everything waiting on `socket`.
	 * Does nothing if the node has already moved on to another socket.
	 */
	private dropSocket(socket: Socket, error: Error): void {
		if (socket !== this._socket) {
			return;
		}

		this._connected = false;
		this._authenticated = false;
		this.rejectPendingCommands(error);
	}

	/**
	 * Disconnect from the memcache server
	 */
	public async disconnect(): Promise<void> {
		// Abandon a connect() in progress so the next one opens a new socket
		this._connecting = undefined;
		/* v8 ignore next -- @preserve */
		if (this._socket) {
			const socket = this._socket;
			this.dropSocket(socket, new Error("Connection closed"));
			this._socket = undefined;
			socket.destroy();
		}
	}

	/**
	 * Reconnect to the memcache server by disconnecting and connecting again
	 */
	public async reconnect(): Promise<void> {
		// First disconnect if currently connected
		if (this._connected || this._socket) {
			// Fail pending commands with a reconnection error before
			// disconnect() fails them as closed
			this.rejectPendingCommands(
				new Error("Connection reset for reconnection"),
			);
			await this.disconnect();
		}

		// Now establish a fresh connection
		await this.connect();
	}

	/**
	 * Build `tls.connect()` options. User-supplied ConnectionOptions (CA, cert,
	 * SNI, …) are passed through, but the node's host/port/path and keep-alive
	 * settings always win so a `tls: { host, port }` object cannot retarget
	 * the socket.
	 */
	private buildTlsConnectOptions(tls: MemcacheTlsOption): TlsConnectionOptions {
		const options: TlsConnectionOptions = tls === true ? {} : { ...tls };
		// Node identity always wins over any host/port/path in user options.
		options.host = undefined;
		options.port = undefined;
		options.path = undefined;
		if (this._port === 0) {
			options.path = this._host;
		} else {
			options.host = this._host;
			options.port = this._port;
		}
		options.keepAlive = this._keepAlive;
		options.keepAliveInitialDelay = this._keepAliveDelay;
		return options;
	}

	/**
	 * Perform SASL PLAIN authentication using the binary protocol
	 */
	private async performSaslAuth(): Promise<void> {
		/* v8 ignore next 3 -- @preserve */
		if (!this._sasl) {
			throw new Error("SASL credentials not configured");
		}

		// Goes through the binary queue like any other request, so a request
		// sent while authentication is in flight can't take its response.
		const response = await this.binaryRequest(
			buildSaslPlainRequest(this._sasl.username, this._sasl.password),
		);
		const status = readStatus(response);

		if (status === STATUS_SUCCESS) {
			this._authenticated = true;
			this.emit("authenticated");
			return;
		}

		if (status === STATUS_AUTH_ERROR) {
			const body = response.subarray(HEADER_SIZE);
			throw new Error(
				`SASL authentication failed: ${body.toString() || "Invalid credentials"}`,
			);
		}

		throw new Error(
			`SASL authentication failed with status: 0x${status.toString(16)}`,
		);
	}

	/**
	 * Send a binary protocol request and wait for its response packet.
	 * Used internally for SASL-authenticated connections.
	 */
	private binaryRequest(packet: Buffer): Promise<Buffer> {
		return new Promise((resolve, reject) => {
			this.queueBinaryRequest(
				packet,
				(response) => {
					resolve(response);
					return true;
				},
				reject,
			);
		});
	}

	/**
	 * Tag a binary request with the next `opaque` value, add it to the queue
	 * and write it to the socket.
	 */
	private queueBinaryRequest(
		packet: Buffer,
		onPacket: BinaryQueueItem["onPacket"],
		reject: BinaryQueueItem["reject"],
	): void {
		if (!this._connected || !this._socket) {
			reject(new Error(`Not connected to memcache server ${this.id}`));
			return;
		}

		// A quit still goes out under overload, as in command()
		const overload = this.overloadError();
		if (overload && packet[1] !== OPCODE_QUIT) {
			reject(overload);
			return;
		}

		const idle = !this.hasPendingCommands();
		this._binaryOpaque = (this._binaryOpaque + 1) >>> 0;
		packet.writeUInt32BE(this._binaryOpaque, 12);
		this._binaryQueue.push({ opaque: this._binaryOpaque, onPacket, reject });
		this.startDeadline(idle);
		this.writeToSocket(this._socket, packet, idle);
	}

	/**
	 * Write to the socket. On an idle node the write goes out at once, so a
	 * lone request isn't delayed. Writes made while other requests are
	 * pending are sent together at the end of the tick: the first corks the
	 * socket and the next tick uncorks it. Without this, each pipelined
	 * command is its own syscall and TCP segment (Nagle's algorithm is off).
	 */
	private writeToSocket(
		socket: Socket,
		data: string | Buffer,
		idle: boolean,
	): void {
		if (idle) {
			socket.write(data);
			return;
		}

		if (socket.writableCorked === 0) {
			socket.cork();
			process.nextTick(() => socket.uncork());
		}

		socket.write(data);
	}

	/**
	 * Write a request made of several parts as one write, the way
	 * `writeToSocket` writes one part: at once on an idle node, otherwise
	 * with the other writes of the tick.
	 */
	private writeParts(socket: Socket, parts: string[], idle: boolean): void {
		if (idle) {
			socket.cork();
			for (const part of parts) {
				socket.write(part);
			}
			socket.uncork();
			return;
		}

		for (const part of parts) {
			this.writeToSocket(socket, part, false);
		}
	}

	/**
	 * Split incoming binary data into response packets. Chunks are only
	 * joined once a whole packet has arrived, so a large value is copied once
	 * instead of on every chunk.
	 */
	private handleBinaryData(data: Buffer): void {
		this._binaryChunks.push(data);
		this._binaryLength += data.length;

		while (this._binaryLength >= HEADER_SIZE) {
			// The header itself is split across chunks.
			if (this._binaryChunks[0].length < HEADER_SIZE) {
				this._binaryChunks = [
					Buffer.concat(this._binaryChunks, this._binaryLength),
				];
			}

			const first = this._binaryChunks[0];
			if (first[0] !== RESPONSE_MAGIC) {
				this.failBinaryRequests(
					new Error(
						`Invalid binary response from ${this.id}: expected magic byte 0x81, received 0x${first[0].toString(16)}`,
					),
				);
				return;
			}

			const packetLength = HEADER_SIZE + first.readUInt32BE(8);
			if (this._binaryLength < packetLength) {
				return;
			}

			const buffer =
				this._binaryChunks.length === 1
					? first
					: Buffer.concat(this._binaryChunks, this._binaryLength);
			const rest = buffer.subarray(packetLength);
			this._binaryChunks = rest.length > 0 ? [rest] : [];
			this._binaryLength = rest.length;
			this.handleBinaryPacket(buffer.subarray(0, packetLength));
		}
	}

	private handleBinaryPacket(packet: Buffer): void {
		const request = this._binaryQueue.peek();
		// No request is waiting for this packet.
		if (!request) {
			return;
		}

		const opaque = packet.readUInt32BE(12);
		if (opaque !== request.opaque) {
			this.failBinaryRequests(
				new Error(
					`Binary response out of order from ${this.id}: expected opaque ${request.opaque}, received ${opaque}`,
				),
			);
			return;
		}

		if (request.onPacket(packet)) {
			this._binaryQueue.shift();
		}
	}

	/**
	 * The responses no longer line up with the pending requests. Fail them
	 * all and close the connection rather than give a caller another
	 * request's response.
	 */
	private failBinaryRequests(error: Error): void {
		this.rejectBinaryRequests(error);
		this._socket?.destroy();
	}

	private rejectBinaryRequests(error: Error): void {
		const pending = this._binaryQueue.drain();
		this._binaryChunks = [];
		this._binaryLength = 0;
		for (const request of pending) {
			request.reject(error);
		}
	}

	/**
	 * Binary protocol GET operation
	 */
	public async binaryGet(key: string): Promise<string | undefined> {
		const response = await this.binaryRequest(buildGetRequest(key));
		const { status, value } = parseGetResponse(response);

		if (status === STATUS_KEY_NOT_FOUND) {
			this.emit("miss", key);
			return undefined;
		}

		/* v8 ignore next 3 -- @preserve */
		if (status !== STATUS_SUCCESS || value === undefined) {
			return undefined;
		}

		this.emit("hit", key, value);
		return value;
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
		return readStatus(response) === STATUS_SUCCESS;
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
		return readStatus(response) === STATUS_SUCCESS;
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
		return readStatus(response) === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol DELETE operation
	 */
	public async binaryDelete(key: string): Promise<boolean> {
		const response = await this.binaryRequest(buildDeleteRequest(key));
		const status = readStatus(response);
		return status === STATUS_SUCCESS || status === STATUS_KEY_NOT_FOUND;
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
		const { status, value } = parseIncrDecrResponse(response);

		/* v8 ignore next 3 -- @preserve */
		if (status !== STATUS_SUCCESS) {
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
		const { status, value } = parseIncrDecrResponse(response);

		/* v8 ignore next 3 -- @preserve */
		if (status !== STATUS_SUCCESS) {
			return undefined;
		}

		return value;
	}

	/**
	 * Binary protocol APPEND operation
	 */
	public async binaryAppend(key: string, value: string): Promise<boolean> {
		const response = await this.binaryRequest(buildAppendRequest(key, value));
		return readStatus(response) === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol PREPEND operation
	 */
	public async binaryPrepend(key: string, value: string): Promise<boolean> {
		const response = await this.binaryRequest(buildPrependRequest(key, value));
		return readStatus(response) === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol TOUCH operation
	 */
	public async binaryTouch(key: string, exptime: number): Promise<boolean> {
		const response = await this.binaryRequest(buildTouchRequest(key, exptime));
		return readStatus(response) === STATUS_SUCCESS;
	}

	/**
	 * Binary protocol FLUSH operation
	 */
	/* v8 ignore next -- @preserve */
	public async binaryFlush(exptime = 0): Promise<boolean> {
		const response = await this.binaryRequest(buildFlushRequest(exptime));
		return readStatus(response) === STATUS_SUCCESS;
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
		const stats: Record<string, string> = {};

		return new Promise((resolve, reject) => {
			this.queueBinaryRequest(
				buildStatRequest(),
				(packet) => {
					const header = deserializeHeader(packet);
					// An empty packet ends the list. An error is a single packet,
					// so it ends the request too.
					if (
						header.status !== STATUS_SUCCESS ||
						(header.keyLength === 0 && header.totalBodyLength === 0)
					) {
						resolve(stats);
						return true;
					}

					if (header.opcode === OPCODE_STAT) {
						const keyEnd = HEADER_SIZE + header.keyLength;
						const key = packet.subarray(HEADER_SIZE, keyEnd).toString("utf8");
						stats[key] = packet.subarray(keyEnd).toString("utf8");
					}

					return false;
				},
				reject,
			);
		});
	}

	/**
	 * Binary protocol QUIT operation
	 */
	public async binaryQuit(): Promise<void> {
		// memcached replies to QUIT before it closes the connection. Queue the
		// request so that reply can't be taken for a later request's response.
		this.queueBinaryRequest(
			buildQuitRequest(),
			() => true,
			() => undefined,
		);
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
	 * Send a generic command to the memcache server. Not an async function:
	 * returning the promise directly saves the extra promise and microtask
	 * turns an async wrapper adds to every command. Everything happens in the
	 * executor, so any failure still rejects instead of throwing.
	 * @param cmd The command string to send (without trailing \r\n)
	 * @param options Command options for response parsing
	 */
	public command(
		cmd: string,
		options?: CommandOptions,
		// biome-ignore lint/suspicious/noExplicitAny: expected
	): Promise<any> {
		return new Promise((resolve, reject) => {
			const socket = this._socket;
			if (!this._connected || !socket) {
				reject(new Error(`Not connected to memcache server ${this.id}`));
				return;
			}

			// Under overload, fail now rather than queue without bound. A quit
			// still goes out: refusing it would make quit() close the
			// connection before the requests ahead of it get their replies.
			const overload = this.overloadError();
			if (overload && cmd !== "quit") {
				reject(overload);
				return;
			}

			const wire = `${cmd}\r\n`;
			const data = options?.data;
			const idle = !this.hasPendingCommands();
			this._commandQueue.push({
				command: cmd,
				resolve,
				reject,
				isMultiline: options?.isMultiline,
				isStats: options?.isStats,
				isConfig: options?.isConfig,
				requestedKeys: options?.requestedKeys,
			});
			this.startDeadline(idle);
			if (data === undefined) {
				this.writeToSocket(socket, wire, idle);
			} else {
				this.writeParts(socket, [wire, data, "\r\n"], idle);
			}
		});
	}

	private handleData(data: Buffer | string): void {
		const chunk = typeof data === "string" ? Buffer.from(data, "utf8") : data;
		if (this._pendingValueBytes > 0) {
			// A value body is arriving: collect its chunks and join them once,
			// when the value and its CRLF are all here. Joining on every chunk
			// copied a large value over and over.
			this._valueChunks.push(chunk);
			this._valueLength += chunk.length;
			const buffered = this._buffer.length + this._valueLength;
			if (buffered < this._pendingValueBytes + 2) {
				return;
			}

			this._buffer = Buffer.concat(
				[this._buffer, ...this._valueChunks],
				buffered,
			);
			this._valueChunks = [];
			this._valueLength = 0;
		} else {
			this._buffer =
				this._buffer.length === 0
					? chunk
					: Buffer.concat([this._buffer, chunk]);
		}

		// Read from an offset into the buffer, and keep what is left once at
		// the end, instead of slicing off every line and value
		const buffer = this._buffer;
		let offset = 0;
		try {
			while (true) {
				// If we're waiting for value data, try to read it first
				if (this._pendingValueBytes > 0) {
					const valueEnd = offset + this._pendingValueBytes;
					if (buffer.length < valueEnd + 2) {
						// Not enough data yet, wait for more
						break;
					}

					this._multilineData.push(buffer.toString("utf8", offset, valueEnd));
					offset = valueEnd + 2;
					this._pendingValueBytes = 0;
				}

				const lineEnd = findLineEnd(buffer, offset);
				if (lineEnd === -1) break;

				const line = readLine(buffer, offset, lineEnd);
				offset = lineEnd + 2;
				this.processLine(line);

				// A hit or miss listener closed the connection, which dropped
				// everything buffered
				if (this._buffer !== buffer) {
					return;
				}
			}
		} finally {
			// Also when a hit or miss listener throws: the lines before it are
			// done, and must not be read again with the next chunk
			if (this._buffer === buffer) {
				if (offset === buffer.length) {
					this._buffer = EMPTY_BUFFER;
				} else if (offset > 0) {
					this._buffer = buffer.subarray(offset);
				}
			}
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
					const sp1 = statLine.indexOf(" ");
					const sp2 = statLine.indexOf(" ", sp1 + 1);
					/* v8 ignore next -- @preserve */
					if (sp1 !== -1 && sp2 !== -1) {
						stats[statLine.substring(sp1 + 1, sp2)] = statLine.substring(
							sp2 + 1,
						);
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

		if (this._currentCommand.isConfig) {
			if (line.startsWith("CONFIG ")) {
				// CONFIG <component> <flags> <bytes>
				const sp1 = line.indexOf(" ");
				const sp2 = line.indexOf(" ", sp1 + 1);
				const sp3 = line.indexOf(" ", sp2 + 1);
				this._pendingValueBytes = Number.parseInt(line.substring(sp3 + 1), 10);
			} else if (line === "END") {
				const result =
					this._multilineData.length > 0 ? this._multilineData : undefined;
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
				// VALUE <key> <flags> <bytes> [casunique]
				const sp1 = line.indexOf(" ");
				const sp2 = line.indexOf(" ", sp1 + 1);
				const sp3 = line.indexOf(" ", sp2 + 1);
				const sp4 = line.indexOf(" ", sp3 + 1);
				const key = line.substring(sp1 + 1, sp2);
				const bytes = parseInt(
					sp4 === -1 ? line.substring(sp3 + 1) : line.substring(sp3 + 1, sp4),
					10,
				);
				if (this._currentCommand.requestedKeys) {
					this._currentCommand.foundKeys?.push(key);
				}
				if (bytes === 0) {
					// Empty value: push "" so foundKeys and _multilineData stay
					// aligned. The trailing \r\n is consumed as an empty line.
					this._multilineData.push("");
				} else {
					this._pendingValueBytes = bytes;
				}
			} else if (line === "END") {
				// The response is complete: settle the command and clear the
				// state before the hit and miss listeners run. A listener that
				// closes the connection then can't fail this command, and the
				// command can't be read after the close has cleared it.
				const command = this._currentCommand;
				const values = this._multilineData;
				this._multilineData = [];
				this._currentCommand = undefined;

				// If requestedKeys is present, resolve with keys and values
				const { requestedKeys, foundKeys } = command;
				if (requestedKeys && foundKeys) {
					command.resolve({
						values: values.length > 0 ? values : undefined,
						foundKeys,
					});
				} else {
					command.resolve(values.length > 0 ? values : undefined);
				}

				// Emit hit/miss events if we have requested keys
				/* v8 ignore next -- @preserve */
				if (requestedKeys && foundKeys) {
					for (let i = 0; i < foundKeys.length; i++) {
						this.emit("hit", foundKeys[i], values[i]);
					}

					// Emit miss events for keys that weren't found. A Set keeps this
					// linear: this runs inside the data handler, and scanning
					// foundKeys for every requested key blocked the event loop for
					// seconds on large multi-gets.
					const found = new Set(foundKeys);
					for (const key of requestedKeys) {
						if (!found.has(key)) {
							this.emit("miss", key);
						}
					}
				}
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
		for (const cmd of this._commandQueue.drain()) {
			cmd.reject(error);
		}
		// Any partly received response belonged to a rejected command
		this._buffer = Buffer.alloc(0);
		this._multilineData = [];
		this._pendingValueBytes = 0;
		this._valueChunks = [];
		this._valueLength = 0;
		this.rejectBinaryRequests(error);
		// Nothing is waiting for a response any more
		clearTimeout(this._deadline);
		this._deadline = undefined;
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
