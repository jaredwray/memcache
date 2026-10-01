// biome-ignore-all lint/suspicious/noExplicitAny: test file
import { type AddressInfo, createServer, type Socket } from "node:net";
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	type MockInstance,
	vi,
} from "vitest";
import {
	OPCODE_DELETE,
	OPCODE_GET,
	OPCODE_INCREMENT,
	OPCODE_NOOP,
	OPCODE_QUIT,
	OPCODE_STAT,
	RESPONSE_MAGIC,
	STATUS_AUTH_ERROR,
	STATUS_INVALID_ARGUMENTS,
	STATUS_SUCCESS,
	serializeHeader,
} from "../src/binary-protocol.js";
import { createNode, MemcacheNode } from "../src/node";
import {
	generateKey,
	generateLargeValue,
	generateValue,
} from "./test-utils.js";

// Dedicated server for tests that flush everything (see docker-compose.yml).
const FLUSH_HOST = "localhost";
const FLUSH_PORT = 11214;

// memcached copies each binary request's opaque value into its responses.
// Returns the opaque of the nth packet written through the spy.
const writtenOpaque = (write: MockInstance, n = 0): number =>
	(write.mock.calls[n][0] as Buffer).readUInt32BE(12);

const binaryResponse = (
	opcode: number,
	opaque: number,
	options: {
		status?: number;
		extras?: Buffer;
		key?: string;
		value?: string | Buffer;
	} = {},
): Buffer => {
	const extras = options.extras ?? Buffer.alloc(0);
	const key = Buffer.from(options.key ?? "", "utf8");
	const value = Buffer.from(options.value ?? "");
	const header = serializeHeader({
		magic: RESPONSE_MAGIC,
		opcode,
		keyLength: key.length,
		extrasLength: extras.length,
		status: options.status ?? STATUS_SUCCESS,
		totalBodyLength: extras.length + key.length + value.length,
		opaque,
	});
	return Buffer.concat([header, extras, key, value]);
};

// A GET hit: 4 bytes of flags, then the value.
const getResponse = (opaque: number, value: string): Buffer =>
	binaryResponse(OPCODE_GET, opaque, { extras: Buffer.alloc(4), value });

const sleep = (ms: number) =>
	new Promise((resolve) => {
		setTimeout(resolve, ms);
	});

// An in-process server; `onConnection` decides how (or whether) to reply.
const startServer = async (onConnection: (socket: Socket) => void) => {
	const sockets = new Set<Socket>();
	const server = createServer((socket) => {
		sockets.add(socket);
		socket.on("close", () => sockets.delete(socket));
		onConnection(socket);
	});
	await new Promise<void>((resolve) => {
		server.listen(0, "127.0.0.1", resolve);
	});
	return {
		port: (server.address() as AddressInfo).port,
		close: () => {
			for (const socket of sockets) {
				socket.destroy();
			}
			server.close();
		},
	};
};

// Accepts connections and reads requests, but never replies.
const startStalledServer = () =>
	startServer((socket) => {
		socket.on("data", () => undefined);
	});

describe("MemcacheNode", () => {
	let node: MemcacheNode;

	beforeEach(() => {
		node = new MemcacheNode("localhost", 11211, {
			timeout: 5000,
		});
	});

	afterEach(async () => {
		if (node.isConnected()) {
			await node.disconnect();
		}
	});

	describe("Constructor and Properties", () => {
		it("should create instance with host and port", () => {
			expect(node).toBeInstanceOf(MemcacheNode);
			expect(node.host).toBe("localhost");
			expect(node.port).toBe(11211);
		});

		it("should generate correct id for standard port", () => {
			const testNode = new MemcacheNode("localhost", 11211);
			expect(testNode.id).toBe("localhost:11211");
		});

		it("should generate correct id for Unix socket (port 0)", () => {
			const testNode = new MemcacheNode("/var/run/memcached.sock", 0);
			expect(testNode.id).toBe("/var/run/memcached.sock");
		});

		it("should generate correct uri for standard port", () => {
			const testNode = new MemcacheNode("localhost", 11211);
			expect(testNode.uri).toBe("memcache://localhost:11211");
		});

		it("should generate correct uri for Unix socket (port 0)", () => {
			const testNode = new MemcacheNode("/var/run/memcached.sock", 0);
			expect(testNode.uri).toBe("memcache:///var/run/memcached.sock");
		});

		it("should generate memcaches:// uri when TLS is enabled", () => {
			expect(new MemcacheNode("localhost", 11211, { tls: true }).uri).toBe(
				"memcaches://localhost:11211",
			);
			expect(new MemcacheNode("localhost", 11211, { tls: {} }).uri).toBe(
				"memcaches://localhost:11211",
			);
		});

		it("should generate memcache:// uri when TLS is explicitly disabled", () => {
			expect(new MemcacheNode("localhost", 11211, { tls: false }).uri).toBe(
				"memcache://localhost:11211",
			);
		});

		it("should generate memcaches:// uri for TLS IPv6 and Unix sockets", () => {
			expect(new MemcacheNode("::1", 21211, { tls: true }).uri).toBe(
				"memcaches://[::1]:21211",
			);
			expect(
				new MemcacheNode("/var/run/memcached.sock", 0, { tls: true }).uri,
			).toBe("memcaches:///var/run/memcached.sock");
		});

		it("should use default options if not provided", () => {
			const testNode = new MemcacheNode("localhost", 11211);
			expect(testNode).toBeInstanceOf(MemcacheNode);
		});

		it("should have default weight of 1", () => {
			const testNode = new MemcacheNode("localhost", 11211);
			expect(testNode.weight).toBe(1);
		});

		it("should accept weight in options", () => {
			const testNode = new MemcacheNode("localhost", 11211, { weight: 5 });
			expect(testNode.weight).toBe(5);
		});

		it("should allow getting and setting weight", () => {
			const testNode = new MemcacheNode("localhost", 11211, { weight: 2 });
			expect(testNode.weight).toBe(2);

			testNode.weight = 10;
			expect(testNode.weight).toBe(10);
		});
	});

	describe("createNode factory function", () => {
		it("should create a new MemcacheNode instance", () => {
			const node = createNode("localhost", 11211);
			expect(node).toBeInstanceOf(MemcacheNode);
			expect(node.host).toBe("localhost");
			expect(node.port).toBe(11211);
		});

		it("should create node with options", () => {
			const node = createNode("localhost", 11211, {
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
				weight: 3,
			});
			expect(node).toBeInstanceOf(MemcacheNode);
			expect(node.weight).toBe(3);
		});

		it("should create node without options", () => {
			const node = createNode("192.168.1.1", 11212);
			expect(node).toBeInstanceOf(MemcacheNode);
			expect(node.host).toBe("192.168.1.1");
			expect(node.port).toBe(11212);
			expect(node.weight).toBe(1); // default weight
		});

		it("should report tlsEnabled from TLS options", () => {
			expect(createNode("localhost", 11211).tlsEnabled).toBe(false);
			expect(createNode("localhost", 11211, { tls: false }).tlsEnabled).toBe(
				false,
			);
			expect(createNode("localhost", 11211, { tls: true }).tlsEnabled).toBe(
				true,
			);
			expect(createNode("localhost", 11211, { tls: {} }).tlsEnabled).toBe(true);
			const withCa = createNode("localhost", 11211, { tls: { ca: "x" } });
			expect(withCa.tlsEnabled).toBe(true);
			expect(withCa.tls).toEqual({ ca: "x" });
		});
	});

	describe("Constructor and Properties", () => {
		it("should have default keepAlive of true", () => {
			const testNode = new MemcacheNode("localhost", 11211);
			expect(testNode.keepAlive).toBe(true);
		});

		it("should accept keepAlive in options", () => {
			const testNode = new MemcacheNode("localhost", 11211, {
				keepAlive: false,
			});
			expect(testNode.keepAlive).toBe(false);
		});

		it("should allow getting and setting keepAlive", () => {
			const testNode = new MemcacheNode("localhost", 11211, {
				keepAlive: true,
			});
			expect(testNode.keepAlive).toBe(true);

			testNode.keepAlive = false;
			expect(testNode.keepAlive).toBe(false);

			testNode.keepAlive = true;
			expect(testNode.keepAlive).toBe(true);
		});

		it("should have default keepAliveDelay of 1000", () => {
			const testNode = new MemcacheNode("localhost", 11211);
			expect(testNode.keepAliveDelay).toBe(1000);
		});

		it("should accept keepAliveDelay in options", () => {
			const testNode = new MemcacheNode("localhost", 11211, {
				keepAliveDelay: 5000,
			});
			expect(testNode.keepAliveDelay).toBe(5000);
		});

		it("should allow getting and setting keepAliveDelay", () => {
			const testNode = new MemcacheNode("localhost", 11211, {
				keepAliveDelay: 2000,
			});
			expect(testNode.keepAliveDelay).toBe(2000);

			testNode.keepAliveDelay = 3000;
			expect(testNode.keepAliveDelay).toBe(3000);

			testNode.keepAliveDelay = 500;
			expect(testNode.keepAliveDelay).toBe(500);
		});
	});

	describe("Connection Lifecycle", () => {
		it("should connect to memcached server", async () => {
			await node.connect();
			expect(node.isConnected()).toBe(true);
		});

		it("should handle connecting when already connected", async () => {
			await node.connect();
			expect(node.isConnected()).toBe(true);

			// Try to connect again - should resolve immediately
			await node.connect();
			expect(node.isConnected()).toBe(true);
		});

		it("should disconnect from memcached server", async () => {
			await node.connect();
			expect(node.isConnected()).toBe(true);

			await node.disconnect();
			expect(node.isConnected()).toBe(false);
		});

		it("should emit connect event", async () => {
			let connected = false;
			node.on("connect", () => {
				connected = true;
			});

			await node.connect();
			expect(connected).toBe(true);
		});

		it("should emit close event on disconnect", async () => {
			await node.connect();

			const closePromise = new Promise<void>((resolve) => {
				node.on("close", () => {
					resolve();
				});
			});

			await node.disconnect();

			// Wait for close event with timeout
			await Promise.race([
				closePromise,
				new Promise((_, reject) =>
					setTimeout(() => reject(new Error("Close event not emitted")), 100),
				),
			]);
		});

		it("should handle quit command", async () => {
			await node.connect();
			await node.quit();
			expect(node.isConnected()).toBe(false);
		});

		it("should reconnect successfully", async () => {
			// First connection
			await node.connect();
			expect(node.isConnected()).toBe(true);

			// Set a value to ensure connection is working
			const key = generateKey("reconnect");
			const value = generateValue();
			const bytes = Buffer.byteLength(value);
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Reconnect
			await node.reconnect();
			expect(node.isConnected()).toBe(true);

			// Verify we can still execute commands after reconnect
			const result = await node.command("version");
			expect(result).toBeDefined();
			expect(typeof result).toBe("string");
			expect(result).toContain("VERSION");
		});

		it("should clear pending commands on reconnect", async () => {
			await node.connect();

			// Queue a command that won't complete
			const key = generateKey("slow");
			const promise = node.command(`get ${key}`, { isMultiline: true });

			// Reconnect immediately (this will disconnect and clear pending commands)
			setImmediate(async () => {
				await node.reconnect();
			});

			// The pending command should be rejected
			await expect(promise).rejects.toThrow(
				"Connection reset for reconnection",
			);
		});

		it("should reconnect when not initially connected", async () => {
			// Don't connect first
			expect(node.isConnected()).toBe(false);

			// Reconnect should establish a connection
			await node.reconnect();
			expect(node.isConnected()).toBe(true);

			// Verify connection works
			const result = await node.command("version");
			expect(result).toContain("VERSION");
		});

		it("should emit connect event on reconnect", async () => {
			await node.connect();

			let connectCount = 0;
			node.on("connect", () => {
				connectCount++;
			});

			// Reconnect
			await node.reconnect();

			// Should emit connect event for the new connection
			expect(connectCount).toBe(1);
			expect(node.isConnected()).toBe(true);
		});

		it("should reject connection to invalid host", async () => {
			const badNode = new MemcacheNode("0.0.0.0", 99999, { timeout: 1000 });
			await expect(badNode.connect()).rejects.toThrow();
		});

		it("should handle connection timeout", async () => {
			// Use a valid IP that won't respond (TEST-NET-1)
			const timeoutNode = new MemcacheNode("192.0.2.0", 11211, {
				timeout: 1000,
			});
			await expect(timeoutNode.connect()).rejects.toThrow("Connection timeout");
		});
	});

	describe("Timeouts", () => {
		it("should keep an idle connection open longer than the timeout", async () => {
			const idleNode = new MemcacheNode("localhost", 11211, { timeout: 200 });
			let timeouts = 0;
			idleNode.on("timeout", () => {
				timeouts++;
			});

			await idleNode.connect();
			await sleep(400);

			expect(idleNode.isConnected()).toBe(true);
			expect(timeouts).toBe(0);
			expect(await idleNode.command("version")).toMatch(/^VERSION /);
			await idleNode.disconnect();
		});

		it("should time out a command when the server stops responding, even while more commands are written", async () => {
			const server = await startStalledServer();
			const stalledNode = new MemcacheNode("127.0.0.1", server.port, {
				timeout: 300,
			});
			let timeouts = 0;
			stalledNode.on("timeout", () => {
				timeouts++;
			});

			try {
				await stalledNode.connect();
				const started = performance.now();
				const first = stalledNode.command("version");
				// Writes don't count as progress
				const writer = setInterval(() => {
					stalledNode.command("version").catch(() => undefined);
				}, 50);

				await expect(first).rejects.toThrow("Command timeout");
				clearInterval(writer);

				const elapsed = performance.now() - started;
				expect(elapsed).toBeGreaterThanOrEqual(300);
				expect(elapsed).toBeLessThan(1500);
				expect(timeouts).toBe(1);
				expect(stalledNode.isConnected()).toBe(false);
			} finally {
				await stalledNode.disconnect();
				server.close();
			}
		});

		it("should not time out a response that keeps arriving", async () => {
			const value = "x".repeat(2000);
			const server = await startServer((socket) => {
				socket.once("data", async () => {
					// About 1 second in total, 50 ms between chunks
					const response = Buffer.from(
						`VALUE slow 0 ${value.length}\r\n${value}\r\nEND\r\n`,
					);
					for (let i = 0; i < response.length; i += 100) {
						socket.write(response.subarray(i, i + 100));
						await sleep(50);
					}
				});
			});
			const slowNode = new MemcacheNode("127.0.0.1", server.port, {
				timeout: 300,
			});

			try {
				await slowNode.connect();
				const result = await slowNode.command("get slow", {
					isMultiline: true,
					requestedKeys: ["slow"],
				});
				expect(result).toEqual({ values: [value], foundKeys: ["slow"] });
			} finally {
				await slowNode.disconnect();
				server.close();
			}
		});

		it("should apply a new timeout to a command that is already waiting", async () => {
			const server = await startStalledServer();
			const stalledNode = new MemcacheNode("127.0.0.1", server.port, {
				timeout: 5000,
			});

			try {
				await stalledNode.connect();
				const started = performance.now();
				const pending = stalledNode.command("version");
				stalledNode.timeout = 200;

				await expect(pending).rejects.toThrow("Command timeout");
				expect(performance.now() - started).toBeLessThan(1500);
			} finally {
				await stalledNode.disconnect();
				server.close();
			}
		});

		it("should time out a binary request that gets no response", async () => {
			const server = await startStalledServer();
			const stalledNode = new MemcacheNode("127.0.0.1", server.port, {
				timeout: 200,
			});

			try {
				await stalledNode.connect();
				await expect(stalledNode.binaryGet("key")).rejects.toThrow(
					"Command timeout",
				);
			} finally {
				await stalledNode.disconnect();
				server.close();
			}
		});

		it("should report a connection timeout when SASL authentication gets no response", async () => {
			const server = await startStalledServer();
			const saslNode = new MemcacheNode("127.0.0.1", server.port, {
				timeout: 200,
				sasl: { username: "user", password: "pass" },
			});
			let timeouts = 0;
			saslNode.on("timeout", () => {
				timeouts++;
			});

			try {
				await expect(saslNode.connect()).rejects.toThrow("Connection timeout");
				expect(timeouts).toBe(1);
				expect(saslNode.isConnected()).toBe(false);
			} finally {
				server.close();
			}
		});

		it("should report a connection timeout when the TLS handshake gets no response", async () => {
			const server = await startStalledServer();
			const tlsNode = new MemcacheNode("127.0.0.1", server.port, {
				timeout: 200,
				tls: { rejectUnauthorized: false },
			});

			try {
				await expect(tlsNode.connect()).rejects.toThrow("Connection timeout");
			} finally {
				server.close();
			}
		});

		it("should stop the deadline timer once nothing is pending", async () => {
			const quickNode = new MemcacheNode("localhost", 11211, { timeout: 100 });
			await quickNode.connect();
			expect(await quickNode.command("version")).toMatch(/^VERSION /);

			// The timer fires once, finds nothing pending and isn't scheduled again
			await vi.waitFor(() => {
				expect((quickNode as any)._deadline).toBeUndefined();
			});
			expect(quickNode.isConnected()).toBe(true);
			await quickNode.disconnect();
		});

		it("should get and set the timeout", () => {
			expect(node.timeout).toBe(5000);
			node.timeout = 1234;
			expect(node.timeout).toBe(1234);
		});
	});

	describe("Connection sharing and replaced sockets", () => {
		it("should share one socket between concurrent connect() calls", async () => {
			let connectEvents = 0;
			node.on("connect", () => {
				connectEvents++;
			});

			const first = node.connect();
			const socket = node.socket;
			const second = node.connect();
			expect(node.socket).toBe(socket);

			await Promise.all([first, second]);
			expect(node.socket).toBe(socket);
			expect(connectEvents).toBe(1);
		});

		it("should reject connect() when disconnect() is called before the socket is ready", async () => {
			const first = node.connect();
			await node.disconnect();
			// A new attempt doesn't wait on the abandoned one
			const second = node.connect();

			await expect(first).rejects.toThrow("Connection closed");
			await second;
			expect(node.isConnected()).toBe(true);
		});

		it("should ignore data, timeout and close from a socket that has been replaced", async () => {
			await node.connect();
			const oldSocket = node.socket as Socket;
			await node.reconnect();
			const newSocket = node.socket as Socket;

			const version = node.command("version");
			// Late events from the replaced socket
			oldSocket.emit("data", Buffer.from("VERSION stale\r\n"));
			oldSocket.emit("timeout");
			oldSocket.emit("close");

			expect(await version).toMatch(/^VERSION \d/);
			expect(node.isConnected()).toBe(true);
			expect(node.socket).toBe(newSocket);
			expect(newSocket.destroyed).toBe(false);
		});

		it("should parse the next connection cleanly after closing mid-response", async () => {
			const key = generateKey("partial");
			const value = generateValue();
			await node.connect();
			await node.command(
				`set ${key} 0 0 ${Buffer.byteLength(value)}\r\n${value}`,
			);

			const socket = node.socket as Socket;
			const writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);
			const partial = node.command(`get ${key}`, {
				isMultiline: true,
				requestedKeys: [key],
			});
			// The server starts a value, then the connection drops
			socket.emit("data", Buffer.from(`VALUE ${key} 0 10\r\nabc`));
			socket.destroy();
			await expect(partial).rejects.toThrow("Connection closed");
			writeSpy.mockRestore();

			await node.connect();
			const result = await node.command(`get ${key}`, {
				isMultiline: true,
				requestedKeys: [key],
			});
			expect(result).toEqual({ values: [value], foundKeys: [key] });
		});
	});

	describe("Generic Command Execution", () => {
		beforeEach(async () => {
			await node.connect();
		});

		it("should execute version command", async () => {
			const result = await node.command("version");
			expect(result).toBeDefined();
			expect(typeof result).toBe("string");
			expect(result).toContain("VERSION");
		});

		it("should execute set command", async () => {
			const key = generateKey("set");
			const value = generateValue();
			const bytes = Buffer.byteLength(value);
			const cmd = `set ${key} 0 0 ${bytes}\r\n${value}`;
			const result = await node.command(cmd);
			expect(result).toBe("STORED");
		});

		it("should execute get command with multiline option", async () => {
			const key = generateKey("get");
			const value = generateValue();

			// First set the value
			const bytes = Buffer.byteLength(value);
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Then get it
			const result = await node.command(`get ${key}`, { isMultiline: true });
			expect(result).toBeInstanceOf(Array);
			expect(result[0]).toBe(value);
		});

		it("should execute get command for non-existent key", async () => {
			const key = generateKey("nonexistent");
			const result = await node.command(`get ${key}`, { isMultiline: true });
			expect(result).toBeUndefined();
		});

		it("should execute delete command", async () => {
			const key = generateKey("delete");
			const value = generateValue();
			const bytes = Buffer.byteLength(value);

			// Set first
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Delete
			const result = await node.command(`delete ${key}`);
			expect(result).toBe("DELETED");
		});

		it("should execute incr command", async () => {
			const key = generateKey("incr");

			// Set initial value
			await node.command(`set ${key} 0 0 1\r\n0`);

			// Increment
			const result = await node.command(`incr ${key} 1`);
			expect(result).toBe(1);
		});

		it("should execute decr command", async () => {
			const key = generateKey("decr");

			// Set initial value
			await node.command(`set ${key} 0 0 2\r\n10`);

			// Decrement
			const result = await node.command(`decr ${key} 1`);
			expect(result).toBe(9);
		});

		it("should execute stats command", async () => {
			const result = await node.command("stats", { isStats: true });
			expect(result).toBeDefined();
			expect(typeof result).toBe("object");
			expect(result.version).toBeDefined();
		});

		it("should execute touch command", async () => {
			const key = generateKey("touch");
			const value = generateValue();
			const bytes = Buffer.byteLength(value);

			// Set first
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Touch
			const result = await node.command(`touch ${key} 100`);
			expect(result).toBe("TOUCHED");
		});

		it("should execute flush_all command", async () => {
			// flush_all clears the whole server, so use the dedicated flush
			// server rather than the one other test files share.
			const flushNode = new MemcacheNode(FLUSH_HOST, FLUSH_PORT, {
				timeout: 5000,
			});
			await flushNode.connect();
			try {
				const result = await flushNode.command("flush_all");
				expect(result).toBe("OK");
			} finally {
				await flushNode.disconnect();
			}
		});

		it("should handle NOT_STORED response for add command", async () => {
			const key = generateKey("add-dup");
			const value = generateValue();
			const bytes = Buffer.byteLength(value);

			// First set the key
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Try to add the same key (should fail since it exists)
			const result = await node.command(`add ${key} 0 0 ${bytes}\r\n${value}`);
			expect(result).toBe(false);
		});

		it("should handle EXISTS response for cas command", async () => {
			const key = generateKey("cas");
			const value = generateValue();
			const bytes = Buffer.byteLength(value);

			// Set initial value
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Get with cas to get the cas token
			const getResult = await node.command(`gets ${key}`, {
				isMultiline: true,
			});
			expect(getResult).toBeDefined();

			// Modify the value to change cas
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Try cas with old token (should get EXISTS)
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(
				`cas ${key} 0 0 ${bytes} 12345\r\n${value}`,
			);

			// Simulate server EXISTS response
			mockSocket.emit("data", "EXISTS\r\n");

			const result = await commandPromise;
			expect(result).toBe("EXISTS");
		});

		it("should handle NOT_FOUND response for delete command", async () => {
			const key = generateKey("delete-notfound");

			// Try to delete a key that doesn't exist
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`delete ${key}`);

			// Simulate server NOT_FOUND response
			mockSocket.emit("data", "NOT_FOUND\r\n");

			const result = await commandPromise;
			expect(result).toBe("NOT_FOUND");
		});

		it("should handle multiple sequential commands", async () => {
			const key1 = generateKey("seq1");
			const key2 = generateKey("seq2");
			const value = generateValue();
			const bytes = Buffer.byteLength(value);

			const result1 = await node.command(
				`set ${key1} 0 0 ${bytes}\r\n${value}`,
			);
			expect(result1).toBe("STORED");

			const result2 = await node.command(
				`set ${key2} 0 0 ${bytes}\r\n${value}`,
			);
			expect(result2).toBe("STORED");

			const result3 = await node.command(`get ${key1}`, { isMultiline: true });
			expect(result3[0]).toBe(value);
		});

		it("should emit hit event for successful get", async () => {
			const key = generateKey("hit");
			const value = generateValue();
			const bytes = Buffer.byteLength(value);

			// Set first
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			let hitEmitted = false;
			let hitKey = "";
			let hitValue = "";

			node.on("hit", (k: string, v: string) => {
				hitEmitted = true;
				hitKey = k;
				hitValue = v;
			});

			// Get with requestedKeys to trigger event
			await node.command(`get ${key}`, {
				isMultiline: true,
				requestedKeys: [key],
			});

			expect(hitEmitted).toBe(true);
			expect(hitKey).toBe(key);
			expect(hitValue).toBe(value);
		});

		it("should emit miss event for non-existent key", async () => {
			const key = generateKey("miss");

			let missEmitted = false;
			let missKey = "";

			node.on("miss", (k: string) => {
				missEmitted = true;
				missKey = k;
			});

			// Get non-existent key with requestedKeys to trigger event
			await node.command(`get ${key}`, {
				isMultiline: true,
				requestedKeys: [key],
			});

			expect(missEmitted).toBe(true);
			expect(missKey).toBe(key);
		});

		it("should emit one hit or miss per requested key, repeats included", async () => {
			const found = generateKey("repeat-hit");
			const missing = generateKey("repeat-miss");
			await node.command(`set ${found} 0 0 1\r\nx`);

			const hits: string[] = [];
			const misses: string[] = [];
			node.on("hit", (k: string) => hits.push(k));
			node.on("miss", (k: string) => misses.push(k));

			// memcached returns a found key once per time it is requested
			const requestedKeys = [missing, found, missing, found];
			await node.command(`get ${requestedKeys.join(" ")}`, {
				isMultiline: true,
				requestedKeys,
			});

			expect(hits).toEqual([found, found]);
			expect(misses).toEqual([missing, missing]);
		});
	});

	describe("Error Handling", () => {
		it("should throw error when not connected", async () => {
			const disconnectedNode = new MemcacheNode("localhost", 11211);
			await expect(disconnectedNode.command("version")).rejects.toThrow(
				"Not connected to memcache server",
			);
		});

		it("should handle protocol errors", async () => {
			await node.connect();

			// Try to set with invalid key (contains space)
			await expect(
				node.command("set invalid key 0 0 5\r\nvalue"),
			).rejects.toThrow();
		});

		it("should reject pending commands on disconnect", async () => {
			await node.connect();

			// Queue a command
			const key = generateKey("slow");
			const promise = node.command(`get ${key}`, { isMultiline: true });

			// Immediately disconnect
			setImmediate(() => {
				node.disconnect();
			});

			await expect(promise).rejects.toThrow("Connection closed");
		});

		it("should emit error event", async () => {
			await node.connect();

			let errorEmitted = false;
			node.on("error", () => {
				errorEmitted = true;
			});

			// Force an error by destroying the socket
			node.socket?.emit("error", new Error("Test error"));

			expect(errorEmitted).toBe(true);
		});
	});

	describe("Command Queue", () => {
		beforeEach(async () => {
			await node.connect();
		});

		it("should maintain FIFO order for commands", async () => {
			const key1 = generateKey("fifo1");
			const key2 = generateKey("fifo2");
			const key3 = generateKey("fifo3");
			const value = generateValue();
			const bytes = Buffer.byteLength(value);

			// Queue multiple commands
			const promises = [
				node.command(`set ${key1} 0 0 ${bytes}\r\n${value}`),
				node.command(`set ${key2} 0 0 ${bytes}\r\n${value}`),
				node.command(`set ${key3} 0 0 ${bytes}\r\n${value}`),
			];

			const results = await Promise.all(promises);

			expect(results[0]).toBe("STORED");
			expect(results[1]).toBe("STORED");
			expect(results[2]).toBe("STORED");
		});

		it("should expose command queue", () => {
			expect(node.commandQueue).toBeDefined();
			expect(Array.isArray(node.commandQueue)).toBe(true);
		});
	});

	describe("Write coalescing", () => {
		const nextTick = () =>
			new Promise<void>((resolve) => {
				process.nextTick(resolve);
			});

		it("should write a command on an idle node at once, without corking", async () => {
			await node.connect();
			const socket = node.socket as Socket;
			const cork = vi.spyOn(socket, "cork");
			const write = vi.spyOn(socket, "write");

			expect(await node.command("version")).toMatch(/^VERSION /);
			expect(await node.command("version")).toMatch(/^VERSION /);

			expect(write).toHaveBeenCalledTimes(2);
			expect(cork).not.toHaveBeenCalled();
		});

		it("should send the commands that follow in the same tick together, in order", async () => {
			await node.connect();
			const socket = node.socket as Socket;
			const cork = vi.spyOn(socket, "cork");
			const uncork = vi.spyOn(socket, "uncork");
			const write = vi.spyOn(socket, "write");

			const keys = ["a", "b", "c"].map((name) => generateKey(`tick-${name}`));
			const results = Promise.all(
				keys.map((key) => node.command(`get ${key}`, { isMultiline: true })),
			);

			// The first command goes out at once; the other two wait for the tick to end
			expect(cork).toHaveBeenCalledTimes(1);
			expect(write.mock.invocationCallOrder[0]).toBeLessThan(
				cork.mock.invocationCallOrder[0],
			);
			expect(uncork).not.toHaveBeenCalled();
			expect(write.mock.calls.map((call) => call[0])).toEqual(
				keys.map((key) => `get ${key}\r\n`),
			);

			await nextTick();
			expect(uncork).toHaveBeenCalledTimes(1);
			expect(await results).toEqual([undefined, undefined, undefined]);
		});

		it("should cork again for commands written on a later tick", async () => {
			await node.connect();
			const socket = node.socket as Socket;
			const cork = vi.spyOn(socket, "cork");
			const uncork = vi.spyOn(socket, "uncork");

			const first = [node.command("version"), node.command("version")];
			await nextTick();
			// The first two are still waiting for their responses
			const second = [node.command("version"), node.command("version")];
			const results = await Promise.all([...first, ...second]);

			expect(results).toHaveLength(4);
			for (const result of results) {
				expect(result).toMatch(/^VERSION /);
			}

			expect(cork).toHaveBeenCalledTimes(2);
			expect(uncork).toHaveBeenCalledTimes(2);
		});

		it("should not throw when the socket is destroyed before the tick ends", async () => {
			await node.connect();
			const socket = node.socket as Socket;
			const uncork = vi.spyOn(socket, "uncork");

			const pending = [node.command("version"), node.command("version")];
			socket.destroy();

			await Promise.all(
				pending.map((command) =>
					expect(command).rejects.toThrow("Connection closed"),
				),
			);
			expect(uncork).toHaveBeenCalledTimes(1);
		});

		it("should send binary requests that follow in the same tick together", async () => {
			await node.connect();
			const socket = node.socket as Socket;
			const cork = vi.spyOn(socket, "cork");
			const writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);

			const results = Promise.all([
				node.binaryGet("key-a"),
				node.binaryGet("key-b"),
			]);
			expect(cork).toHaveBeenCalledTimes(1);
			expect(writeSpy).toHaveBeenCalledTimes(2);

			socket.emit(
				"data",
				Buffer.concat([
					getResponse(writtenOpaque(writeSpy, 0), "value-a"),
					getResponse(writtenOpaque(writeSpy, 1), "value-b"),
				]),
			);
			expect(await results).toEqual(["value-a", "value-b"]);
			writeSpy.mockRestore();
		});
	});

	describe("Multiline Response Handling", () => {
		beforeEach(async () => {
			await node.connect();
		});

		it("should handle multiple keys in get command", async () => {
			const key1 = generateKey("multi1");
			const key2 = generateKey("multi2");
			const value1 = generateValue();
			const value2 = generateValue();

			// Set both keys
			await node.command(
				`set ${key1} 0 0 ${Buffer.byteLength(value1)}\r\n${value1}`,
			);
			await node.command(
				`set ${key2} 0 0 ${Buffer.byteLength(value2)}\r\n${value2}`,
			);

			// Get both
			const result = await node.command(`get ${key1} ${key2}`, {
				isMultiline: true,
			});

			expect(result).toBeInstanceOf(Array);
			expect(result.length).toBe(2);
			expect(result[0]).toBe(value1);
			expect(result[1]).toBe(value2);
		});

		it("should handle large values", async () => {
			const key = generateKey("large");
			const value = generateLargeValue(10000); // 10KB value
			const bytes = Buffer.byteLength(value);

			const setResult = await node.command(
				`set ${key} 0 0 ${bytes}\r\n${value}`,
			);
			expect(setResult).toBe("STORED");

			const result = await node.command(`get ${key}`, { isMultiline: true });
			expect(result).toBeDefined();
			expect(result[0]).toBe(value);
			expect(result[0].length).toBe(10000);
		});

		it("should handle partial data delivery for value bytes", async () => {
			const key = generateKey("partial");
			// Use a value guaranteed to be longer than 5 characters for proper partial delivery testing
			const value = generateLargeValue(20);
			const bytes = Buffer.byteLength(value);

			// Set the value first
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`get ${key}`, {
				isMultiline: true,
			});

			// Simulate partial data delivery - send VALUE line first
			mockSocket.emit("data", `VALUE ${key} 0 ${bytes}\r\n`);

			// Send only part of the value bytes (not enough)
			mockSocket.emit("data", value.substring(0, 5));

			// Send rest of value and END
			mockSocket.emit("data", `${value.substring(5)}\r\nEND\r\n`);

			const result = await commandPromise;
			expect(result).toBeDefined();
			expect(result[0]).toBe(value);
		});
	});

	describe("Large value buffering", () => {
		let socket: Socket;
		let writeSpy: MockInstance;

		beforeEach(async () => {
			await node.connect();
			socket = node.socket as Socket;
			// Requests never reach the server; each test supplies the response
			writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);
		});

		afterEach(() => {
			writeSpy.mockRestore();
		});

		const get = (key: string) =>
			node.command(`get ${key}`, { isMultiline: true, requestedKeys: [key] });

		const deliver = (target: Socket, data: string, size: number) => {
			const bytes = Buffer.from(data);
			for (let i = 0; i < bytes.length; i += size) {
				target.emit("data", bytes.subarray(i, i + size));
			}
		};

		it.each([
			["16 KB", 16 * 1024],
			["64 KB", 64 * 1024],
		])("should copy a 1 MB value in %s chunks once", async (_, size) => {
			const value = generateLargeValue(1024 * 1024);
			const result = get("large");

			socket.emit("data", Buffer.from(`VALUE large 0 ${value.length}\r\n`));
			const concat = vi.spyOn(Buffer, "concat");
			deliver(socket, value, size);
			socket.emit("data", Buffer.from("\r\nEND\r\n"));
			expect(concat).toHaveBeenCalledTimes(1);
			concat.mockRestore();

			expect(await result).toEqual({ values: [value], foundKeys: ["large"] });
		});

		it("should read a value delivered one byte at a time", async () => {
			// CRLF inside a value must not end it early
			const value = `line one\r\nline two\r\n${generateLargeValue(1000)}`;
			const result = get("bytes");

			deliver(
				socket,
				`VALUE bytes 0 ${Buffer.byteLength(value)}\r\n${value}\r\nEND\r\n`,
				1,
			);

			expect(await result).toEqual({ values: [value], foundKeys: ["bytes"] });
		});

		it("should wait for a value's CRLF split across chunks", async () => {
			const value = generateLargeValue(100);
			const result = get("split");

			socket.emit("data", Buffer.from("VALUE split 0 100\r\n"));
			socket.emit("data", Buffer.from(value));
			socket.emit("data", Buffer.from("\r"));
			socket.emit("data", Buffer.from("\nEND\r\n"));

			expect(await result).toEqual({ values: [value], foundKeys: ["split"] });
		});

		it("should read several values and END from one chunk", async () => {
			const keys = ["first", "second", "empty"];
			const values = ["one", "two\r\nlines", ""];
			const result = node.command(`get ${keys.join(" ")}`, {
				isMultiline: true,
				requestedKeys: keys,
			});

			const response = keys
				.map(
					(key, i) =>
						`VALUE ${key} 0 ${Buffer.byteLength(values[i])}\r\n${values[i]}\r\n`,
				)
				.join("");
			socket.emit("data", Buffer.from(`${response}END\r\n`));

			expect(await result).toEqual({ values, foundKeys: keys });
		});

		it("should drop a partly received value when the connection closes", async () => {
			const partial = get("dropped");
			socket.emit("data", Buffer.from("VALUE dropped 0 10\r\nabc"));
			socket.emit("data", Buffer.from("def"));
			socket.destroy();
			await expect(partial).rejects.toThrow("Connection closed");

			await node.connect();
			const next = node.socket as Socket;
			const nextWrite = vi.spyOn(next, "write").mockImplementation(() => true);
			const result = get("fresh");
			// Arriving in pieces, the next value would pick up any leftover chunks
			next.emit("data", Buffer.from("VALUE fresh 0 5\r\n"));
			next.emit("data", Buffer.from("hello"));
			next.emit("data", Buffer.from("\r\nEND\r\n"));

			expect(await result).toEqual({ values: ["hello"], foundKeys: ["fresh"] });
			nextWrite.mockRestore();
		});
	});

	describe("Error Handling", () => {
		it("should handle ERROR response for stats command", async () => {
			await node.connect();

			const mockSocket = (node as any)._socket;
			const commandPromise = node.command("stats invalid_type", {
				isStats: true,
			});

			// Simulate server ERROR response
			mockSocket.emit("data", "ERROR\r\n");

			await expect(commandPromise).rejects.toThrow("ERROR");
		});

		it("should handle CLIENT_ERROR response for stats command", async () => {
			await node.connect();

			const mockSocket = (node as any)._socket;
			const commandPromise = node.command("stats", { isStats: true });

			// Simulate server CLIENT_ERROR response
			mockSocket.emit("data", "CLIENT_ERROR bad command\r\n");

			await expect(commandPromise).rejects.toThrow("CLIENT_ERROR bad command");
		});

		it("should handle SERVER_ERROR response for stats command", async () => {
			await node.connect();

			const mockSocket = (node as any)._socket;
			const commandPromise = node.command("stats", { isStats: true });

			// Simulate server SERVER_ERROR response
			mockSocket.emit("data", "SERVER_ERROR out of memory\r\n");

			await expect(commandPromise).rejects.toThrow(
				"SERVER_ERROR out of memory",
			);
		});

		it("should handle unexpected line in stats command response", async () => {
			await node.connect();

			const mockSocket = (node as any)._socket;
			const commandPromise = node.command("stats", { isStats: true });

			// Simulate unexpected response line (not STAT, not END, not ERROR)
			mockSocket.emit("data", "UNEXPECTED_LINE\r\n");
			// Then send END to complete the command
			mockSocket.emit("data", "END\r\n");

			// Should still resolve successfully, ignoring the unexpected line
			const result = await commandPromise;
			expect(result).toBeDefined();
		});

		it("should handle ERROR response for multiline get command", async () => {
			await node.connect();

			const key = generateKey("error");
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`get ${key}`, {
				isMultiline: true,
				requestedKeys: [key],
			});

			// Simulate server ERROR response
			mockSocket.emit("data", "ERROR\r\n");

			await expect(commandPromise).rejects.toThrow("ERROR");
		});

		it("should handle CLIENT_ERROR response for multiline get command", async () => {
			await node.connect();

			const key = generateKey("clienterror");
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`get ${key}`, {
				isMultiline: true,
				requestedKeys: [key],
			});

			// Simulate server CLIENT_ERROR response
			mockSocket.emit("data", "CLIENT_ERROR invalid key\r\n");

			await expect(commandPromise).rejects.toThrow("CLIENT_ERROR invalid key");
		});

		it("should handle SERVER_ERROR response for multiline get command", async () => {
			await node.connect();

			const key = generateKey("servererror");
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`get ${key}`, {
				isMultiline: true,
				requestedKeys: [key],
			});

			// Simulate server SERVER_ERROR response
			mockSocket.emit("data", "SERVER_ERROR temporary failure\r\n");

			await expect(commandPromise).rejects.toThrow(
				"SERVER_ERROR temporary failure",
			);
		});

		it("should reject current command on disconnect", async () => {
			await node.connect();

			// Start a command but don't let it complete
			const key = generateKey("pending");
			const commandPromise = node.command(`get ${key}`, {
				isMultiline: true,
			});

			// Disconnect immediately
			await node.disconnect();

			// The command should be rejected
			await expect(commandPromise).rejects.toThrow();
		});

		it("should reject queued commands on disconnect", async () => {
			await node.connect();

			// Queue multiple commands without responses
			const key1 = generateKey("queue1");
			const key2 = generateKey("queue2");
			const key3 = generateKey("queue3");
			const promise1 = node.command(`get ${key1}`, { isMultiline: true });
			const promise2 = node.command(`get ${key2}`, { isMultiline: true });
			const promise3 = node.command(`get ${key3}`, { isMultiline: true });

			// Disconnect immediately
			await node.disconnect();

			// All commands should be rejected
			await expect(promise1).rejects.toThrow();
			await expect(promise2).rejects.toThrow();
			await expect(promise3).rejects.toThrow();
		});

		it("should reject connection when socket error occurs before connected", async () => {
			// Use an invalid port to trigger connection error before _connected is set
			const badNode = new MemcacheNode("localhost", 1, { timeout: 500 });

			// The connection should be rejected due to socket error
			await expect(badNode.connect()).rejects.toThrow();
		}, 10000);

		it("should handle error response for multiline command without requestedKeys", async () => {
			await node.connect();

			const key = generateKey("nokeys");
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`get ${key}`, {
				isMultiline: true,
				// Note: no requestedKeys provided
			});

			// Simulate server ERROR response for multiline command
			mockSocket.emit("data", "ERROR\r\n");

			await expect(commandPromise).rejects.toThrow("ERROR");
		});

		it("should reject current command in rejectPendingCommands when current command exists", async () => {
			await node.connect();

			// Start a command that will become _currentCommand
			const key = generateKey("current");
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`get ${key}`, {
				isMultiline: true,
			});

			// Send partial response to set _currentCommand
			mockSocket.emit("data", `VALUE ${key} 0 5\r\n`);

			// Trigger close event which calls rejectPendingCommands
			mockSocket.emit("close");

			await expect(commandPromise).rejects.toThrow("Connection closed");
		});

		it("should handle SERVER_ERROR after partial VALUE response in multiline command", async () => {
			await node.connect();

			const key1 = generateKey("partial1");
			const key2 = generateKey("partial2");
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`get ${key1} ${key2}`, {
				isMultiline: true,
			});

			// First send a VALUE line and its data
			mockSocket.emit("data", `VALUE ${key1} 0 5\r\n`);
			mockSocket.emit("data", "test1\r\n");

			// Then send a SERVER_ERROR (simulating server issue mid-response)
			mockSocket.emit("data", "SERVER_ERROR out of memory\r\n");

			await expect(commandPromise).rejects.toThrow(
				"SERVER_ERROR out of memory",
			);
		});
	});

	describe("binaryStats multi-chunk handling", () => {
		const buildStatPacket = (
			opaque: number,
			key: string,
			value: string,
		): Buffer => binaryResponse(OPCODE_STAT, opaque, { key, value });

		// memcached ends the list with an empty STAT packet.
		const buildTerminator = (opaque: number): Buffer =>
			binaryResponse(OPCODE_STAT, opaque);

		it("should collect stats that span multiple data events", async () => {
			await node.connect();

			const socket = (node as any)._socket;
			// Suppress the real stats request — we will fake the response
			const writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);

			const statsPromise = node.binaryStats();
			const opaque = writtenOpaque(writeSpy);

			// First chunk: a complete stat record
			socket.emit("data", buildStatPacket(opaque, "pid", "12345"));
			// Second chunk: another stat + terminator
			socket.emit(
				"data",
				Buffer.concat([
					buildStatPacket(opaque, "uptime", "42"),
					buildTerminator(opaque),
				]),
			);

			const stats = await statsPromise;
			expect(stats.pid).toBe("12345");
			expect(stats.uptime).toBe("42");

			writeSpy.mockRestore();
		});

		it("should read a header split across data events", async () => {
			await node.connect();

			const socket = (node as any)._socket;
			const writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);

			const statsPromise = node.binaryStats();
			const opaque = writtenOpaque(writeSpy);

			const fullPacket = Buffer.concat([
				buildStatPacket(opaque, "version", "1.6.0"),
				buildTerminator(opaque),
			]);

			// Split inside the 24-byte header
			socket.emit("data", fullPacket.subarray(0, 10));
			socket.emit("data", fullPacket.subarray(10));

			const stats = await statsPromise;
			expect(stats.version).toBe("1.6.0");

			writeSpy.mockRestore();
		});

		it("should ignore packets with non-OPCODE_STAT opcode", async () => {
			await node.connect();

			const socket = (node as any)._socket;
			const writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);

			const statsPromise = node.binaryStats();
			const opaque = writtenOpaque(writeSpy);

			// A NOOP-opcode packet (not OPCODE_STAT) should be skipped without
			// failing — exercises the opcode guard's false branch.
			socket.emit(
				"data",
				Buffer.concat([
					binaryResponse(OPCODE_NOOP, opaque, {
						key: "ignored",
						value: "data",
					}),
					buildStatPacket(opaque, "pid", "999"),
					buildTerminator(opaque),
				]),
			);

			const stats = await statsPromise;
			expect(stats.ignored).toBeUndefined();
			expect(stats.pid).toBe("999");

			writeSpy.mockRestore();
		});

		it("should finish on an error response so the next request gets its own response", async () => {
			await node.connect();

			const socket = (node as any)._socket;
			const writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);

			const statsPromise = node.binaryStats();
			const getPromise = node.binaryGet("after-stats");

			// memcached answers an error with a single packet and no terminator
			socket.emit(
				"data",
				Buffer.concat([
					binaryResponse(OPCODE_STAT, writtenOpaque(writeSpy, 0), {
						status: STATUS_AUTH_ERROR,
						value: "Auth failure.",
					}),
					getResponse(writtenOpaque(writeSpy, 1), "value-after-stats"),
				]),
			);

			expect(await statsPromise).toEqual({});
			expect(await getPromise).toBe("value-after-stats");

			writeSpy.mockRestore();
		});
	});

	describe("binaryRequest multi-chunk handling", () => {
		it("should assemble a response that spans multiple data events", async () => {
			await node.connect();

			const socket = (node as any)._socket;
			const writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);

			const getPromise = node.binaryGet("multi-chunk-key");
			const fullPacket = getResponse(writtenOpaque(writeSpy), "hello-world");

			// Header in the first chunk, body in the second
			socket.emit("data", fullPacket.subarray(0, 26));
			socket.emit("data", fullPacket.subarray(26));

			const result = await getPromise;
			expect(result).toBe("hello-world");

			writeSpy.mockRestore();
		});

		it("should copy a large value once instead of on every chunk", async () => {
			await node.connect();

			const socket = (node as any)._socket;
			const writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);

			const value = generateLargeValue(256 * 1024);
			const getPromise = node.binaryGet("large-key");
			const fullPacket = getResponse(writtenOpaque(writeSpy), value);

			const concatSpy = vi.spyOn(Buffer, "concat");
			for (let i = 0; i < fullPacket.length; i += 1024) {
				socket.emit("data", fullPacket.subarray(i, i + 1024));
			}
			expect(concatSpy).toHaveBeenCalledTimes(1);
			concatSpy.mockRestore();

			expect(await getPromise).toBe(value);

			writeSpy.mockRestore();
		});

		it("should return false from binaryDelete on unexpected status", async () => {
			await node.connect();

			const socket = (node as any)._socket;
			const writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);

			const deletePromise = node.binaryDelete("some-key");

			socket.emit(
				"data",
				binaryResponse(OPCODE_DELETE, writtenOpaque(writeSpy), {
					status: STATUS_INVALID_ARGUMENTS,
				}),
			);

			const result = await deletePromise;
			expect(result).toBe(false);

			writeSpy.mockRestore();
		});
	});

	describe("Binary request queue", () => {
		let socket: any;
		let writeSpy: MockInstance;

		beforeEach(async () => {
			await node.connect();
			socket = (node as any)._socket;
			// Requests never reach the server; each test supplies the responses
			writeSpy = vi.spyOn(socket, "write").mockImplementation(() => true);
		});

		afterEach(() => {
			writeSpy.mockRestore();
		});

		it("should give pipelined requests their own responses from one chunk", async () => {
			const results = Promise.all([
				node.binaryGet("key-a"),
				node.binaryGet("key-b"),
				node.binaryIncr("counter"),
			]);

			const opaques = [0, 1, 2].map((n) => writtenOpaque(writeSpy, n));
			expect(new Set(opaques).size).toBe(3);

			const counter = Buffer.alloc(8);
			counter.writeUInt32BE(7, 4);
			socket.emit(
				"data",
				Buffer.concat([
					getResponse(opaques[0], "value-a"),
					getResponse(opaques[1], "value-b"),
					binaryResponse(OPCODE_INCREMENT, opaques[2], { value: counter }),
				]),
			);

			expect(await results).toEqual(["value-a", "value-b", 7]);
		});

		it("should split responses when a header and the next packet share chunks", async () => {
			const first = node.binaryGet("key-a");
			const second = node.binaryGet("key-b");

			const packets = Buffer.concat([
				getResponse(writtenOpaque(writeSpy, 0), "value-a"),
				getResponse(writtenOpaque(writeSpy, 1), "value-b"),
			]);
			socket.emit("data", packets.subarray(0, 10));
			socket.emit("data", packets.subarray(10));

			expect(await first).toBe("value-a");
			expect(await second).toBe("value-b");
		});

		it("should reject pending requests and close the connection when a response is out of order", async () => {
			const first = node.binaryGet("key-a");
			const second = node.binaryGet("key-b");

			// The second request's response arrives first
			socket.emit("data", getResponse(writtenOpaque(writeSpy, 1), "value-b"));

			await expect(first).rejects.toThrow("Binary response out of order");
			await expect(second).rejects.toThrow("Binary response out of order");
			expect(socket.destroyed).toBe(true);
		});

		it("should reject pending requests when a response is not a binary packet", async () => {
			const pending = node.binaryGet("key-a");

			socket.emit("data", Buffer.from("SERVER_ERROR out of memory\r\n"));

			await expect(pending).rejects.toThrow("expected magic byte 0x81");
			expect(socket.destroyed).toBe(true);
		});

		it("should reject pending requests when the connection closes", async () => {
			const get = node.binaryGet("key-a");
			const stats = node.binaryStats();

			// Part of a response is buffered when the connection closes
			socket.emit(
				"data",
				getResponse(writtenOpaque(writeSpy, 0), "value-a").subarray(0, 10),
			);
			socket.destroy();

			await expect(get).rejects.toThrow("Connection closed");
			await expect(stats).rejects.toThrow("Connection closed");
			expect((node as any)._binaryChunks).toEqual([]);
			expect((node as any)._binaryLength).toBe(0);
		});

		it("should ignore a response that no request is waiting for", async () => {
			const first = node.binaryGet("key-a");
			socket.emit(
				"data",
				Buffer.concat([
					getResponse(writtenOpaque(writeSpy, 0), "value-a"),
					getResponse(12345, "unexpected"),
				]),
			);
			expect(await first).toBe("value-a");

			const second = node.binaryGet("key-b");
			socket.emit("data", getResponse(writtenOpaque(writeSpy, 1), "value-b"));
			expect(await second).toBe("value-b");
			expect(socket.destroyed).toBe(false);
		});

		it("should parse text again once no binary requests are waiting", async () => {
			const get = node.binaryGet("key-a");
			socket.emit("data", getResponse(writtenOpaque(writeSpy, 0), "value-a"));
			expect(await get).toBe("value-a");

			const version = node.command("version");
			socket.emit("data", "VERSION 1.6.45\r\n");
			expect(await version).toBe("VERSION 1.6.45");
		});

		it("should match the reply to binaryQuit so it is not taken for the next response", async () => {
			await node.binaryQuit();
			const get = node.binaryGet("key-a");

			socket.emit(
				"data",
				Buffer.concat([
					binaryResponse(OPCODE_QUIT, writtenOpaque(writeSpy, 0)),
					getResponse(writtenOpaque(writeSpy, 1), "value-a"),
				]),
			);

			expect(await get).toBe("value-a");
		});

		it("should wrap the opaque value after 2^32 - 1", async () => {
			(node as any)._binaryOpaque = 0xffffffff;
			const get = node.binaryGet("key-a");
			expect(writtenOpaque(writeSpy)).toBe(0);

			socket.emit("data", getResponse(0, "value-a"));
			expect(await get).toBe("value-a");
		});

		it("should reject binary requests after the connection closes", async () => {
			const closed = new Promise((resolve) => socket.once("close", resolve));
			socket.destroy();
			await closed;

			await expect(node.binaryGet("key-a")).rejects.toThrow(
				"Not connected to memcache server localhost:11211",
			);
			expect(writeSpy).not.toHaveBeenCalled();
		});

		it("should reject binary requests on a node that is not connected", async () => {
			const disconnectedNode = new MemcacheNode("localhost", 11211);
			await expect(disconnectedNode.binaryGet("key")).rejects.toThrow(
				"Not connected to memcache server localhost:11211",
			);
			await expect(disconnectedNode.binaryStats()).rejects.toThrow(
				"Not connected to memcache server localhost:11211",
			);
		});
	});

	describe("binaryQuit edge cases", () => {
		it("should resolve without error when binaryQuit is called on disconnected node", async () => {
			const disconnectedNode = new MemcacheNode("localhost", 11211, {
				timeout: 5000,
			});
			// Never connect — _socket is undefined
			await expect(disconnectedNode.binaryQuit()).resolves.toBeUndefined();
		});
	});

	describe("Unexpected line handling", () => {
		it("should ignore unexpected line during config command", async () => {
			await node.connect();

			const mockSocket = (node as any)._socket;
			const commandPromise = node.command("config get cluster", {
				isConfig: true,
			});

			// Unexpected line that isn't CONFIG/END/ERROR should be ignored
			mockSocket.emit("data", "UNEXPECTED_GARBAGE\r\n");
			// Then a CONFIG line + bytes + END to complete
			mockSocket.emit("data", "CONFIG cluster 0 5\r\nhello\r\nEND\r\n");

			const result = await commandPromise;
			expect(result).toBeDefined();
			expect(Array.isArray(result)).toBe(true);
		});

		it("should ignore unexpected line during multiline get command", async () => {
			await node.connect();

			const key = generateKey("unexpected");
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`get ${key}`, {
				isMultiline: true,
				requestedKeys: [key],
			});

			// Send a line that matches none of VALUE/END/ERROR branches
			mockSocket.emit("data", "UNEXPECTED_LINE\r\n");
			// Then complete with END
			mockSocket.emit("data", "END\r\n");

			const result = await commandPromise;
			// No values were returned — result should reflect the miss
			expect(result).toBeDefined();
		});
	});
});
