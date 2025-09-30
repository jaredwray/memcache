// biome-ignore-all lint/suspicious/noExplicitAny: test file
import type { Socket } from "node:net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Memcache, { MemcacheEvents } from "../src/index";

describe("Memcache", () => {
	let client: Memcache;

	beforeEach(() => {
		client = new Memcache({
			timeout: 5000,
		});
	});

	afterEach(() => {
		if (client.isConnected()) {
			client.disconnect();
		}
	});

	describe("Constructor", () => {
		it("should create instance with default options", () => {
			const defaultClient = new Memcache();
			expect(defaultClient).toBeInstanceOf(Memcache);
		});

		it("should create instance with custom options", () => {
			const customClient = new Memcache({
				timeout: 10000,
				keepAlive: false,
			});
			expect(customClient).toBeInstanceOf(Memcache);
		});

		it("should initialize with default node localhost:11211", () => {
			const testClient = new Memcache();
			expect(testClient.ring).toBeDefined();
			expect(testClient.nodes).toHaveLength(1);
			expect(testClient.nodes).toContain("localhost:11211");
		});

		it("should initialize with nodes from options", () => {
			const testClient = new Memcache({
				nodes: ["localhost:11211", "localhost:11212", "127.0.0.1:11213"],
			});
			expect(testClient.nodes).toHaveLength(3);
			expect(testClient.nodes).toContain("localhost:11211");
			expect(testClient.nodes).toContain("localhost:11212");
			expect(testClient.nodes).toContain("127.0.0.1:11213");
		});

		it("should parse node URIs with protocols", () => {
			const testClient = new Memcache({
				nodes: [
					"memcache://localhost:11211",
					"tcp://192.168.1.100:11212",
					"server3",
				],
			});
			expect(testClient.nodes).toHaveLength(3);
			expect(testClient.nodes).toContain("localhost:11211");
			expect(testClient.nodes).toContain("192.168.1.100:11212");
			expect(testClient.nodes).toContain("server3:11211");
		});

		it("should handle Unix socket URIs in nodes", () => {
			const testClient = new Memcache({
				nodes: ["unix:///var/run/memcached.sock", "/tmp/memcached.sock"],
			});
			expect(testClient.nodes).toHaveLength(2);
			expect(testClient.nodes).toContain("/var/run/memcached.sock");
			expect(testClient.nodes).toContain("/tmp/memcached.sock");
		});

		it("should handle IPv6 addresses in nodes", () => {
			const testClient = new Memcache({
				nodes: ["[::1]:11211", "memcache://[2001:db8::1]:11212"],
			});
			expect(testClient.nodes).toHaveLength(2);
			expect(testClient.nodes).toContain("::1:11211");
			expect(testClient.nodes).toContain("2001:db8::1:11212");
		});

		it("should allow setting timeout via setter", () => {
			const testClient = new Memcache();
			expect(testClient.timeout).toBe(5000); // Default timeout

			testClient.timeout = 10000;
			expect(testClient.timeout).toBe(10000);

			testClient.timeout = 2000;
			expect(testClient.timeout).toBe(2000);
		});

		it("should allow getting and setting keepAlive property", () => {
			const testClient = new Memcache();
			expect(testClient.keepAlive).toBe(true); // Default keepAlive

			testClient.keepAlive = false;
			expect(testClient.keepAlive).toBe(false);

			testClient.keepAlive = true;
			expect(testClient.keepAlive).toBe(true);
		});

		it("should initialize with custom keepAlive", () => {
			const testClient = new Memcache({ keepAlive: false });
			expect(testClient.keepAlive).toBe(false);

			const testClient2 = new Memcache({ keepAlive: true });
			expect(testClient2.keepAlive).toBe(true);
		});

		it("should allow getting and setting keepAliveDelay property", () => {
			const testClient = new Memcache();
			expect(testClient.keepAliveDelay).toBe(1000); // Default keepAliveDelay

			testClient.keepAliveDelay = 3000;
			expect(testClient.keepAliveDelay).toBe(3000);

			testClient.keepAliveDelay = 100;
			expect(testClient.keepAliveDelay).toBe(100);
		});

		it("should initialize with custom keepAliveDelay", () => {
			const testClient = new Memcache({ keepAliveDelay: 2000 });
			expect(testClient.keepAliveDelay).toBe(2000);
		});

		it("should allow setting socket property", () => {
			const testClient = new Memcache();
			expect(testClient.socket).toBe(undefined); // Default socket

			const mockSocket = { fake: "socket" } as any;
			testClient.socket = mockSocket;
			expect(testClient.socket).toBe(mockSocket);

			testClient.socket = undefined;
			expect(testClient.socket).toBe(undefined);
		});

		it("should allow setting commandQueue property", () => {
			const testClient = new Memcache();
			expect(testClient.commandQueue).toEqual([]); // Default empty queue

			const mockQueue = [
				{
					command: "get test",
					resolve: vi.fn(),
					reject: vi.fn(),
					isMultiline: true,
				},
			];
			testClient.commandQueue = mockQueue;
			expect(testClient.commandQueue).toEqual(mockQueue);

			const anotherQueue = [
				{
					command: "set key value",
					resolve: vi.fn(),
					reject: vi.fn(),
					isMultiline: false,
					isStats: true,
				},
			];
			testClient.commandQueue = anotherQueue;
			expect(testClient.commandQueue).toEqual(anotherQueue);
		});
	});

	describe("parseUri", () => {
		it("should parse host and port from simple format", () => {
			const result = client.parseUri("localhost:11211");
			expect(result).toEqual({ host: "localhost", port: 11211 });
		});

		it("should parse host and port from memcache:// format", () => {
			const result = client.parseUri("memcache://localhost:11211");
			expect(result).toEqual({ host: "localhost", port: 11211 });
		});

		it("should parse host and port from memcached:// format", () => {
			const result = client.parseUri("memcached://localhost:11211");
			expect(result).toEqual({ host: "localhost", port: 11211 });
		});

		it("should parse host and port from tcp:// format", () => {
			const result = client.parseUri("tcp://localhost:11211");
			expect(result).toEqual({ host: "localhost", port: 11211 });
		});

		it("should parse host with default port", () => {
			const result = client.parseUri("localhost");
			expect(result).toEqual({ host: "localhost", port: 11211 });
		});

		it("should parse host with default port from memcache:// format", () => {
			const result = client.parseUri("memcache://localhost");
			expect(result).toEqual({ host: "localhost", port: 11211 });
		});

		it("should parse host with default port from memcached:// format", () => {
			const result = client.parseUri("memcached://localhost");
			expect(result).toEqual({ host: "localhost", port: 11211 });
		});

		it("should parse host with default port from tcp:// format", () => {
			const result = client.parseUri("tcp://localhost");
			expect(result).toEqual({ host: "localhost", port: 11211 });
		});

		it("should parse IP address with port", () => {
			const result = client.parseUri("127.0.0.1:11212");
			expect(result).toEqual({ host: "127.0.0.1", port: 11212 });
		});

		it("should parse IPv6 address with brackets and port", () => {
			const result = client.parseUri("[::1]:11211");
			expect(result).toEqual({ host: "::1", port: 11211 });
		});

		it("should parse IPv6 address with brackets from memcache:// format", () => {
			const result = client.parseUri("memcache://[2001:db8::1]:11212");
			expect(result).toEqual({ host: "2001:db8::1", port: 11212 });
		});

		it("should parse IPv6 address with brackets and default port", () => {
			const result = client.parseUri("[::1]");
			expect(result).toEqual({ host: "::1", port: 11211 });
		});

		it("should parse domain with port", () => {
			const result = client.parseUri("memcache.example.com:11213");
			expect(result).toEqual({ host: "memcache.example.com", port: 11213 });
		});

		it("should parse Unix domain socket path", () => {
			const result = client.parseUri("/var/run/memcached.sock");
			expect(result).toEqual({ host: "/var/run/memcached.sock", port: 0 });
		});

		it("should parse Unix domain socket path from unix:// format", () => {
			const result = client.parseUri("unix:///var/run/memcached.sock");
			expect(result).toEqual({ host: "/var/run/memcached.sock", port: 0 });
		});

		it("should throw error for invalid protocol", () => {
			expect(() => client.parseUri("http://localhost:11211")).toThrow(
				"Invalid protocol",
			);
		});

		it("should throw error for empty host", () => {
			expect(() => client.parseUri(":11211")).toThrow(
				"Invalid URI format: host is required",
			);
		});

		it("should throw error for invalid port", () => {
			expect(() => client.parseUri("localhost:abc")).toThrow(
				"Invalid port number",
			);
		});

		it("should throw error for negative port", () => {
			expect(() => client.parseUri("localhost:-1")).toThrow(
				"Invalid port number",
			);
		});

		it("should throw error for port zero with network host", () => {
			expect(() => client.parseUri("localhost:0")).toThrow(
				"Invalid port number",
			);
		});

		it("should throw error for port too large", () => {
			expect(() => client.parseUri("localhost:65536")).toThrow(
				"Invalid port number",
			);
		});

		it("should throw error for IPv6 missing closing bracket", () => {
			expect(() => client.parseUri("[::1:11211")).toThrow(
				"Invalid IPv6 format: missing closing bracket",
			);
		});

		it("should throw error for IPv6 with empty host", () => {
			expect(() => client.parseUri("[]:11211")).toThrow(
				"Invalid URI format: host is required",
			);
		});

		it("should throw error for IPv6 with invalid format after bracket", () => {
			expect(() => client.parseUri("[::1]abc")).toThrow(
				"Invalid IPv6 format: expected ':' after bracket",
			);
		});

		it("should throw error for IPv6 with invalid port", () => {
			expect(() => client.parseUri("[::1]:99999")).toThrow(
				"Invalid port number",
			);
		});

		it("should throw error for too many colons in regular format", () => {
			expect(() => client.parseUri("host:port:extra")).toThrow(
				"Invalid URI format",
			);
		});
	});

	describe("Key Validation", () => {
		it("should throw error for empty key", async () => {
			await expect(async () => {
				await client.get("");
			}).rejects.toThrow("Key cannot be empty");
		});

		it("should throw error for key longer than 250 characters", async () => {
			const longKey = "a".repeat(251);
			await expect(async () => {
				await client.get(longKey);
			}).rejects.toThrow("Key length cannot exceed 250 characters");
		});

		it("should throw error for key with spaces", async () => {
			await expect(async () => {
				await client.get("key with spaces");
			}).rejects.toThrow(
				"Key cannot contain spaces, newlines, or null characters",
			);
		});

		it("should throw error for key with newlines", async () => {
			await expect(async () => {
				await client.get("key\nwith\nnewlines");
			}).rejects.toThrow(
				"Key cannot contain spaces, newlines, or null characters",
			);
		});
	});

	describe("Connection Management", () => {
		it("should handle connection state", () => {
			expect(client.isConnected()).toBe(false);
		});

		it("should throw error when not connected", async () => {
			await expect(async () => {
				await client.get("test");
			}).rejects.toThrow("Not connected to memcache server");
		});

		it("should handle connecting when already connected", async () => {
			const client12 = new Memcache();
			await client12.connect();
			expect(client12.isConnected()).toBe(true);

			// Try to connect again - should resolve immediately
			await client12.connect();
			expect(client12.isConnected()).toBe(true);

			client12.disconnect();
		});

		it("should handle connection errors", async () => {
			const client13 = new Memcache({
				timeout: 100,
			});

			await expect(client13.connect("0.0.0.0", 99999)).rejects.toThrow();
		});

		it("should handle connection timeout", async () => {
			const client14 = new Memcache({
				timeout: 100, // Very short timeout
			});

			await expect(client14.connect("192.0.2.0", 11211)).rejects.toThrow(
				"Connection timeout",
			);
		});

		it("should handle error event before connection is established", async () => {
			const client16 = new Memcache({
				timeout: 100,
			});

			// This should trigger an error immediately due to invalid port
			await expect(client16.connect("localhost", 99999)).rejects.toThrow();
		});

		it("should reject on socket error before connection", async () => {
			const client18 = new Memcache({
				timeout: 5000,
			});

			// Start connect and immediately simulate error
			const connectPromise = client18.connect("localhost", 11211);

			// Access the socket that was just created
			const privateClient = client18 as any;
			if (privateClient._socket) {
				// Immediately emit an error before 'connect' event
				process.nextTick(() => {
					privateClient._socket.emit("error", new Error("Early socket error"));
				});
			}

			await expect(connectPromise).rejects.toThrow("Early socket error");
		});

		it("should emit error events after connection", async () => {
			const client15 = new Memcache();
			await client15.connect();

			let errorEmitted = false;
			client15.on("error", () => {
				errorEmitted = true;
			});

			// Force an error by destroying the socket
			const socket = (client15 as any).socket as Socket;
			socket.emit("error", new Error("Test error"));

			expect(errorEmitted).toBe(true);
			client15.disconnect();
		});
	});

	describe("Command Queue", () => {
		it("should handle multiple commands when not connected", async () => {
			const promises = [
				client.get("key1").catch((e) => e.message),
				client.get("key2").catch((e) => e.message),
				client.get("key3").catch((e) => e.message),
			];

			const results = await Promise.all(promises);
			expect(results).toEqual([
				"Not connected to memcache server",
				"Not connected to memcache server",
				"Not connected to memcache server",
			]);
		});
	});

	describe("Error Handling", () => {
		it("should handle connection being closed during pending commands", async () => {
			const client2 = new Memcache();
			await client2.connect();

			// Use a non-existent key with unique name
			const uniqueKey = `test-close-${Date.now()}`;

			// Start a command but don't await it
			const pendingCommand = await client2
				.get(uniqueKey)
				.catch((e) => e.message);

			// Immediately disconnect to catch command in flight
			setImmediate(() => {
				client2.disconnect();
			});

			const result = await pendingCommand;
			// With async hook, command might complete as undefined (not found) or get "Connection closed"
			expect([undefined, "Connection closed"]).toContain(result);
		});

		it("should reject current command on connection close", async () => {
			const client17 = new Memcache();
			await client17.connect();

			// Access private members for testing
			const privateClient = client17 as any;

			// Create a mock command that will be the current command
			const mockCommand = {
				command: "get test",
				resolve: vi.fn(),
				reject: vi.fn(),
				isMultiline: true,
			};

			// Set it as current command
			privateClient.currentCommand = mockCommand;

			// Trigger rejectPendingCommands
			privateClient.rejectPendingCommands(new Error("Test error"));

			// Verify the current command was rejected
			expect(mockCommand.reject).toHaveBeenCalledWith(new Error("Test error"));
			expect(privateClient.currentCommand).toBe(undefined);

			client17.disconnect();
		});

		it("should handle multiple pending commands when connection closes", async () => {
			const client3 = new Memcache();
			await client3.connect();

			// Use unique keys that don't exist
			const timestamp = Date.now();

			// Start multiple commands without awaiting
			const commands = [
				client3.get(`key1-${timestamp}`).catch((e) => e.message),
				client3.get(`key2-${timestamp}`).catch((e) => e.message),
				client3.get(`key3-${timestamp}`).catch((e) => e.message),
			];

			// Immediately disconnect to catch commands in flight
			setImmediate(() => {
				client3.disconnect();
			});

			const results = await Promise.all(commands);
			// With async hook, commands might complete as undefined or get "Connection closed"
			results.forEach((result) => {
				expect([undefined, "Connection closed"]).toContain(result);
			});
		});

		it("should handle protocol errors in responses", async () => {
			const client4 = new Memcache();
			await client4.connect();

			// Test with an invalid key to trigger an error
			const veryLongKey = "k".repeat(251);
			await expect(client4.get(veryLongKey)).rejects.toThrow(
				"Key length cannot exceed 250 characters",
			);

			client4.disconnect();
		});

		it("should handle server errors", async () => {
			const client5 = new Memcache();
			await client5.connect();

			// Force an error by simulating a bad response
			// We'll inject an error response directly
			const socket = (client5 as any).socket as Socket;

			// Mock socket write to simulate ERROR response
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					socket.emit("data", "ERROR something went wrong\r\n");
				});
				return true;
			});

			// Send a command with unique key
			const commandPromise = client5
				.get(`test-error-${Date.now()}`)
				.catch((e) => e.message);

			const result = await commandPromise;
			expect(result).toBe("ERROR something went wrong");

			socket.write = originalWrite;
			client5.disconnect();
		});

		it("should handle CLIENT_ERROR responses", async () => {
			const client6 = new Memcache();
			await client6.connect();

			const socket = (client6 as any).socket as Socket;

			// Mock socket write to immediately emit CLIENT_ERROR
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					socket.emit("data", "CLIENT_ERROR bad command\r\n");
				});
				return true;
			});

			// Send a command
			const commandPromise = client6
				.set("test", "value")
				.catch((e) => e.message);

			const result = await commandPromise;
			expect(result).toBe("CLIENT_ERROR bad command");

			socket.write = originalWrite;
			client6.disconnect();
		});

		it("should handle SERVER_ERROR responses", async () => {
			const client7 = new Memcache();
			await client7.connect();

			const socket = (client7 as any).socket as Socket;

			// Mock socket write to simulate SERVER_ERROR
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					socket.emit("data", "SERVER_ERROR out of memory\r\n");
				});
				return true;
			});

			// Send a command with unique key
			const commandPromise = client7
				.get(`test-server-error-${Date.now()}`)
				.catch((e) => e.message);

			const result = await commandPromise;
			expect(result).toBe("SERVER_ERROR out of memory");

			socket.write = originalWrite;
			client7.disconnect();
		});

		it("should handle stats SERVER_ERROR", async () => {
			const client8 = new Memcache();
			await client8.connect();

			const socket = (client8 as any).socket as Socket;

			// Send a stats command
			const commandPromise = client8.stats().catch((e) => e.message);

			// Simulate a SERVER_ERROR response for stats
			socket.emit("data", "SERVER_ERROR stats error\r\n");

			const result = await commandPromise;
			expect(result).toBe("SERVER_ERROR stats error");

			client8.disconnect();
		});
	});

	describe("Protocol Parsing", () => {
		it("should handle partial value reads", async () => {
			const client9 = new Memcache();
			await client9.connect();

			const socket = (client9 as any).socket as Socket;
			const testKey = `test-partial-${Date.now()}`;

			// Mock socket write to simulate VALUE response
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					// Simulate a VALUE response with the value and END in one go
					socket.emit("data", `VALUE ${testKey} 0 5\r\nhello\r\nEND\r\n`);
				});
				return true;
			});

			// Send a get command with unique key
			const commandPromise = client9.get(testKey);

			const result = await commandPromise;
			expect(result).toBe("hello");

			socket.write = originalWrite;
			client9.disconnect();
		});

		it("should handle numeric responses", async () => {
			const client10 = new Memcache();
			await client10.connect();

			const socket = (client10 as any).socket as Socket;

			// Simulate numeric response for incr/decr
			const commandPromise = (client10 as any).sendCommand("incr test 1");
			socket.emit("data", "42\r\n");

			const result = await commandPromise;
			expect(result).toBe(42);

			client10.disconnect();
		});

		it("should handle generic string responses", async () => {
			const client11 = new Memcache();
			await client11.connect();

			const socket = (client11 as any).socket as Socket;

			// Simulate a generic response
			const commandPromise = (client11 as any).sendCommand("custom_command");
			socket.emit("data", "CUSTOM_RESPONSE\r\n");

			const result = await commandPromise;
			expect(result).toBe("CUSTOM_RESPONSE");

			client11.disconnect();
		});
	});

	describe("Memcached Integration Tests", () => {
		it("should connect to memcached server", async () => {
			await client.connect();
			expect(client.isConnected()).toBe(true);
		});

		it("should set and get a value", async () => {
			await client.connect();
			const key = "test-key";
			const value = "test-value";

			const setResult = await client.set(key, value);
			expect(setResult).toBe(true);

			const getValue = await client.get(key);
			expect(getValue).toBe(value);
		});

		it("should handle multiple gets", async () => {
			await client.connect();

			await client.set("key1", "value1");
			await client.set("key2", "value2");

			const results = await client.gets(["key1", "key2", "key3"]);
			expect(results.get("key1")).toBe("value1");
			expect(results.get("key2")).toBe("value2");
			expect(results.has("key3")).toBe(false);
		});

		it("should delete a key", async () => {
			await client.connect();
			const key = "delete-test";

			await client.set(key, "value");
			const deleteResult = await client.delete(key);
			expect(deleteResult).toBe(true);

			const getValue = await client.get(key);
			expect(getValue).toBe(undefined);
		});

		it("should increment and decrement values", async () => {
			await client.connect();
			const key = "counter";

			await client.set(key, "10");

			const incrResult = await client.incr(key, 5);
			expect(incrResult).toBe(15);

			const decrResult = await client.decr(key, 3);
			expect(decrResult).toBe(12);
		});

		it("should handle add command", async () => {
			await client.connect();
			const key = "add-test";

			const firstAdd = await client.add(key, "value1");
			expect(firstAdd).toBe(true);

			const secondAdd = await client.add(key, "value2");
			expect(secondAdd).toBe(false);
		});

		it("should handle replace command", async () => {
			await client.connect();
			const key = "replace-test";

			const replaceNonExistent = await client.replace(key, "value1");
			expect(replaceNonExistent).toBe(false);

			await client.set(key, "initial");
			const replaceExisting = await client.replace(key, "replaced");
			expect(replaceExisting).toBe(true);

			const getValue = await client.get(key);
			expect(getValue).toBe("replaced");
		});

		it("should handle cas command successfully", async () => {
			await client.connect();
			const key = "cas-test";

			const socket = (client as any).socket as Socket;

			// Set initial value
			await client.set(key, "initial");

			// Mock socket write to simulate STORED response for CAS
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					socket.emit("data", "STORED\r\n");
				});
				return true;
			});

			// CAS should succeed with mocked response
			const casResult = await client.cas(key, "updated", "12345");
			expect(casResult).toBe(true);

			socket.write = originalWrite;
		});

		it("should handle cas with exptime and flags", async () => {
			await client.connect();
			const key = "cas-test-params";

			const socket = (client as any).socket as Socket;

			await client.set(key, "initial");

			// Mock socket write to simulate STORED response for CAS
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					socket.emit("data", "STORED\r\n");
				});
				return true;
			});

			const casResult = await client.cas(key, "updated", "67890", 3600, 42);
			expect(casResult).toBe(true);

			socket.write = originalWrite;
		});

		it("should handle cas with default parameters", async () => {
			await client.connect();
			const key = "cas-test-defaults";

			const socket = (client as any).socket as Socket;

			await client.set(key, "initial");

			// Mock socket write to simulate STORED response for CAS
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					socket.emit("data", "STORED\r\n");
				});
				return true;
			});

			const casResult = await client.cas(key, "updated", "11111");
			expect(casResult).toBe(true);

			socket.write = originalWrite;
		});

		it("should handle append and prepend", async () => {
			await client.connect();
			const key = "concat-test";

			await client.set(key, "middle");

			await client.prepend(key, "start-");
			await client.append(key, "-end");

			const getValue = await client.get(key);
			expect(getValue).toBe("start-middle-end");
		});

		it("should handle touch command", async () => {
			await client.connect();
			const key = "touch-test";

			await client.set(key, "value", 3600);
			const touchResult = await client.touch(key, 7200);
			expect(touchResult).toBe(true);
		});

		it("should handle flush commands", async () => {
			await client.connect();

			await client.set("flush1", "value1");
			await client.set("flush2", "value2");

			const flushResult = await client.flush();
			expect(flushResult).toBe(true);

			const getValue1 = await client.get("flush1");
			const getValue2 = await client.get("flush2");
			expect(getValue1).toBe(undefined);
			expect(getValue2).toBe(undefined);
		});

		it("should handle flushAll with delay", async () => {
			await client.connect();

			// Test flushAll with delay parameter (just test the command works)
			const flushResult = await client.flush(1);
			expect(flushResult).toBe(true);

			// Also test without delay
			const flushResultNoDelay = await client.flush();
			expect(flushResultNoDelay).toBe(true);
		});

		it("should get stats", async () => {
			await client.connect();

			const stats = await client.stats();
			expect(stats).toBeDefined();
			expect(typeof stats).toBe("object");
		});

		it("should get stats with type", async () => {
			await client.connect();

			const stats = await client.stats("items");
			expect(stats).toBeDefined();
			expect(typeof stats).toBe("object");
		});

		it("should get version", async () => {
			await client.connect();

			const version = await client.version();
			expect(version).toBeDefined();
			expect(typeof version).toBe("string");
		});

		it("should handle quit command", async () => {
			await client.connect();
			expect(client.isConnected()).toBe(true);

			await client.quit();
			expect(client.isConnected()).toBe(false);
		});

		it("should handle disconnect", async () => {
			await client.connect();
			expect(client.isConnected()).toBe(true);

			client.disconnect();
			expect(client.isConnected()).toBe(false);
		});
	});

	describe("Hooks", () => {
		it("should call beforeHook and afterHook for get operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:get", beforeHookMock);
			client.onHook("after:get", afterHookMock);

			await client.connect();
			await client.set("hook-test", "hook-value");

			const result = await client.get("hook-test");

			expect(beforeHookMock).toHaveBeenCalledWith({ key: "hook-test" });
			expect(afterHookMock).toHaveBeenCalledWith({
				key: "hook-test",
				value: ["hook-value"],
			});
			expect(result).toBe("hook-value");
		});

		it("should call hooks even when key doesn't exist", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:get", beforeHookMock);
			client.onHook("after:get", afterHookMock);

			await client.connect();

			const result = await client.get("non-existent-hook-key");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "non-existent-hook-key",
			});
			expect(afterHookMock).toHaveBeenCalledWith({
				key: "non-existent-hook-key",
				value: undefined,
			});
			expect(result).toBe(undefined);
		});

		it("should support multiple hook listeners", async () => {
			const beforeHook1 = vi.fn();
			const beforeHook2 = vi.fn();
			const afterHook1 = vi.fn();
			const afterHook2 = vi.fn();

			client.onHook("before:get", beforeHook1);
			client.onHook("before:get", beforeHook2);
			client.onHook("after:get", afterHook1);
			client.onHook("after:get", afterHook2);

			await client.connect();
			await client.set("multi-hook-test", "value");
			await client.get("multi-hook-test");

			expect(beforeHook1).toHaveBeenCalled();
			expect(beforeHook2).toHaveBeenCalled();
			expect(afterHook1).toHaveBeenCalled();
			expect(afterHook2).toHaveBeenCalled();
		});

		it("should allow removing hook listeners", async () => {
			const hookMock = vi.fn();

			client.onHook("before:get", hookMock);

			await client.connect();
			await client.set("remove-hook-test", "value");

			// First call should trigger the hook
			await client.get("remove-hook-test");
			expect(hookMock).toHaveBeenCalledTimes(1);

			// Remove the hook
			client.removeHook("before:get", hookMock);

			// Second call should not trigger the hook
			await client.get("remove-hook-test");
			expect(hookMock).toHaveBeenCalledTimes(1);
		});

		it("should handle async hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:get", asyncBeforeHook);
			client.onHook("after:get", asyncAfterHook);

			await client.connect();
			await client.set("async-hook-test", "async-value");

			const start = Date.now();
			const result = await client.get("async-hook-test");
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe("async-value");
			expect(duration).toBeGreaterThanOrEqual(20); // At least 20ms for both hooks
		});

		it("should handle hook errors based on throwHookErrors setting", async () => {
			// Test with throwHookErrors = true (should throw)
			const errorClient = new Memcache();
			errorClient.throwHookErrors = true;

			const errorHook = vi.fn().mockImplementation(() => {
				throw new Error("Hook error");
			});

			errorClient.onHook("before:get", errorHook);

			await errorClient.connect();
			await errorClient.set("error-hook-test", "value");

			// Hook error should propagate and reject the promise
			await expect(errorClient.get("error-hook-test")).rejects.toThrow(
				"Hook error",
			);
			expect(errorHook).toHaveBeenCalled();

			await errorClient.disconnect();

			// Test with throwHookErrors = false (should not throw)
			const noErrorClient = new Memcache();
			noErrorClient.throwHookErrors = false;

			const errorHook2 = vi.fn().mockImplementation(() => {
				throw new Error("Hook error 2");
			});

			noErrorClient.onHook("before:get", errorHook2);

			await noErrorClient.connect();
			await noErrorClient.set("error-hook-test2", "value2");

			// Operation should succeed despite hook error
			const result = await noErrorClient.get("error-hook-test2");
			expect(result).toBe("value2");
			expect(errorHook2).toHaveBeenCalled();

			await noErrorClient.disconnect();
		});

		it("should provide hook context with correct data", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:get", beforeHookMock);
			client.onHook("after:get", afterHookMock);

			await client.connect();

			// Test with multiple keys
			await client.set("context-key1", "value1");
			await client.set("context-key2", "value2");

			await client.get("context-key1");

			expect(beforeHookMock).toHaveBeenLastCalledWith({ key: "context-key1" });
			expect(afterHookMock).toHaveBeenLastCalledWith({
				key: "context-key1",
				value: ["value1"],
			});

			await client.get("context-key2");

			expect(beforeHookMock).toHaveBeenLastCalledWith({ key: "context-key2" });
			expect(afterHookMock).toHaveBeenLastCalledWith({
				key: "context-key2",
				value: ["value2"],
			});
		});

		it("should allow using onceHook for single execution", async () => {
			const onceHookMock = vi.fn();

			client.onceHook("before:get", onceHookMock);

			await client.connect();
			await client.set("once-hook-test", "value");

			// First call should trigger the hook
			await client.get("once-hook-test");
			expect(onceHookMock).toHaveBeenCalledTimes(1);

			// Second call should not trigger the hook (it was removed after first execution)
			await client.get("once-hook-test");
			expect(onceHookMock).toHaveBeenCalledTimes(1);
		});

		it("should allow clearing all hooks", async () => {
			const hook1 = vi.fn();
			const hook2 = vi.fn();

			client.onHook("before:get", hook1);
			client.onHook("after:get", hook2);

			await client.connect();
			await client.set("clear-hooks-test", "value");

			// First call should trigger hooks
			await client.get("clear-hooks-test");
			expect(hook1).toHaveBeenCalledTimes(1);
			expect(hook2).toHaveBeenCalledTimes(1);

			// Clear all hooks
			client.clearHooks();

			// Second call should not trigger any hooks
			await client.get("clear-hooks-test");
			expect(hook1).toHaveBeenCalledTimes(1);
			expect(hook2).toHaveBeenCalledTimes(1);
		});

		it("should maintain hook execution order", async () => {
			const executionOrder: string[] = [];

			client.onHook("before:get", () => {
				executionOrder.push("before1");
			});

			client.onHook("before:get", () => {
				executionOrder.push("before2");
			});

			client.onHook("after:get", () => {
				executionOrder.push("after1");
			});

			client.onHook("after:get", () => {
				executionOrder.push("after2");
			});

			await client.connect();
			await client.set("order-test", "value");
			await client.get("order-test");

			expect(executionOrder).toEqual([
				"before1",
				"before2",
				"after1",
				"after2",
			]);
		});

		it("should call beforeHook and afterHook for set operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:set", beforeHookMock);
			client.onHook("after:set", afterHookMock);

			await client.connect();

			const result = await client.set(
				"set-hook-test",
				"set-hook-value",
				3600,
				42,
			);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "set-hook-test",
				value: "set-hook-value",
				exptime: 3600,
				flags: 42,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "set-hook-test",
				value: "set-hook-value",
				exptime: 3600,
				flags: 42,
				success: true,
			});

			expect(result).toBe(true);
		});

		it("should call set hooks with default parameters", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:set", beforeHookMock);
			client.onHook("after:set", afterHookMock);

			await client.connect();

			const result = await client.set("set-hook-default", "default-value");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "set-hook-default",
				value: "default-value",
				exptime: 0,
				flags: 0,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "set-hook-default",
				value: "default-value",
				exptime: 0,
				flags: 0,
				success: true,
			});

			expect(result).toBe(true);
		});

		it("should handle async set hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:set", asyncBeforeHook);
			client.onHook("after:set", asyncAfterHook);

			await client.connect();

			const result = await client.set("async-set-test", "async-value");

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(true);
		});

		it("should call beforeHook and afterHook for gets operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:gets", beforeHookMock);
			client.onHook("after:gets", afterHookMock);

			await client.connect();

			// Set some values first
			await client.set("gets-hook-1", "value1");
			await client.set("gets-hook-2", "value2");
			await client.set("gets-hook-3", "value3");

			const keys = [
				"gets-hook-1",
				"gets-hook-2",
				"gets-hook-3",
				"non-existent",
			];
			const result = await client.gets(keys);

			expect(beforeHookMock).toHaveBeenCalledWith({
				keys: ["gets-hook-1", "gets-hook-2", "gets-hook-3", "non-existent"],
			});

			const expectedMap = new Map([
				["gets-hook-1", "value1"],
				["gets-hook-2", "value2"],
				["gets-hook-3", "value3"],
			]);

			expect(afterHookMock).toHaveBeenCalledWith({
				keys: ["gets-hook-1", "gets-hook-2", "gets-hook-3", "non-existent"],
				values: expectedMap,
			});

			expect(result.get("gets-hook-1")).toBe("value1");
			expect(result.get("gets-hook-2")).toBe("value2");
			expect(result.get("gets-hook-3")).toBe("value3");
			expect(result.has("non-existent")).toBe(false);
		});

		it("should call gets hooks when all keys are missing", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:gets", beforeHookMock);
			client.onHook("after:gets", afterHookMock);

			await client.connect();

			const keys = ["missing1", "missing2", "missing3"];
			const result = await client.gets(keys);

			expect(beforeHookMock).toHaveBeenCalledWith({
				keys: ["missing1", "missing2", "missing3"],
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				keys: ["missing1", "missing2", "missing3"],
				values: new Map(), // Empty map when no keys found
			});

			expect(result.size).toBe(0);
		});

		it("should handle async gets hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:gets", asyncBeforeHook);
			client.onHook("after:gets", asyncAfterHook);

			await client.connect();
			await client.set("async-gets-1", "value1");
			await client.set("async-gets-2", "value2");

			const start = Date.now();
			const result = await client.gets(["async-gets-1", "async-gets-2"]);
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result.get("async-gets-1")).toBe("value1");
			expect(result.get("async-gets-2")).toBe("value2");
			expect(duration).toBeGreaterThanOrEqual(20); // At least 20ms for both hooks
		});

		it("should call beforeHook and afterHook for add operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:add", beforeHookMock);
			client.onHook("after:add", afterHookMock);

			await client.connect();

			// First add should succeed
			const result = await client.add("add-hook-test", "add-value", 3600, 10);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "add-hook-test",
				value: "add-value",
				exptime: 3600,
				flags: 10,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "add-hook-test",
				value: "add-value",
				exptime: 3600,
				flags: 10,
				success: true,
			});

			expect(result).toBe(true);

			// Second add should fail (key already exists)
			beforeHookMock.mockClear();
			afterHookMock.mockClear();

			const result2 = await client.add(
				"add-hook-test",
				"another-value",
				1800,
				5,
			);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "add-hook-test",
				value: "another-value",
				exptime: 1800,
				flags: 5,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "add-hook-test",
				value: "another-value",
				exptime: 1800,
				flags: 5,
				success: false,
			});

			expect(result2).toBe(false);
		});

		it("should call add hooks with default parameters", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:add", beforeHookMock);
			client.onHook("after:add", afterHookMock);

			await client.connect();

			// Ensure key doesn't exist
			await client.delete("add-hook-default");

			const result = await client.add("add-hook-default", "default-value");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "add-hook-default",
				value: "default-value",
				exptime: 0,
				flags: 0,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "add-hook-default",
				value: "default-value",
				exptime: 0,
				flags: 0,
				success: true,
			});

			expect(result).toBe(true);
		});

		it("should handle async add hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:add", asyncBeforeHook);
			client.onHook("after:add", asyncAfterHook);

			await client.connect();

			// Ensure key doesn't exist
			await client.delete("async-add-test");

			const start = Date.now();
			const result = await client.add("async-add-test", "async-value");
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(true);
			expect(duration).toBeGreaterThanOrEqual(20); // At least 20ms for both hooks
		});

		it("should call beforeHook and afterHook for replace operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:replace", beforeHookMock);
			client.onHook("after:replace", afterHookMock);

			await client.connect();

			// First replace should fail (key doesn't exist)
			const result1 = await client.replace(
				"replace-hook-test",
				"replace-value",
				3600,
				15,
			);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "replace-hook-test",
				value: "replace-value",
				exptime: 3600,
				flags: 15,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "replace-hook-test",
				value: "replace-value",
				exptime: 3600,
				flags: 15,
				success: false,
			});

			expect(result1).toBe(false);

			// Set the key first
			await client.set("replace-hook-test", "initial-value");

			// Clear mocks
			beforeHookMock.mockClear();
			afterHookMock.mockClear();

			// Now replace should succeed
			const result2 = await client.replace(
				"replace-hook-test",
				"new-value",
				1800,
				20,
			);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "replace-hook-test",
				value: "new-value",
				exptime: 1800,
				flags: 20,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "replace-hook-test",
				value: "new-value",
				exptime: 1800,
				flags: 20,
				success: true,
			});

			expect(result2).toBe(true);

			// Verify the value was replaced
			const getValue = await client.get("replace-hook-test");
			expect(getValue).toBe("new-value");
		});

		it("should call replace hooks with default parameters", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:replace", beforeHookMock);
			client.onHook("after:replace", afterHookMock);

			await client.connect();

			// Set a key first
			await client.set("replace-hook-default", "initial");

			const result = await client.replace(
				"replace-hook-default",
				"replaced-value",
			);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "replace-hook-default",
				value: "replaced-value",
				exptime: 0,
				flags: 0,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "replace-hook-default",
				value: "replaced-value",
				exptime: 0,
				flags: 0,
				success: true,
			});

			expect(result).toBe(true);
		});

		it("should handle async replace hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:replace", asyncBeforeHook);
			client.onHook("after:replace", asyncAfterHook);

			await client.connect();

			// Set a key first
			await client.set("async-replace-test", "initial");

			const start = Date.now();
			const result = await client.replace(
				"async-replace-test",
				"async-replaced",
			);
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(true);
			expect(duration).toBeGreaterThanOrEqual(20); // At least 20ms for both hooks
		});

		it("should call beforeHook and afterHook for append operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:append", beforeHookMock);
			client.onHook("after:append", afterHookMock);

			await client.connect();

			// First append should fail (key doesn't exist)
			const result1 = await client.append("append-hook-test", "-appended");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "append-hook-test",
				value: "-appended",
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "append-hook-test",
				value: "-appended",
				success: false,
			});

			expect(result1).toBe(false);

			// Set the key first
			await client.set("append-hook-test", "initial");

			// Clear mocks
			beforeHookMock.mockClear();
			afterHookMock.mockClear();

			// Now append should succeed
			const result2 = await client.append("append-hook-test", "-appended");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "append-hook-test",
				value: "-appended",
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "append-hook-test",
				value: "-appended",
				success: true,
			});

			expect(result2).toBe(true);

			// Verify the value was appended
			const getValue = await client.get("append-hook-test");
			expect(getValue).toBe("initial-appended");
		});

		it("should handle multiple append hooks", async () => {
			const beforeHook1 = vi.fn();
			const beforeHook2 = vi.fn();
			const afterHook1 = vi.fn();
			const afterHook2 = vi.fn();

			client.onHook("before:append", beforeHook1);
			client.onHook("before:append", beforeHook2);
			client.onHook("after:append", afterHook1);
			client.onHook("after:append", afterHook2);

			await client.connect();

			// Set a key first
			await client.set("multi-append-hook", "start");
			await client.append("multi-append-hook", "-end");

			expect(beforeHook1).toHaveBeenCalled();
			expect(beforeHook2).toHaveBeenCalled();
			expect(afterHook1).toHaveBeenCalled();
			expect(afterHook2).toHaveBeenCalled();
		});

		it("should handle async append hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:append", asyncBeforeHook);
			client.onHook("after:append", asyncAfterHook);

			await client.connect();

			// Set a key first
			await client.set("async-append-test", "initial");

			const start = Date.now();
			const result = await client.append("async-append-test", "-async");
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(true);
			expect(duration).toBeGreaterThanOrEqual(20); // At least 20ms for both hooks

			// Verify the append worked
			const getValue = await client.get("async-append-test");
			expect(getValue).toBe("initial-async");
		});

		it("should call beforeHook and afterHook for prepend operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:prepend", beforeHookMock);
			client.onHook("after:prepend", afterHookMock);

			await client.connect();

			// First prepend should fail (key doesn't exist)
			const result1 = await client.prepend("prepend-hook-test", "prefix-");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "prepend-hook-test",
				value: "prefix-",
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "prepend-hook-test",
				value: "prefix-",
				success: false,
			});

			expect(result1).toBe(false);

			// Set the key first
			await client.set("prepend-hook-test", "initial");

			// Clear mocks
			beforeHookMock.mockClear();
			afterHookMock.mockClear();

			// Now prepend should succeed
			const result2 = await client.prepend("prepend-hook-test", "prefix-");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "prepend-hook-test",
				value: "prefix-",
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "prepend-hook-test",
				value: "prefix-",
				success: true,
			});

			expect(result2).toBe(true);

			// Verify the value was prepended
			const getValue = await client.get("prepend-hook-test");
			expect(getValue).toBe("prefix-initial");
		});

		it("should handle async prepend hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:prepend", asyncBeforeHook);
			client.onHook("after:prepend", asyncAfterHook);

			await client.connect();

			// Set a key first
			await client.set("async-prepend-test", "end");

			const start = Date.now();
			const result = await client.prepend("async-prepend-test", "start-");
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(true);
			expect(duration).toBeGreaterThanOrEqual(20); // At least 20ms for both hooks

			// Verify the prepend worked
			const getValue = await client.get("async-prepend-test");
			expect(getValue).toBe("start-end");
		});

		it("should support removing prepend hooks", async () => {
			const hookMock = vi.fn();

			client.onHook("before:prepend", hookMock);

			await client.connect();
			await client.set("remove-prepend-hook", "value");

			// First call should trigger the hook
			await client.prepend("remove-prepend-hook", "pre-");
			expect(hookMock).toHaveBeenCalledTimes(1);

			// Remove the hook
			client.removeHook("before:prepend", hookMock);

			// Second call should not trigger the hook
			await client.prepend("remove-prepend-hook", "another-");
			expect(hookMock).toHaveBeenCalledTimes(1);
		});

		it("should call beforeHook and afterHook for delete operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:delete", beforeHookMock);
			client.onHook("after:delete", afterHookMock);

			await client.connect();

			// First delete should fail (key doesn't exist)
			const result1 = await client.delete("delete-hook-test");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "delete-hook-test",
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "delete-hook-test",
				success: false,
			});

			expect(result1).toBe(false);

			// Set the key first
			await client.set("delete-hook-test", "value-to-delete");

			// Clear mocks
			beforeHookMock.mockClear();
			afterHookMock.mockClear();

			// Now delete should succeed
			const result2 = await client.delete("delete-hook-test");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "delete-hook-test",
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "delete-hook-test",
				success: true,
			});

			expect(result2).toBe(true);

			// Verify the key was deleted
			const getValue = await client.get("delete-hook-test");
			expect(getValue).toBe(undefined);
		});

		it("should handle async delete hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:delete", asyncBeforeHook);
			client.onHook("after:delete", asyncAfterHook);

			await client.connect();

			// Set a key first
			await client.set("async-delete-test", "to-be-deleted");

			const start = Date.now();
			const result = await client.delete("async-delete-test");
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(true);
			expect(duration).toBeGreaterThanOrEqual(20); // At least 20ms for both hooks

			// Verify the delete worked
			const getValue = await client.get("async-delete-test");
			expect(getValue).toBe(undefined);
		});

		it("should handle delete hook errors with throwHookErrors", async () => {
			const errorClient = new Memcache();
			errorClient.throwHookErrors = true;

			const errorHook = vi.fn().mockImplementation(() => {
				throw new Error("Delete hook error");
			});

			errorClient.onHook("before:delete", errorHook);

			await errorClient.connect();
			await errorClient.set("error-delete-test", "value");

			// Hook error should propagate and reject the promise
			await expect(errorClient.delete("error-delete-test")).rejects.toThrow(
				"Delete hook error",
			);
			expect(errorHook).toHaveBeenCalled();

			await errorClient.disconnect();
		});

		it("should call beforeHook and afterHook for incr operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:incr", beforeHookMock);
			client.onHook("after:incr", afterHookMock);

			await client.connect();

			// Set an initial numeric value
			await client.set("incr-hook-test", "10");

			// Increment by 5
			const result = await client.incr("incr-hook-test", 5);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "incr-hook-test",
				value: 5,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "incr-hook-test",
				value: 5,
				newValue: 15,
			});

			expect(result).toBe(15);
		});

		it("should call incr hooks with default increment value", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:incr", beforeHookMock);
			client.onHook("after:incr", afterHookMock);

			await client.connect();

			// Set an initial numeric value
			await client.set("incr-hook-default", "20");

			// Increment with default value (1)
			const result = await client.incr("incr-hook-default");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "incr-hook-default",
				value: 1,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "incr-hook-default",
				value: 1,
				newValue: 21,
			});

			expect(result).toBe(21);
		});

		it("should handle incr hooks when key doesn't exist", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:incr", beforeHookMock);
			client.onHook("after:incr", afterHookMock);

			await client.connect();

			// Try to increment a non-existent key
			const result = await client.incr("incr-hook-nonexistent", 3);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "incr-hook-nonexistent",
				value: 3,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "incr-hook-nonexistent",
				value: 3,
				newValue: undefined,
			});

			expect(result).toBe(undefined);
		});

		it("should handle async incr hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:incr", asyncBeforeHook);
			client.onHook("after:incr", asyncAfterHook);

			await client.connect();

			// Set an initial numeric value
			await client.set("async-incr-test", "100");

			const start = Date.now();
			const result = await client.incr("async-incr-test", 10);
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(110);
			expect(duration).toBeGreaterThanOrEqual(20); // At least 20ms for both hooks
		});

		it("should call beforeHook and afterHook for decr operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:decr", beforeHookMock);
			client.onHook("after:decr", afterHookMock);

			await client.connect();

			// Set an initial numeric value
			await client.set("decr-hook-test", "50");

			// Decrement by 7
			const result = await client.decr("decr-hook-test", 7);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "decr-hook-test",
				value: 7,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "decr-hook-test",
				value: 7,
				newValue: 43,
			});

			expect(result).toBe(43);
		});

		it("should call decr hooks with default decrement value", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:decr", beforeHookMock);
			client.onHook("after:decr", afterHookMock);

			await client.connect();

			// Set an initial numeric value
			await client.set("decr-hook-default", "30");

			// Decrement with default value (1)
			const result = await client.decr("decr-hook-default");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "decr-hook-default",
				value: 1,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "decr-hook-default",
				value: 1,
				newValue: 29,
			});

			expect(result).toBe(29);
		});

		it("should handle decr hooks when key doesn't exist", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:decr", beforeHookMock);
			client.onHook("after:decr", afterHookMock);

			await client.connect();

			// Try to decrement a non-existent key
			const result = await client.decr("decr-hook-nonexistent", 5);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "decr-hook-nonexistent",
				value: 5,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "decr-hook-nonexistent",
				value: 5,
				newValue: undefined,
			});

			expect(result).toBe(undefined);
		});

		it("should handle async decr hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:decr", asyncBeforeHook);
			client.onHook("after:decr", asyncAfterHook);

			await client.connect();

			// Set an initial numeric value
			await client.set("async-decr-test", "200");

			const start = Date.now();
			const result = await client.decr("async-decr-test", 25);
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(175);
			expect(duration).toBeGreaterThanOrEqual(15); // At least 15ms for both hooks (with tolerance for timing variations)
		});

		it("should handle decr not going below zero", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:decr", beforeHookMock);
			client.onHook("after:decr", afterHookMock);

			await client.connect();

			// Set a small value
			await client.set("decr-zero-test", "5");

			// Try to decrement by more than the value (memcached won't go below 0)
			const result = await client.decr("decr-zero-test", 10);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "decr-zero-test",
				value: 10,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "decr-zero-test",
				value: 10,
				newValue: 0, // Memcached stops at 0
			});

			expect(result).toBe(0);
		});

		it("should call beforeHook and afterHook for touch operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:touch", beforeHookMock);
			client.onHook("after:touch", afterHookMock);

			await client.connect();

			// Set a key first
			await client.set("touch-hook-test", "value");

			// Touch with new exptime
			const result = await client.touch("touch-hook-test", 7200);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "touch-hook-test",
				exptime: 7200,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "touch-hook-test",
				exptime: 7200,
				success: true,
			});

			expect(result).toBe(true);
		});

		it("should handle touch hooks when key doesn't exist", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:touch", beforeHookMock);
			client.onHook("after:touch", afterHookMock);

			await client.connect();

			// Try to touch a non-existent key
			const result = await client.touch("touch-hook-nonexistent", 3600);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "touch-hook-nonexistent",
				exptime: 3600,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "touch-hook-nonexistent",
				exptime: 3600,
				success: false,
			});

			expect(result).toBe(false);
		});

		it("should handle async touch hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:touch", asyncBeforeHook);
			client.onHook("after:touch", asyncAfterHook);

			await client.connect();

			// Set a key first
			await client.set("async-touch-test", "value");

			const start = Date.now();
			const result = await client.touch("async-touch-test", 1800);
			const duration = Date.now() - start;

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(true);
			expect(duration).toBeGreaterThanOrEqual(20); // At least 20ms for both hooks
		});

		it("should support multiple touch hook listeners", async () => {
			const beforeHook1 = vi.fn();
			const beforeHook2 = vi.fn();
			const afterHook1 = vi.fn();
			const afterHook2 = vi.fn();

			client.onHook("before:touch", beforeHook1);
			client.onHook("before:touch", beforeHook2);
			client.onHook("after:touch", afterHook1);
			client.onHook("after:touch", afterHook2);

			await client.connect();

			// Set a key first
			await client.set("multi-touch-hook", "value");
			await client.touch("multi-touch-hook", 900);

			expect(beforeHook1).toHaveBeenCalled();
			expect(beforeHook2).toHaveBeenCalled();
			expect(afterHook1).toHaveBeenCalled();
			expect(afterHook2).toHaveBeenCalled();
		});

		it("should call beforeHook and afterHook for cas operation", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:cas", beforeHookMock);
			client.onHook("after:cas", afterHookMock);

			await client.connect();

			const socket = (client as any).socket as Socket;

			// Set a key first
			await client.set("cas-hook-test", "initial-value");

			// Mock socket write to simulate STORED response for CAS
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					socket.emit("data", "STORED\r\n");
				});
				return true;
			});

			// Perform CAS
			const result = await client.cas(
				"cas-hook-test",
				"new-value",
				"12345",
				3600,
				10,
			);

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "cas-hook-test",
				value: "new-value",
				casToken: "12345",
				exptime: 3600,
				flags: 10,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "cas-hook-test",
				value: "new-value",
				casToken: "12345",
				exptime: 3600,
				flags: 10,
				success: true,
			});

			expect(result).toBe(true);

			socket.write = originalWrite;
		});

		it("should call cas hooks with default parameters", async () => {
			const beforeHookMock = vi.fn();
			const afterHookMock = vi.fn();

			client.onHook("before:cas", beforeHookMock);
			client.onHook("after:cas", afterHookMock);

			await client.connect();

			const socket = (client as any).socket as Socket;

			// Set a key first
			await client.set("cas-hook-default", "initial");

			// Mock socket write to simulate STORED response for CAS
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					socket.emit("data", "STORED\r\n");
				});
				return true;
			});

			const result = await client.cas("cas-hook-default", "updated", "54321");

			expect(beforeHookMock).toHaveBeenCalledWith({
				key: "cas-hook-default",
				value: "updated",
				casToken: "54321",
				exptime: 0,
				flags: 0,
			});

			expect(afterHookMock).toHaveBeenCalledWith({
				key: "cas-hook-default",
				value: "updated",
				casToken: "54321",
				exptime: 0,
				flags: 0,
				success: true,
			});

			expect(result).toBe(true);

			socket.write = originalWrite;
		});

		it("should handle async cas hooks", async () => {
			const asyncBeforeHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			const asyncAfterHook = vi.fn().mockImplementation(async () => {
				return new Promise((resolve) => setTimeout(resolve, 10));
			});

			client.onHook("before:cas", asyncBeforeHook);
			client.onHook("after:cas", asyncAfterHook);

			await client.connect();

			const socket = (client as any).socket as Socket;

			// Set a key first
			await client.set("async-cas-test", "initial");

			// Mock socket write to simulate STORED response for CAS
			const originalWrite = socket.write;
			socket.write = vi.fn().mockImplementation(() => {
				setImmediate(() => {
					socket.emit("data", "STORED\r\n");
				});
				return true;
			});

			const result = await client.cas("async-cas-test", "updated", "99999");

			expect(asyncBeforeHook).toHaveBeenCalled();
			expect(asyncAfterHook).toHaveBeenCalled();
			expect(result).toBe(true);

			socket.write = originalWrite;
		});

		it("should handle cas hook errors with throwHookErrors", async () => {
			const errorClient = new Memcache();
			errorClient.throwHookErrors = true;

			const errorHook = vi.fn().mockImplementation(() => {
				throw new Error("CAS hook error");
			});

			errorClient.onHook("before:cas", errorHook);

			await errorClient.connect();
			await errorClient.set("error-cas-test", "value");

			// Hook error should propagate and reject the promise
			await expect(
				errorClient.cas("error-cas-test", "new-value", "12345"),
			).rejects.toThrow("CAS hook error");
			expect(errorHook).toHaveBeenCalled();

			await errorClient.disconnect();
		});

		it("should support multiple cas hook listeners", async () => {
			const beforeHook1 = vi.fn();
			const beforeHook2 = vi.fn();
			const afterHook1 = vi.fn();
			const afterHook2 = vi.fn();

			client.onHook("before:cas", beforeHook1);
			client.onHook("before:cas", beforeHook2);
			client.onHook("after:cas", afterHook1);
			client.onHook("after:cas", afterHook2);

			await client.connect();

			// Set a key first
			await client.set("multi-cas-hook", "start");
			await client.cas("multi-cas-hook", "end", "77777");

			expect(beforeHook1).toHaveBeenCalled();
			expect(beforeHook2).toHaveBeenCalled();
			expect(afterHook1).toHaveBeenCalled();
			expect(afterHook2).toHaveBeenCalled();
		});
	});

	describe("HashRing Integration", () => {
		it("should expose ring property", () => {
			expect(client.ring).toBeDefined();
		});

		it("should return default node initially", () => {
			expect(client.nodes).toEqual(["localhost:11211"]);
		});

		it("should return nodes as string array", () => {
			client.ring.addNode("localhost:11211");
			client.ring.addNode("localhost:11212");
			client.ring.addNode("127.0.0.1:11213");

			const nodes = client.nodes;
			expect(nodes).toHaveLength(3);
			expect(nodes).toContain("localhost:11211");
			expect(nodes).toContain("localhost:11212");
			expect(nodes).toContain("127.0.0.1:11213");
		});

		it("should allow adding nodes to ring", () => {
			// Client starts with default localhost:11211 node
			client.ring.addNode("server1");
			client.ring.addNode("server2");
			client.ring.addNode("server3");

			expect(client.ring.nodes.size).toBe(4); // 3 + default
			expect(client.ring.nodes.has("server1")).toBe(true);
			expect(client.ring.nodes.has("server2")).toBe(true);
			expect(client.ring.nodes.has("server3")).toBe(true);
		});

		it("should allow getting node for a key", () => {
			client.ring.addNode("server1");
			client.ring.addNode("server2");
			client.ring.addNode("server3");

			const node = client.ring.getNode("test-key");
			expect(node).toBeDefined();
			expect(["server1", "server2", "server3"]).toContain(node);
		});

		it("should return consistent node for same key", () => {
			client.ring.addNode("server1");
			client.ring.addNode("server2");
			client.ring.addNode("server3");

			const node1 = client.ring.getNode("test-key");
			const node2 = client.ring.getNode("test-key");
			expect(node1).toBe(node2);
		});

		it("should allow removing nodes from ring", () => {
			// Client starts with default localhost:11211 node
			client.ring.addNode("server1");
			client.ring.addNode("server2");
			client.ring.addNode("server3");

			expect(client.ring.nodes.size).toBe(4); // 3 + default

			client.ring.removeNode("server2");
			expect(client.ring.nodes.size).toBe(3);
			expect(client.ring.nodes.has("server2")).toBe(false);
		});

		it("should allow adding weighted nodes", () => {
			// Client starts with default localhost:11211 node
			client.ring.addNode("heavy-server", 3);
			client.ring.addNode("light-server", 1);

			expect(client.ring.nodes.size).toBe(3); // 2 + default

			// Heavy server should get more keys
			const distribution = new Map<string, number>();
			for (let i = 0; i < 100; i++) {
				const node = client.ring.getNode(`key-${i}`);
				if (node) {
					distribution.set(node, (distribution.get(node) || 0) + 1);
				}
			}

			const heavy = distribution.get("heavy-server") || 0;
			const light = distribution.get("light-server") || 0;
			expect(heavy).toBeGreaterThan(light);
		});

		it("should support getting multiple replica nodes", () => {
			client.ring.addNode("server1");
			client.ring.addNode("server2");
			client.ring.addNode("server3");
			client.ring.addNode("server4");

			const replicas = client.ring.getNodes("test-key", 3);
			expect(replicas).toHaveLength(3);

			// All replicas should be unique
			const uniqueReplicas = new Set(replicas);
			expect(uniqueReplicas.size).toBe(3);
		});

		it("should handle ring with only default node", () => {
			// Client starts with default localhost:11211 node
			const node = client.ring.getNode("test-key");
			expect(node).toBe("localhost:11211");

			const replicas = client.ring.getNodes("test-key", 3);
			expect(replicas).toEqual(["localhost:11211"]);
		});
	});

	describe("MemcacheEvents", () => {
		it("should export MemcacheEvents enum with correct values", () => {
			expect(MemcacheEvents.CONNECT).toBe("connect");
			expect(MemcacheEvents.QUIT).toBe("quit");
			expect(MemcacheEvents.HIT).toBe("hit");
			expect(MemcacheEvents.MISS).toBe("miss");
			expect(MemcacheEvents.ERROR).toBe("error");
			expect(MemcacheEvents.WARN).toBe("warn");
			expect(MemcacheEvents.INFO).toBe("info");
			expect(MemcacheEvents.TIMEOUT).toBe("timeout");
			expect(MemcacheEvents.CLOSE).toBe("close");
		});

		it("should emit connect event on connection", async () => {
			let connectEmitted = false;
			client.on(MemcacheEvents.CONNECT, () => {
				connectEmitted = true;
			});

			await client.connect();
			expect(connectEmitted).toBe(true);
		});

		it("should emit error event", async () => {
			let errorEmitted = false;
			let errorMessage = "";
			client.on(MemcacheEvents.ERROR, (error: Error) => {
				errorEmitted = true;
				errorMessage = error.message;
			});

			await client.connect();
			const socket = (client as any).socket as Socket;
			socket.emit("error", new Error("Test error"));

			expect(errorEmitted).toBe(true);
			expect(errorMessage).toBe("Test error");
		});

		it("should emit timeout event", async () => {
			let timeoutEmitted = false;
			client.on(MemcacheEvents.TIMEOUT, () => {
				timeoutEmitted = true;
			});

			const connectPromise = client.connect();
			const socket = (client as any).socket as Socket;
			socket.emit("timeout");

			await expect(connectPromise).rejects.toThrow("Connection timeout");
			expect(timeoutEmitted).toBe(true);
		});

		it("should emit close event", async () => {
			let closeEmitted = false;
			client.on(MemcacheEvents.CLOSE, () => {
				closeEmitted = true;
			});

			await client.connect();
			const socket = (client as any).socket as Socket;
			socket.emit("close");

			expect(closeEmitted).toBe(true);
		});

		it("should emit hit event with key and value on successful get", async () => {
			let hitEmitted = false;
			let hitKey = "";
			let hitValue = "";

			client.on(MemcacheEvents.HIT, (key: string, value: string) => {
				hitEmitted = true;
				hitKey = key;
				hitValue = value;
			});

			await client.connect();
			await client.set("test-hit", "test-value");
			await client.get("test-hit");

			expect(hitEmitted).toBe(true);
			expect(hitKey).toBe("test-hit");
			expect(hitValue).toBe("test-value");
		});

		it("should emit miss event with key on failed get", async () => {
			let missEmitted = false;
			let missKey = "";

			client.on(MemcacheEvents.MISS, (key: string) => {
				missEmitted = true;
				missKey = key;
			});

			await client.connect();
			await client.get("non-existent-key");

			expect(missEmitted).toBe(true);
			expect(missKey).toBe("non-existent-key");
		});

		it("should emit hit events for multiple gets with mixed results", async () => {
			const hits: Array<{ key: string; value: string }> = [];
			const misses: string[] = [];

			client.on(MemcacheEvents.HIT, (key: string, value: string) => {
				hits.push({ key, value });
			});

			client.on(MemcacheEvents.MISS, (key: string) => {
				misses.push(key);
			});

			await client.connect();
			await client.set("exists1", "value1");
			await client.set("exists2", "value2");

			await client.gets(["exists1", "non-existent", "exists2", "another-miss"]);

			expect(hits).toHaveLength(2);
			expect(hits[0]).toEqual({ key: "exists1", value: "value1" });
			expect(hits[1]).toEqual({ key: "exists2", value: "value2" });
			expect(misses).toEqual(["non-existent", "another-miss"]);
		});

		it("should emit miss events for all keys when gets returns no results", async () => {
			const misses: string[] = [];

			client.on(MemcacheEvents.MISS, (key: string) => {
				misses.push(key);
			});

			await client.connect();

			// Simulate gets with no results by mocking the response
			const socket = (client as any).socket as Socket;
			const getPromise = client.gets(["key1", "key2", "key3"]);

			// Simulate END response with no VALUE lines (empty result)
			socket.emit("data", "END\r\n");

			await getPromise;
			expect(misses).toEqual(["key1", "key2", "key3"]);
		});
	});
});
