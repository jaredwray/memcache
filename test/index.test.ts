// biome-ignore-all lint/suspicious/noExplicitAny: test file
import type { Socket } from "net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Memcache from "../src/index";

describe("Memcache", () => {
	let client: Memcache;

	beforeEach(() => {
		client = new Memcache({
			host: "localhost",
			port: 11211,
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
				host: "127.0.0.1",
				port: 11212,
				timeout: 10000,
				keepAlive: false,
			});
			expect(customClient).toBeInstanceOf(Memcache);
		});

		it("should allow setting timeout via setter", () => {
			const testClient = new Memcache();
			expect(testClient.timeout).toBe(5000); // Default timeout

			testClient.timeout = 10000;
			expect(testClient.timeout).toBe(10000);

			testClient.timeout = 2000;
			expect(testClient.timeout).toBe(2000);
		});

		it("should allow getting and setting host property", () => {
			const testClient = new Memcache();
			expect(testClient.host).toBe("localhost"); // Default host

			testClient.host = "127.0.0.1";
			expect(testClient.host).toBe("127.0.0.1");

			testClient.host = "memcache.example.com";
			expect(testClient.host).toBe("memcache.example.com");
		});

		it("should initialize with custom host", () => {
			const testClient = new Memcache({ host: "192.168.1.100" });
			expect(testClient.host).toBe("192.168.1.100");
		});

		it("should allow getting and setting port property", () => {
			const testClient = new Memcache();
			expect(testClient.port).toBe(11211); // Default port

			testClient.port = 11212;
			expect(testClient.port).toBe(11212);

			testClient.port = 9999;
			expect(testClient.port).toBe(9999);
		});

		it("should initialize with custom port", () => {
			const testClient = new Memcache({ port: 11212 });
			expect(testClient.port).toBe(11212);
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

		it("should allow getting and setting buffer property", () => {
			const testClient = new Memcache();
			expect(testClient.buffer).toBe(""); // Default buffer

			testClient.buffer = "test data";
			expect(testClient.buffer).toBe("test data");

			testClient.buffer = "more data\r\n";
			expect(testClient.buffer).toBe("more data\r\n");
		});

		it("should allow getting and setting multilineData property", () => {
			const testClient = new Memcache();
			expect(testClient.multilineData).toEqual([]); // Default empty array

			testClient.multilineData = ["line1", "line2"];
			expect(testClient.multilineData).toEqual(["line1", "line2"]);

			testClient.multilineData = ["data1", "data2", "data3"];
			expect(testClient.multilineData).toEqual(["data1", "data2", "data3"]);
		});

		it("should allow setting socket property", () => {
			const testClient = new Memcache();
			expect(testClient.socket).toBe(null); // Default socket

			const mockSocket = { fake: "socket" } as any;
			testClient.socket = mockSocket;
			expect(testClient.socket).toBe(mockSocket);

			testClient.socket = null;
			expect(testClient.socket).toBe(null);
		});

		it("should allow setting connected property", () => {
			const testClient = new Memcache();
			expect(testClient.connected).toBe(false); // Default connected state

			testClient.connected = true;
			expect(testClient.connected).toBe(true);

			testClient.connected = false;
			expect(testClient.connected).toBe(false);
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
				host: "0.0.0.0",
				port: 99999, // Invalid port
				timeout: 100,
			});

			await expect(client13.connect()).rejects.toThrow();
		});

		it("should handle connection timeout", async () => {
			const client14 = new Memcache({
				host: "192.0.2.0", // Non-routable IP address that will timeout
				port: 11211,
				timeout: 100, // Very short timeout
			});

			await expect(client14.connect()).rejects.toThrow("Connection timeout");
		});

		it("should handle error event before connection is established", async () => {
			const client16 = new Memcache({
				host: "localhost",
				port: 99999, // Invalid port that will cause immediate error
				timeout: 100,
			});

			// This should trigger an error immediately due to invalid port
			await expect(client16.connect()).rejects.toThrow();
		});

		it("should reject on socket error before connection", async () => {
			const client18 = new Memcache({
				host: "localhost",
				port: 11211,
				timeout: 5000,
			});

			// Start connect and immediately simulate error
			const connectPromise = client18.connect();

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

			// Start a command but don't await it
			const pendingCommand = client2.get("test-key").catch((e) => e.message);

			// Force disconnect while command is pending
			client2.disconnect();

			const result = await pendingCommand;
			expect(result).toBe("Connection closed");
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
			expect(privateClient.currentCommand).toBe(null);

			client17.disconnect();
		});

		it("should handle multiple pending commands when connection closes", async () => {
			const client3 = new Memcache();
			await client3.connect();

			// Start multiple commands without awaiting
			const commands = [
				client3.get("key1").catch((e) => e.message),
				client3.get("key2").catch((e) => e.message),
				client3.get("key3").catch((e) => e.message),
			];

			// Force disconnect
			client3.disconnect();

			const results = await Promise.all(commands);
			expect(results).toEqual([
				"Connection closed",
				"Connection closed",
				"Connection closed",
			]);
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

			// Send a command
			const commandPromise = client5.get("test").catch((e) => e.message);

			// Simulate an error response
			socket.emit("data", "ERROR something went wrong\r\n");

			const result = await commandPromise;
			expect(result).toBe("ERROR something went wrong");

			client5.disconnect();
		});

		it("should handle CLIENT_ERROR responses", async () => {
			const client6 = new Memcache();
			await client6.connect();

			const socket = (client6 as any).socket as Socket;

			// Send a command
			const commandPromise = client6
				.set("test", "value")
				.catch((e) => e.message);

			// Simulate a CLIENT_ERROR response
			socket.emit("data", "CLIENT_ERROR bad command\r\n");

			const result = await commandPromise;
			expect(result).toBe("CLIENT_ERROR bad command");

			client6.disconnect();
		});

		it("should handle SERVER_ERROR responses", async () => {
			const client7 = new Memcache();
			await client7.connect();

			const socket = (client7 as any).socket as Socket;

			// Send a command
			const commandPromise = client7.get("test").catch((e) => e.message);

			// Simulate a SERVER_ERROR response for multiline command
			socket.emit("data", "SERVER_ERROR out of memory\r\n");

			const result = await commandPromise;
			expect(result).toBe("SERVER_ERROR out of memory");

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

			// Send a get command
			const commandPromise = client9.get("test");

			// Simulate a VALUE response with the value and END in one go
			// The protocol parser expects the value data immediately after VALUE line
			socket.emit("data", "VALUE test 0 5\r\nhello\r\nEND\r\n");

			const result = await commandPromise;
			expect(result).toBe("hello");

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

	describe("Integration Tests (requires memcached server)", () => {
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
			expect(getValue).toBe(null);
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
			expect(getValue1).toBe(null);
			expect(getValue2).toBe(null);
		});

		it("should handle flushAll with delay", async () => {
			await client.connect();

			// Test flushAll with delay parameter (just test the command works)
			const flushResult = await client.flushAll(1);
			expect(flushResult).toBe(true);

			// Also test without delay
			const flushResultNoDelay = await client.flushAll();
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
});
