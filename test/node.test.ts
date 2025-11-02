// biome-ignore-all lint/suspicious/noExplicitAny: test file
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MemcacheNode } from "../src/node";

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
			const key = "node-test-reconnect";
			const value = "initial-value";
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
			const promise = node.command("get slow-key", { isMultiline: true });

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
			const key = "node-test-set";
			const value = "test-value";
			const bytes = Buffer.byteLength(value);
			const cmd = `set ${key} 0 0 ${bytes}\r\n${value}`;
			const result = await node.command(cmd);
			expect(result).toBe("STORED");
		});

		it("should execute get command with multiline option", async () => {
			const key = "node-test-get";
			const value = "test-value";

			// First set the value
			const bytes = Buffer.byteLength(value);
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Then get it
			const result = await node.command(`get ${key}`, { isMultiline: true });
			expect(result).toBeInstanceOf(Array);
			expect(result[0]).toBe(value);
		});

		it("should execute get command for non-existent key", async () => {
			const key = "node-test-nonexistent";
			const result = await node.command(`get ${key}`, { isMultiline: true });
			expect(result).toBeUndefined();
		});

		it("should execute delete command", async () => {
			const key = "node-test-delete";
			const value = "test-value";
			const bytes = Buffer.byteLength(value);

			// Set first
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Delete
			const result = await node.command(`delete ${key}`);
			expect(result).toBe("DELETED");
		});

		it("should execute incr command", async () => {
			const key = "node-test-incr";

			// Set initial value
			await node.command(`set ${key} 0 0 1\r\n0`);

			// Increment
			const result = await node.command(`incr ${key} 1`);
			expect(result).toBe(1);
		});

		it("should execute decr command", async () => {
			const key = "node-test-decr";

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
			const key = "node-test-touch";
			const value = "test-value";
			const bytes = Buffer.byteLength(value);

			// Set first
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Touch
			const result = await node.command(`touch ${key} 100`);
			expect(result).toBe("TOUCHED");
		});

		it("should execute flush_all command", async () => {
			const result = await node.command("flush_all");
			expect(result).toBe("OK");
		});

		it("should handle NOT_STORED response for add command", async () => {
			const key = "node-test-add-duplicate";
			const value = "test-value";
			const bytes = Buffer.byteLength(value);

			// First set the key
			await node.command(`set ${key} 0 0 ${bytes}\r\n${value}`);

			// Try to add the same key (should fail since it exists)
			const result = await node.command(`add ${key} 0 0 ${bytes}\r\n${value}`);
			expect(result).toBe(false);
		});

		it("should handle EXISTS response for cas command", async () => {
			const key = "node-test-cas-exists";
			const value = "test-value";
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
			const key = "node-test-delete-nonexistent";

			// Try to delete a key that doesn't exist
			const mockSocket = (node as any)._socket;
			const commandPromise = node.command(`delete ${key}`);

			// Simulate server NOT_FOUND response
			mockSocket.emit("data", "NOT_FOUND\r\n");

			const result = await commandPromise;
			expect(result).toBe("NOT_FOUND");
		});

		it("should handle multiple sequential commands", async () => {
			const key1 = "node-test-seq1";
			const key2 = "node-test-seq2";
			const value = "test";
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
			const key = "node-test-hit";
			const value = "test-value";
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
			const key = "node-test-miss";

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
			const promise = node.command("get slow-key", { isMultiline: true });

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
			const key1 = "node-fifo-1";
			const key2 = "node-fifo-2";
			const key3 = "node-fifo-3";
			const value = "test";
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

	describe("Multiline Response Handling", () => {
		beforeEach(async () => {
			await node.connect();
		});

		it("should handle multiple keys in get command", async () => {
			const key1 = "node-multi-1";
			const key2 = "node-multi-2";
			const value1 = "value1";
			const value2 = "value2";

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
			const key = "node-large";
			const value = "x".repeat(10000); // 10KB value
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
			const key = "node-partial";
			const value = "test-value-12345";
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

			const mockSocket = (node as any)._socket;
			const commandPromise = node.command("get test_key", {
				isMultiline: true,
				requestedKeys: ["test_key"],
			});

			// Simulate server ERROR response
			mockSocket.emit("data", "ERROR\r\n");

			await expect(commandPromise).rejects.toThrow("ERROR");
		});

		it("should handle CLIENT_ERROR response for multiline get command", async () => {
			await node.connect();

			const mockSocket = (node as any)._socket;
			const commandPromise = node.command("get test_key", {
				isMultiline: true,
				requestedKeys: ["test_key"],
			});

			// Simulate server CLIENT_ERROR response
			mockSocket.emit("data", "CLIENT_ERROR invalid key\r\n");

			await expect(commandPromise).rejects.toThrow("CLIENT_ERROR invalid key");
		});

		it("should handle SERVER_ERROR response for multiline get command", async () => {
			await node.connect();

			const mockSocket = (node as any)._socket;
			const commandPromise = node.command("get test_key", {
				isMultiline: true,
				requestedKeys: ["test_key"],
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
			const commandPromise = node.command("get pending_key", {
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
			const promise1 = node.command("get key1", { isMultiline: true });
			const promise2 = node.command("get key2", { isMultiline: true });
			const promise3 = node.command("get key3", { isMultiline: true });

			// Disconnect immediately
			await node.disconnect();

			// All commands should be rejected
			await expect(promise1).rejects.toThrow();
			await expect(promise2).rejects.toThrow();
			await expect(promise3).rejects.toThrow();
		});
	});
});
