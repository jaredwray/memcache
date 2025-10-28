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

		it("should use default options if not provided", () => {
			const testNode = new MemcacheNode("localhost", 11211);
			expect(testNode).toBeInstanceOf(MemcacheNode);
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
	});
});
