import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Memcache from "../src/index";

describe("Multi-Node Memcache", () => {
	let client: Memcache;

	beforeEach(() => {
		client = new Memcache({
			nodes: ["localhost:11211", "localhost:11212", "localhost:11213"],
			timeout: 5000,
		});
	});

	afterEach(async () => {
		if (client.isConnected()) {
			await client.disconnect();
		}
	});

	describe("Initialization", () => {
		it("should initialize with 3 nodes", () => {
			expect(client.nodes).toHaveLength(3);
			expect(client.nodes).toContain("localhost:11211");
			expect(client.nodes).toContain("localhost:11212");
			expect(client.nodes).toContain("localhost:11213");
		});

		it("should have access to all node instances", () => {
			const nodes = client.getNodes();
			expect(nodes.size).toBe(3);
			expect(nodes.has("localhost:11211")).toBe(true);
			expect(nodes.has("localhost:11212")).toBe(true);
			expect(nodes.has("localhost:11213")).toBe(true);
		});

		it("should have consistent hash ring configured", () => {
			expect(client.ring).toBeDefined();
			expect(client.ring.nodes.size).toBe(3);
		});
	});

	describe("Connection Management", () => {
		it("should connect to all nodes", async () => {
			await client.connect();
			expect(client.isConnected()).toBe(true);

			const nodes = client.getNodes();
			for (const node of nodes.values()) {
				expect(node.isConnected()).toBe(true);
			}
		});

		it("should support lazy connection per node", async () => {
			// Don't explicitly connect
			const key = "lazy-test";
			const value = "test-value";

			// This should auto-connect to the appropriate node
			await client.set(key, value);

			// Only the node handling this key should be connected
			const node = client.getNode(key);
			expect(node).toBeDefined();
			expect(node?.isConnected()).toBe(true);
		});

		it("should disconnect from all nodes", async () => {
			await client.connect();
			expect(client.isConnected()).toBe(true);

			await client.disconnect();
			expect(client.isConnected()).toBe(false);

			const nodes = client.getNodes();
			for (const node of nodes.values()) {
				expect(node.isConnected()).toBe(false);
			}
		});
	});

	describe("Key Distribution via Hash Ring", () => {
		it("should route keys to specific nodes via hash ring", async () => {
			const testKeys = ["key1", "key2", "key3", "key4", "key5"];

			// Get node assignments
			const assignments = new Map<string, string>();
			for (const key of testKeys) {
				const node = client.getNode(key);
				expect(node).toBeDefined();
				// biome-ignore lint/style/noNonNullAssertion: we just checked it
				assignments.set(key, node!.id);
			}

			// Verify consistent routing (same key always goes to same node)
			for (const key of testKeys) {
				const node = client.getNode(key);
				// biome-ignore lint/style/noNonNullAssertion: we checked above
				expect(node!.id).toBe(assignments.get(key));
			}
		});

		it("should distribute keys across multiple nodes", async () => {
			// Create many keys to ensure distribution
			const keys: string[] = [];
			for (let i = 0; i < 100; i++) {
				keys.push(`test-key-${i}`);
			}

			// Count distribution
			const distribution = new Map<string, number>();
			for (const key of keys) {
				const node = client.getNode(key);
				expect(node).toBeDefined();
				// biome-ignore lint/style/noNonNullAssertion: we just checked it
				const nodeId = node!.id;
				distribution.set(nodeId, (distribution.get(nodeId) || 0) + 1);
			}

			// All nodes should have some keys
			expect(distribution.size).toBeGreaterThan(1);

			// Each node should have a reasonable share (not perfect, but not 0 either)
			for (const count of distribution.values()) {
				expect(count).toBeGreaterThan(0);
				expect(count).toBeLessThan(100); // No single node should have all keys
			}
		});
	});

	describe("Data Operations Across Nodes", () => {
		beforeEach(async () => {
			await client.connect();
			await client.flush();
		});

		it("should set and get values on different nodes", async () => {
			const testData = [
				{ key: "node-test-1", value: "value1" },
				{ key: "node-test-2", value: "value2" },
				{ key: "node-test-3", value: "value3" },
			];

			// Set all values
			for (const { key, value } of testData) {
				const result = await client.set(key, value);
				expect(result).toBe(true);
			}

			// Get all values
			for (const { key, value } of testData) {
				const result = await client.get(key);
				expect(result).toBe(value);
			}
		});

		it("should handle gets() with keys from different nodes", async () => {
			// Set values that will likely distribute across nodes
			const testData: Record<string, string> = {};
			for (let i = 0; i < 20; i++) {
				const key = `multi-get-${i}`;
				const value = `value-${i}`;
				testData[key] = value;
				await client.set(key, value);
			}

			// Get all keys at once
			const keys = Object.keys(testData);
			const results = await client.gets(keys);

			// Verify all values
			expect(results.size).toBe(keys.length);
			for (const [key, value] of results.entries()) {
				expect(value).toBe(testData[key]);
			}
		});

		it("should handle delete across nodes", async () => {
			const key1 = "delete-test-1";
			const key2 = "delete-test-2";

			await client.set(key1, "value1");
			await client.set(key2, "value2");

			// Verify both exist
			expect(await client.get(key1)).toBe("value1");
			expect(await client.get(key2)).toBe("value2");

			// Delete both
			expect(await client.delete(key1)).toBe(true);
			expect(await client.delete(key2)).toBe(true);

			// Verify both are gone
			expect(await client.get(key1)).toBeUndefined();
			expect(await client.get(key2)).toBeUndefined();
		});
	});

	describe("Broadcast Operations", () => {
		beforeEach(async () => {
			await client.connect();
		});

		it("should flush all nodes", async () => {
			// Set data on multiple nodes
			await client.set("flush-test-1", "value1");
			await client.set("flush-test-2", "value2");
			await client.set("flush-test-3", "value3");

			// Flush all
			const result = await client.flush();
			expect(result).toBe(true);

			// Verify all data is gone
			expect(await client.get("flush-test-1")).toBeUndefined();
			expect(await client.get("flush-test-2")).toBeUndefined();
			expect(await client.get("flush-test-3")).toBeUndefined();
		});

		it("should get stats from all nodes", async () => {
			const stats = await client.stats();

			// Should have stats from all 3 nodes
			expect(stats.size).toBe(3);
			expect(stats.has("localhost:11211")).toBe(true);
			expect(stats.has("localhost:11212")).toBe(true);
			expect(stats.has("localhost:11213")).toBe(true);

			// Each stat should have version info
			for (const nodeStat of stats.values()) {
				expect(nodeStat.version).toBeDefined();
				expect(typeof nodeStat.version).toBe("string");
			}
		});

		it("should get version from first node", async () => {
			const version = await client.version();
			expect(version).toBeDefined();
			expect(typeof version).toBe("string");
			expect(version).toContain("VERSION");
		});
	});

	describe("Node Management", () => {
		it("should add a new node dynamically", async () => {
			// Start with default nodes
			expect(client.nodes).toHaveLength(3);

			// Add a new node (using port 11214 which doesn't exist, but we can add it to the ring)
			// Note: This won't connect successfully, but should be added to the ring
			await client.addNode("localhost:11214");

			expect(client.nodes).toHaveLength(4);
			expect(client.nodes).toContain("localhost:11214");
		});

		it("should remove a node dynamically", async () => {
			expect(client.nodes).toHaveLength(3);

			// Remove a node
			await client.removeNode("localhost:11213");

			expect(client.nodes).toHaveLength(2);
			expect(client.nodes).not.toContain("localhost:11213");
		});

		it("should throw error when adding duplicate node", async () => {
			await expect(client.addNode("localhost:11211")).rejects.toThrow(
				"already exists",
			);
		});
	});

	describe("Parallel Operations", () => {
		beforeEach(async () => {
			await client.connect();
			await client.flush();
		});

		it("should handle parallel gets() efficiently", async () => {
			// Set 30 keys
			const keys: string[] = [];
			for (let i = 0; i < 30; i++) {
				const key = `parallel-${i}`;
				keys.push(key);
				await client.set(key, `value-${i}`);
			}

			// Get all in parallel via gets()
			const startTime = Date.now();
			const results = await client.gets(keys);
			const duration = Date.now() - startTime;

			// Verify all retrieved
			expect(results.size).toBe(30);

			// Should be reasonably fast (parallel execution)
			// With 3 nodes, should be faster than sequential
			expect(duration).toBeLessThan(1000); // Generous threshold
		});

		it("should handle concurrent set operations", async () => {
			const promises = [];

			for (let i = 0; i < 20; i++) {
				promises.push(client.set(`concurrent-${i}`, `value-${i}`));
			}

			const results = await Promise.all(promises);

			// All should succeed
			expect(results.every((r) => r === true)).toBe(true);

			// Verify all values
			for (let i = 0; i < 20; i++) {
				const value = await client.get(`concurrent-${i}`);
				expect(value).toBe(`value-${i}`);
			}
		});
	});

	describe("Consistent Hashing Behavior", () => {
		it("should maintain key routing after reconnection", async () => {
			await client.connect();

			const testKey = "consistency-test";
			const node1 = client.getNode(testKey);
			expect(node1).toBeDefined();
			// biome-ignore lint/style/noNonNullAssertion: we just checked it
			const nodeId1 = node1!.id;

			// Disconnect and reconnect
			await client.disconnect();
			await client.connect();

			const node2 = client.getNode(testKey);
			expect(node2).toBeDefined();
			// biome-ignore lint/style/noNonNullAssertion: we just checked it
			const nodeId2 = node2!.id;

			// Should route to same node
			expect(nodeId2).toBe(nodeId1);
		});
	});
});
