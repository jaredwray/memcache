import { describe, expect, it } from "vitest";
import { getNodeIndexForKey, type HashFunction, HashRing } from "../src/ketama";
import { MemcacheNode } from "../src/node";

describe("HashRing", () => {
	describe("Constructor", () => {
		it("should create an empty hash ring", () => {
			const ring = new HashRing();
			expect(ring.getNode("test")).toBe(undefined);
		});

		it("should create a hash ring with initial nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			expect(ring.getNode("test")).toBeDefined();
		});

		it("should create a hash ring with weighted nodes", () => {
			const ring = new HashRing([
				{ node: "node1", weight: 2 },
				{ node: "node2", weight: 1 },
			]);
			expect(ring.getNode("test")).toBeDefined();
		});

		it("should support custom hash function", () => {
			const customHash: HashFunction = (input) => {
				let hash = 0;
				for (let i = 0; i < input.length; i++) {
					hash = (hash << 5) - hash + input[i];
					hash = hash & hash;
				}
				return hash;
			};

			const ring = new HashRing(["node1", "node2"], customHash);
			expect(ring.getNode("test")).toBeDefined();
		});

		it("should support string hash algorithm names", () => {
			const ring = new HashRing(["node1", "node2"], "md5");
			expect(ring.getNode("test")).toBeDefined();
		});

		it("should support object nodes with key property", () => {
			const ring = new HashRing([
				{ key: "server1", host: "localhost", port: 11211 },
				{ key: "server2", host: "localhost", port: 11212 },
			]);
			const node = ring.getNode("test");
			expect(node).toBeDefined();
			expect(node).toHaveProperty("key");
			expect(node).toHaveProperty("host");
			expect(node).toHaveProperty("port");
		});
	});

	describe("addNode", () => {
		it("should add a node to the ring", () => {
			const ring = new HashRing<string>();
			ring.addNode("node1");
			expect(ring.getNode("test")).toBe("node1");
		});

		it("should add multiple nodes to the ring", () => {
			const ring = new HashRing<string>();
			ring.addNode("node1");
			ring.addNode("node2");
			ring.addNode("node3");
			expect(ring.getNode("test")).toBeDefined();
		});

		it("should add a node with weight", () => {
			const ring = new HashRing<string>();
			ring.addNode("node1", 2);
			ring.addNode("node2", 1);
			expect(ring.getNode("test")).toBeDefined();
		});

		it("should update existing node weight", () => {
			const ring = new HashRing(["node1", "node2"]);
			ring.addNode("node1", 5);
			expect(ring.getNode("test")).toBeDefined();
		});

		it("should remove node when weight is 0", () => {
			const ring = new HashRing(["node1", "node2"]);
			ring.addNode("node1", 0);
			const node = ring.getNode("test");
			expect(node).toBe("node2");
		});

		it("should throw error for negative weight", () => {
			const ring = new HashRing<string>();
			expect(() => ring.addNode("node1", -1)).toThrow(RangeError);
			expect(() => ring.addNode("node1", -1)).toThrow(
				"Cannot add a node to the hashring with weight < 0",
			);
		});
	});

	describe("removeNode", () => {
		it("should remove a node from the ring", () => {
			const ring = new HashRing(["node1", "node2"]);
			ring.removeNode("node1");
			const node = ring.getNode("test");
			expect(node).toBe("node2");
		});

		it("should handle removing non-existent node", () => {
			const ring = new HashRing(["node1"]);
			ring.removeNode("node2");
			expect(ring.getNode("test")).toBe("node1");
		});

		it("should handle removing all nodes", () => {
			const ring = new HashRing(["node1", "node2"]);
			ring.removeNode("node1");
			ring.removeNode("node2");
			expect(ring.getNode("test")).toBe(undefined);
		});

		it("should remove object nodes correctly", () => {
			const node1 = { key: "server1", host: "localhost" };
			const node2 = { key: "server2", host: "localhost" };
			const ring = new HashRing([node1, node2]);
			ring.removeNode(node1);
			expect(ring.getNode("test")).toEqual(node2);
		});
	});

	describe("getNode", () => {
		it("should return undefined for empty ring", () => {
			const ring = new HashRing<string>();
			expect(ring.getNode("test")).toBe(undefined);
		});

		it("should return the same node for same input", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const node1 = ring.getNode("test-key");
			const node2 = ring.getNode("test-key");
			expect(node1).toBe(node2);
		});

		it("should distribute keys across nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const nodes = new Set<string>();

			for (let i = 0; i < 100; i++) {
				const node = ring.getNode(`key-${i}`);
				if (node) nodes.add(node);
			}

			// Should use all 3 nodes with 100 keys
			expect(nodes.size).toBe(3);
		});

		it("should accept Buffer input", () => {
			const ring = new HashRing(["node1", "node2"]);
			const buffer = Buffer.from("test-key");
			const node = ring.getNode(buffer);
			expect(node).toBeDefined();
		});

		it("should return same node for string and buffer with same content", () => {
			const ring = new HashRing(["node1", "node2"]);
			const stringNode = ring.getNode("test-key");
			const bufferNode = ring.getNode(Buffer.from("test-key"));
			expect(stringNode).toBe(bufferNode);
		});
	});

	describe("getNodes", () => {
		it("should return multiple replica nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3", "node4"]);
			const nodes = ring.getNodes("test-key", 3);
			expect(nodes).toHaveLength(3);
		});

		it("should return unique nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const nodes = ring.getNodes("test-key", 3);
			const uniqueNodes = new Set(nodes);
			expect(uniqueNodes.size).toBe(3);
		});

		it("should return all nodes when replicas >= node count", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const nodes = ring.getNodes("test-key", 5);
			expect(nodes).toHaveLength(3);
		});

		it("should return all nodes when replicas equal node count", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const nodes = ring.getNodes("test-key", 3);
			expect(nodes).toHaveLength(3);
		});

		it("should return consistent replicas for same key", () => {
			const ring = new HashRing(["node1", "node2", "node3", "node4"]);
			const nodes1 = ring.getNodes("test-key", 2);
			const nodes2 = ring.getNodes("test-key", 2);
			expect(nodes1).toEqual(nodes2);
		});

		it("should handle single node ring", () => {
			const ring = new HashRing(["node1"]);
			const nodes = ring.getNodes("test-key", 3);
			expect(nodes).toEqual(["node1"]);
		});

		it("should return empty array for empty ring", () => {
			const ring = new HashRing<string>();
			const nodes = ring.getNodes("test-key", 3);
			expect(nodes).toEqual([]);
		});
	});

	describe("Distribution", () => {
		it("should distribute keys relatively evenly across nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const distribution = new Map<string, number>();

			for (let i = 0; i < 1000; i++) {
				const node = ring.getNode(`key-${i}`);
				if (node) {
					distribution.set(node, (distribution.get(node) || 0) + 1);
				}
			}

			expect(distribution.size).toBe(3);

			// Each node should get roughly 333 keys (allow for some variance)
			for (const [_node, count] of distribution) {
				expect(count).toBeGreaterThan(250);
				expect(count).toBeLessThan(450);
			}
		});

		it("should respect node weights in distribution", () => {
			const ring = new HashRing<string>();
			ring.addNode("heavy", 3);
			ring.addNode("light", 1);

			const distribution = new Map<string, number>();

			for (let i = 0; i < 1000; i++) {
				const node = ring.getNode(`key-${i}`);
				if (node) {
					distribution.set(node, (distribution.get(node) || 0) + 1);
				}
			}

			const heavy = distribution.get("heavy") || 0;
			const light = distribution.get("light") || 0;

			// Heavy node should get roughly 3x more keys than light node
			expect(heavy).toBeGreaterThan(light * 2);
			expect(heavy).toBeLessThan(light * 4);
		});
	});

	describe("Consistent Hashing", () => {
		it("should minimize key redistribution when adding nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const assignments = new Map<string, string>();

			// Record initial assignments
			for (let i = 0; i < 100; i++) {
				const key = `key-${i}`;
				const node = ring.getNode(key);
				if (node) assignments.set(key, node);
			}

			// Add a new node
			ring.addNode("node4");

			// Count how many keys moved
			let movedKeys = 0;
			for (const [key, originalNode] of assignments) {
				const newNode = ring.getNode(key);
				if (newNode !== originalNode) {
					movedKeys++;
				}
			}

			// Should move roughly 25% of keys (1/4 of keys to new node)
			expect(movedKeys).toBeGreaterThan(10);
			expect(movedKeys).toBeLessThan(40);
		});

		it("should minimize key redistribution when removing nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3", "node4"]);
			const assignments = new Map<string, string>();

			// Record initial assignments
			for (let i = 0; i < 100; i++) {
				const key = `key-${i}`;
				const node = ring.getNode(key);
				if (node) assignments.set(key, node);
			}

			// Remove a node
			ring.removeNode("node4");

			// Count how many keys moved that weren't on node4
			let movedKeys = 0;
			for (const [key, originalNode] of assignments) {
				if (originalNode !== "node4") {
					const newNode = ring.getNode(key);
					if (newNode !== originalNode) {
						movedKeys++;
					}
				}
			}

			// Only keys that were on node4 should move
			// Keys on other nodes should stay put
			expect(movedKeys).toBe(0);
		});
	});

	describe("baseWeight", () => {
		it("should allow changing base weight", () => {
			const originalWeight = HashRing.baseWeight;
			HashRing.baseWeight = 100;

			const ring = new HashRing(["node1", "node2"]);
			expect(ring.getNode("test")).toBeDefined();

			// Restore original value
			HashRing.baseWeight = originalWeight;
		});

		it("should affect number of virtual nodes in the ring", () => {
			const originalWeight = HashRing.baseWeight;

			// Test with low base weight
			HashRing.baseWeight = 10;
			const ring1 = new HashRing(["node1"]);
			const node1 = ring1.getNode("test");
			expect(node1).toBe("node1");

			// Test with high base weight
			HashRing.baseWeight = 100;
			const ring2 = new HashRing(["node1"]);
			const node2 = ring2.getNode("test");
			expect(node2).toBe("node1");

			// Restore original value
			HashRing.baseWeight = originalWeight;
		});
	});

	describe("getters", () => {
		it("should expose clock getter", () => {
			const ring = new HashRing(["node1", "node2"]);
			const clock = ring.clock;
			expect(Array.isArray(clock)).toBe(true);
			expect(clock.length).toBeGreaterThan(0);
			// Each entry should be [hash, nodeKey]
			expect(clock[0]).toHaveLength(2);
			expect(typeof clock[0][0]).toBe("number");
			expect(typeof clock[0][1]).toBe("string");
		});

		it("should expose nodes getter", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const nodes = ring.nodes;
			expect(nodes instanceof Map).toBe(true);
			expect(nodes.size).toBe(3);
			expect(nodes.has("node1")).toBe(true);
			expect(nodes.has("node2")).toBe(true);
			expect(nodes.has("node3")).toBe(true);
		});

		it("should return empty clock for empty ring", () => {
			const ring = new HashRing<string>();
			expect(ring.clock).toEqual([]);
		});

		it("should return empty nodes map for empty ring", () => {
			const ring = new HashRing<string>();
			expect(ring.nodes.size).toBe(0);
		});

		it("should return sorted clock entries", () => {
			const ring = new HashRing(["node1", "node2"]);
			const clock = ring.clock;
			// Verify clock is sorted by hash value
			for (let i = 1; i < clock.length; i++) {
				expect(clock[i][0]).toBeGreaterThanOrEqual(clock[i - 1][0]);
			}
		});

		it("should return nodes map with object nodes", () => {
			const node1 = { key: "server1", host: "localhost", port: 11211 };
			const node2 = { key: "server2", host: "localhost", port: 11212 };
			const ring = new HashRing([node1, node2]);
			const nodes = ring.nodes;
			expect(nodes.size).toBe(2);
			expect(nodes.get("server1")).toEqual(node1);
			expect(nodes.get("server2")).toEqual(node2);
		});
	});
});

describe("getNodeIndexForKey", () => {
	it("should return a valid node index", () => {
		const nodes = [
			{ node: new MemcacheNode("server1", 11211), weight: 1 },
			{ node: new MemcacheNode("server2", 11211), weight: 1 },
			{ node: new MemcacheNode("server3", 11211), weight: 1 },
			{ node: new MemcacheNode("server4", 11211), weight: 1 },
		];
		const index = getNodeIndexForKey("test-key", nodes);
		expect(index).toBeGreaterThanOrEqual(0);
		expect(index).toBeLessThan(nodes.length);
	});

	it("should return consistent index for same key", () => {
		const nodes = [
			{ node: new MemcacheNode("server1", 11211), weight: 1 },
			{ node: new MemcacheNode("server2", 11211), weight: 1 },
			{ node: new MemcacheNode("server3", 11211), weight: 1 },
			{ node: new MemcacheNode("server4", 11211), weight: 1 },
		];
		const index1 = getNodeIndexForKey("test-key", nodes);
		const index2 = getNodeIndexForKey("test-key", nodes);
		expect(index1).toBe(index2);
	});

	it("should distribute keys across all nodes", () => {
		const nodes = [
			{ node: new MemcacheNode("server1", 11211), weight: 1 },
			{ node: new MemcacheNode("server2", 11211), weight: 1 },
			{ node: new MemcacheNode("server3", 11211), weight: 1 },
			{ node: new MemcacheNode("server4", 11211), weight: 1 },
		];
		const distribution = new Map<number, number>();

		for (let i = 0; i < 1000; i++) {
			const index = getNodeIndexForKey(`key-${i}`, nodes);
			distribution.set(index, (distribution.get(index) || 0) + 1);
		}

		// Should use all nodes
		expect(distribution.size).toBe(nodes.length);

		// Each node should get roughly 250 keys (allow for some variance)
		for (const [_index, count] of distribution) {
			expect(count).toBeGreaterThan(150);
			expect(count).toBeLessThan(350);
		}
	});

	it("should respect node weights in distribution", () => {
		const nodes = [
			{ node: new MemcacheNode("heavy", 11211), weight: 3 },
			{ node: new MemcacheNode("light", 11211), weight: 1 },
		];
		const distribution = new Map<number, number>();

		for (let i = 0; i < 1000; i++) {
			const index = getNodeIndexForKey(`key-${i}`, nodes);
			distribution.set(index, (distribution.get(index) || 0) + 1);
		}

		const heavy = distribution.get(0) || 0;
		const light = distribution.get(1) || 0;

		// Heavy node should get roughly 3x more keys than light node
		expect(heavy).toBeGreaterThan(light * 2);
		expect(heavy).toBeLessThan(light * 4);
	});

	it("should accept Buffer input", () => {
		const nodes = [
			{ node: new MemcacheNode("server1", 11211), weight: 1 },
			{ node: new MemcacheNode("server2", 11211), weight: 1 },
		];
		const buffer = Buffer.from("test-key");
		const index = getNodeIndexForKey(buffer, nodes);
		expect(index).toBeGreaterThanOrEqual(0);
		expect(index).toBeLessThan(nodes.length);
	});

	it("should return same index for string and buffer with same content", () => {
		const nodes = [
			{ node: new MemcacheNode("server1", 11211), weight: 1 },
			{ node: new MemcacheNode("server2", 11211), weight: 1 },
			{ node: new MemcacheNode("server3", 11211), weight: 1 },
		];
		const stringIndex = getNodeIndexForKey("test-key", nodes);
		const bufferIndex = getNodeIndexForKey(Buffer.from("test-key"), nodes);
		expect(stringIndex).toBe(bufferIndex);
	});

	it("should work with single node", () => {
		const nodes = [{ node: new MemcacheNode("server1", 11211), weight: 1 }];
		const index = getNodeIndexForKey("test-key", nodes);
		expect(index).toBe(0);
	});

	it("should work with many nodes", () => {
		const nodes = Array.from({ length: 100 }, (_, i) => ({
			node: new MemcacheNode(`server${i}`, 11211),
			weight: 1,
		}));
		const index = getNodeIndexForKey("test-key", nodes);
		expect(index).toBeGreaterThanOrEqual(0);
		expect(index).toBeLessThan(nodes.length);
	});

	it("should throw error for empty nodes array", () => {
		expect(() => getNodeIndexForKey("test-key", [])).toThrow(
			"nodes array must not be empty",
		);
	});

	it("should use consistent hashing (same as HashRing)", () => {
		// Verify that getNodeIndexForKey uses the same consistent hashing
		// algorithm as HashRing by comparing results
		const nodes = [
			{ node: new MemcacheNode("server1", 11211), weight: 1 },
			{ node: new MemcacheNode("server2", 11211), weight: 1 },
			{ node: new MemcacheNode("server3", 11211), weight: 1 },
			{ node: new MemcacheNode("server4", 11211), weight: 1 },
			{ node: new MemcacheNode("server5", 11211), weight: 1 },
		];

		// Create a HashRing with the same node ids
		const nodeIds = nodes.map((n) => n.node.id);
		const ring = new HashRing(nodeIds);

		for (let i = 0; i < 100; i++) {
			const key = `key-${i}`;
			const index = getNodeIndexForKey(key, nodes);
			const nodeId = ring.getNode(key);

			// The node id from HashRing should match the node at the returned index
			expect(nodes[index].node.id).toBe(nodeId);
		}
	});

	it("should distribute different keys differently", () => {
		const nodes = [
			{ node: new MemcacheNode("server1", 11211), weight: 1 },
			{ node: new MemcacheNode("server2", 11211), weight: 1 },
			{ node: new MemcacheNode("server3", 11211), weight: 1 },
			{ node: new MemcacheNode("server4", 11211), weight: 1 },
		];
		const indices = new Set<number>();

		// Generate 20 different keys and collect their indices
		for (let i = 0; i < 20; i++) {
			const index = getNodeIndexForKey(`key-${i}`, nodes);
			indices.add(index);
		}

		// Should have multiple different indices (not all keys going to same node)
		expect(indices.size).toBeGreaterThan(1);
	});

	it("should work with nodes on different ports", () => {
		const nodes = [
			{ node: new MemcacheNode("localhost", 11211), weight: 1 },
			{ node: new MemcacheNode("localhost", 11212), weight: 1 },
			{ node: new MemcacheNode("localhost", 11213), weight: 1 },
		];

		const index = getNodeIndexForKey("test-key", nodes);
		expect(index).toBeGreaterThanOrEqual(0);
		expect(index).toBeLessThan(nodes.length);

		// Should be consistent
		const index2 = getNodeIndexForKey("test-key", nodes);
		expect(index).toBe(index2);
	});

	it("should handle mixed weights", () => {
		const nodes = [
			{ node: new MemcacheNode("server1", 11211), weight: 1 },
			{ node: new MemcacheNode("server2", 11211), weight: 2 },
			{ node: new MemcacheNode("server3", 11211), weight: 3 },
		];
		const distribution = new Map<number, number>();

		for (let i = 0; i < 1000; i++) {
			const index = getNodeIndexForKey(`key-${i}`, nodes);
			distribution.set(index, (distribution.get(index) || 0) + 1);
		}

		// All nodes should be used
		expect(distribution.size).toBe(3);

		// Higher weight nodes should get more keys
		const node1 = distribution.get(0) || 0;
		const node2 = distribution.get(1) || 0;
		const node3 = distribution.get(2) || 0;

		// Node 3 (weight 3) should get more than node 1 (weight 1)
		expect(node3).toBeGreaterThan(node1);
		// Node 2 (weight 2) should get more than node 1 (weight 1)
		expect(node2).toBeGreaterThan(node1);
	});
});
