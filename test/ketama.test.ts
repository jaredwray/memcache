import { describe, expect, it } from "vitest";
import { HashRing, KetamaDistributionHash } from "../src/ketama.js";
import { MemcacheNode } from "../src/node.js";

describe("HashRing", () => {
	describe("constructor", () => {
		it("should create empty ring with no initial nodes", () => {
			const ring = new HashRing();
			expect(ring.nodes.size).toBe(0);
			expect(ring.clock.length).toBe(0);
		});

		it("should create ring with simple string nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			expect(ring.nodes.size).toBe(3);
			expect(ring.clock.length).toBeGreaterThan(0);
		});

		it("should create ring with weighted nodes", () => {
			const ring = new HashRing([
				{ node: "heavy", weight: 3 },
				{ node: "light", weight: 1 },
			]);
			expect(ring.nodes.size).toBe(2);
			// Heavy node should have more virtual nodes
			const heavyVirtualNodes = ring.clock.filter(([, n]) => n === "heavy");
			const lightVirtualNodes = ring.clock.filter(([, n]) => n === "light");
			expect(heavyVirtualNodes.length).toBeGreaterThan(
				lightVirtualNodes.length,
			);
		});

		it("should create ring with mixed weighted and unweighted nodes", () => {
			const ring = new HashRing(["simple", { node: "weighted", weight: 2 }]);
			expect(ring.nodes.size).toBe(2);
		});

		it("should create ring with custom hash function (md5)", () => {
			const ring = new HashRing(["node1"], "md5");
			expect(ring.clock.length).toBeGreaterThan(0);
		});

		it("should create ring with custom hash function", () => {
			const customHash = (buf: Buffer) => {
				let hash = 0;
				for (let i = 0; i < buf.length; i++) {
					hash = (hash << 5) - hash + buf[i];
					hash |= 0; // Convert to 32bit integer
				}
				return hash;
			};
			const ring = new HashRing(["node1"], customHash);
			expect(ring.clock.length).toBeGreaterThan(0);
		});

		it("should handle object nodes with key property", () => {
			const ring = new HashRing<{ key: string; port: number }>([
				{ key: "server1", port: 11211 },
				{ key: "server2", port: 11212 },
			]);
			expect(ring.nodes.size).toBe(2);
		});
	});

	describe("getters", () => {
		it("should return clock via getter", () => {
			const ring = new HashRing(["node1"]);
			const clock = ring.clock;
			expect(Array.isArray(clock)).toBe(true);
			expect(clock.length).toBeGreaterThan(0);
			expect(clock[0]).toHaveLength(2); // [hash, node]
		});

		it("should return nodes via getter", () => {
			const ring = new HashRing(["node1", "node2"]);
			const nodes = ring.nodes;
			expect(nodes instanceof Map).toBe(true);
			expect(nodes.size).toBe(2);
			expect(nodes.get("node1")).toBe("node1");
		});

		it("should return readonly nodes map", () => {
			const ring = new HashRing(["node1"]);
			const nodes = ring.nodes;
			expect(nodes).toBeInstanceOf(Map);
		});
	});

	describe("addNode", () => {
		it("should add a node with default weight", () => {
			const ring = new HashRing();
			ring.addNode("node1");
			expect(ring.nodes.size).toBe(1);
			expect(ring.clock.length).toBe(HashRing.baseWeight);
		});

		it("should add a node with custom weight", () => {
			const ring = new HashRing();
			ring.addNode("node1", 2);
			expect(ring.nodes.size).toBe(1);
			expect(ring.clock.length).toBe(HashRing.baseWeight * 2);
		});

		it("should update existing node weight", () => {
			const ring = new HashRing();
			ring.addNode("node1", 1);
			const initialClockLength = ring.clock.length;
			ring.addNode("node1", 2);
			expect(ring.nodes.size).toBe(1);
			expect(ring.clock.length).toBe(initialClockLength * 2);
		});

		it("should remove node when weight is 0", () => {
			const ring = new HashRing(["node1"]);
			expect(ring.nodes.size).toBe(1);
			ring.addNode("node1", 0);
			expect(ring.nodes.size).toBe(0);
			expect(ring.clock.length).toBe(0);
		});

		it("should throw error for negative weight", () => {
			const ring = new HashRing();
			expect(() => ring.addNode("node1", -1)).toThrow(RangeError);
			expect(() => ring.addNode("node1", -1)).toThrow(
				"Cannot add a node to the hashring with weight < 0",
			);
		});
	});

	describe("removeNode", () => {
		it("should remove existing node", () => {
			const ring = new HashRing(["node1", "node2"]);
			expect(ring.nodes.size).toBe(2);
			ring.removeNode("node1");
			expect(ring.nodes.size).toBe(1);
			expect(ring.nodes.has("node1")).toBe(false);
		});

		it("should be no-op when removing non-existent node", () => {
			const ring = new HashRing(["node1"]);
			ring.removeNode("nonexistent");
			expect(ring.nodes.size).toBe(1);
		});

		it("should remove all virtual nodes from clock", () => {
			const ring = new HashRing(["node1"]);
			const initialClockLength = ring.clock.length;
			expect(initialClockLength).toBeGreaterThan(0);
			ring.removeNode("node1");
			expect(ring.clock.length).toBe(0);
		});
	});

	describe("getNode", () => {
		it("should return node for a given key", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const node = ring.getNode("test-key");
			expect(node).toBeDefined();
			expect(["node1", "node2", "node3"]).toContain(node);
		});

		it("should return consistent node for same key", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const node1 = ring.getNode("test-key");
			const node2 = ring.getNode("test-key");
			expect(node1).toBe(node2);
		});

		it("should return undefined for empty ring", () => {
			const ring = new HashRing();
			const node = ring.getNode("test-key");
			expect(node).toBeUndefined();
		});

		it("should accept Buffer input", () => {
			const ring = new HashRing(["node1", "node2"]);
			const node = ring.getNode(Buffer.from("test-key"));
			expect(node).toBeDefined();
		});

		it("should return same node for string and Buffer with same content", () => {
			const ring = new HashRing(["node1", "node2"]);
			const nodeFromString = ring.getNode("test-key");
			const nodeFromBuffer = ring.getNode(Buffer.from("test-key"));
			expect(nodeFromString).toBe(nodeFromBuffer);
		});

		it("should distribute keys across nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const distribution = new Map<string, number>();

			// Test with many keys to ensure distribution
			for (let i = 0; i < 300; i++) {
				const node = ring.getNode(`key-${i}`);
				if (node) {
					distribution.set(node, (distribution.get(node) || 0) + 1);
				}
			}

			// All nodes should get some keys
			expect(distribution.size).toBe(3);
			// Each node should get roughly 1/3 of keys (with some variance)
			for (const count of distribution.values()) {
				expect(count).toBeGreaterThan(50); // At least some keys
				expect(count).toBeLessThan(200); // Not too many keys
			}
		});
	});

	describe("getNodes (replicas)", () => {
		it("should return empty array for empty ring", () => {
			const ring = new HashRing();
			const nodes = ring.getNodes("test-key", 3);
			expect(nodes).toEqual([]);
		});

		it("should return all nodes when replicas >= node count", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const nodes = ring.getNodes("test-key", 5);
			expect(nodes.length).toBe(3);
			expect(nodes).toContain("node1");
			expect(nodes).toContain("node2");
			expect(nodes).toContain("node3");
		});

		it("should return all nodes when replicas equals node count", () => {
			const ring = new HashRing(["node1", "node2", "node3"]);
			const nodes = ring.getNodes("test-key", 3);
			expect(nodes.length).toBe(3);
		});

		it("should return requested number of unique nodes", () => {
			const ring = new HashRing(["node1", "node2", "node3", "node4"]);
			const nodes = ring.getNodes("test-key", 2);
			expect(nodes.length).toBe(2);
			expect(new Set(nodes).size).toBe(2); // All unique
		});

		it("should return consistent replicas for same key", () => {
			const ring = new HashRing(["node1", "node2", "node3", "node4"]);
			const nodes1 = ring.getNodes("test-key", 3);
			const nodes2 = ring.getNodes("test-key", 3);
			expect(nodes1).toEqual(nodes2);
		});

		it("should return nodes in ring order", () => {
			const ring = new HashRing(["node1", "node2", "node3", "node4"]);
			const nodes = ring.getNodes("test-key", 3);
			expect(nodes.length).toBe(3);
			// All nodes should be unique
			const uniqueNodes = new Set(nodes);
			expect(uniqueNodes.size).toBe(3);
		});

		it("should handle single node ring", () => {
			const ring = new HashRing(["node1"]);
			const nodes = ring.getNodes("test-key", 3);
			expect(nodes).toEqual(["node1"]);
		});
	});
});

describe("KetamaDistributionHash", () => {
	describe("constructor", () => {
		it("should create instance with default hash function", () => {
			const distribution = new KetamaDistributionHash();
			expect(distribution.name).toBe("ketama");
			expect(distribution.nodes).toEqual([]);
		});

		it("should create instance with custom hash algorithm", () => {
			const distribution = new KetamaDistributionHash("md5");
			expect(distribution.name).toBe("ketama");
		});

		it("should create instance with custom hash function", () => {
			const customHash = (buf: Buffer) => buf.readInt32BE();
			const distribution = new KetamaDistributionHash(customHash);
			expect(distribution.name).toBe("ketama");
		});
	});

	describe("nodes getter", () => {
		it("should return empty array when no nodes added", () => {
			const distribution = new KetamaDistributionHash();
			expect(distribution.nodes).toEqual([]);
		});

		it("should return all added nodes", () => {
			const distribution = new KetamaDistributionHash();
			const node1 = new MemcacheNode("localhost", 11211);
			const node2 = new MemcacheNode("localhost", 11212);

			distribution.addNode(node1);
			distribution.addNode(node2);

			const nodes = distribution.nodes;
			expect(nodes.length).toBe(2);
			expect(nodes).toContain(node1);
			expect(nodes).toContain(node2);
		});
	});

	describe("addNode", () => {
		it("should add node to distribution", () => {
			const distribution = new KetamaDistributionHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			expect(distribution.nodes.length).toBe(1);
			expect(distribution.nodes[0]).toBe(node);
		});

		it("should add node with custom weight", () => {
			const distribution = new KetamaDistributionHash();
			const node = new MemcacheNode("localhost", 11211, { weight: 3 });

			distribution.addNode(node);
			expect(distribution.nodes.length).toBe(1);
		});

		it("should handle multiple nodes", () => {
			const distribution = new KetamaDistributionHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);

			expect(distribution.nodes.length).toBe(2);
		});
	});

	describe("removeNode", () => {
		it("should remove node by ID", () => {
			const distribution = new KetamaDistributionHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			expect(distribution.nodes.length).toBe(1);

			distribution.removeNode(node.id);
			expect(distribution.nodes.length).toBe(0);
		});

		it("should be no-op when removing non-existent node", () => {
			const distribution = new KetamaDistributionHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			distribution.removeNode("nonexistent:11211");
			expect(distribution.nodes.length).toBe(1);
		});
	});

	describe("getNode", () => {
		it("should get node by ID", () => {
			const distribution = new KetamaDistributionHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			const retrieved = distribution.getNode("localhost:11211");

			expect(retrieved).toBe(node);
		});

		it("should return undefined for non-existent node", () => {
			const distribution = new KetamaDistributionHash();
			const retrieved = distribution.getNode("nonexistent:11211");

			expect(retrieved).toBeUndefined();
		});

		it("should distinguish between different node IDs", () => {
			const distribution = new KetamaDistributionHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);

			expect(distribution.getNode("server1:11211")).toBe(node1);
			expect(distribution.getNode("server2:11211")).toBe(node2);
		});
	});

	describe("getNodesByKey", () => {
		it("should return node for given key", () => {
			const distribution = new KetamaDistributionHash();
			const node1 = new MemcacheNode("localhost", 11211);
			const node2 = new MemcacheNode("localhost", 11212);

			distribution.addNode(node1);
			distribution.addNode(node2);

			const nodes = distribution.getNodesByKey("test-key");
			expect(nodes.length).toBe(1);
			expect([node1, node2]).toContain(nodes[0]);
		});

		it("should return consistent node for same key", () => {
			const distribution = new KetamaDistributionHash();
			const node1 = new MemcacheNode("localhost", 11211);
			const node2 = new MemcacheNode("localhost", 11212);

			distribution.addNode(node1);
			distribution.addNode(node2);

			const nodes1 = distribution.getNodesByKey("test-key");
			const nodes2 = distribution.getNodesByKey("test-key");
			expect(nodes1[0]).toBe(nodes2[0]);
		});

		it("should return empty array when no nodes available", () => {
			const distribution = new KetamaDistributionHash();
			const nodes = distribution.getNodesByKey("test-key");
			expect(nodes).toEqual([]);
		});

		it("should distribute keys across nodes", () => {
			const distribution = new KetamaDistributionHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);
			const node3 = new MemcacheNode("server3", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);
			distribution.addNode(node3);

			const distributionMap = new Map<string, number>();

			// Test with many keys
			for (let i = 0; i < 300; i++) {
				const nodes = distribution.getNodesByKey(`key-${i}`);
				if (nodes.length > 0) {
					const nodeId = nodes[0].id;
					distributionMap.set(nodeId, (distributionMap.get(nodeId) || 0) + 1);
				}
			}

			// All nodes should receive some keys
			expect(distributionMap.size).toBe(3);
		});

		it("should handle weighted nodes", () => {
			const distribution = new KetamaDistributionHash();
			const heavyNode = new MemcacheNode("heavy", 11211, { weight: 3 });
			const lightNode = new MemcacheNode("light", 11211, { weight: 1 });

			distribution.addNode(heavyNode);
			distribution.addNode(lightNode);

			const distributionMap = new Map<string, number>();

			// Test with many keys
			for (let i = 0; i < 400; i++) {
				const nodes = distribution.getNodesByKey(`key-${i}`);
				if (nodes.length > 0) {
					const nodeId = nodes[0].id;
					distributionMap.set(nodeId, (distributionMap.get(nodeId) || 0) + 1);
				}
			}

			const heavyCount = distributionMap.get("heavy:11211") || 0;
			const lightCount = distributionMap.get("light:11211") || 0;

			// Heavy node should handle more keys than light node
			expect(heavyCount).toBeGreaterThan(lightCount);
		});
	});

	describe("integration", () => {
		it("should handle add, get, and remove operations", () => {
			const distribution = new KetamaDistributionHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			// Add nodes
			distribution.addNode(node1);
			distribution.addNode(node2);
			expect(distribution.nodes.length).toBe(2);

			// Get by key
			const nodeForKey = distribution.getNodesByKey("my-key");
			expect(nodeForKey.length).toBe(1);

			// Get by ID
			const retrievedNode = distribution.getNode("server1:11211");
			expect(retrievedNode).toBe(node1);

			// Remove node
			distribution.removeNode("server1:11211");
			expect(distribution.nodes.length).toBe(1);
			expect(distribution.getNode("server1:11211")).toBeUndefined();
		});

		it("should redistribute keys when nodes are removed", () => {
			const distribution = new KetamaDistributionHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);

			const keysOnNode1BeforeRemoval = [];
			for (let i = 0; i < 100; i++) {
				const nodes = distribution.getNodesByKey(`key-${i}`);
				if (nodes[0]?.id === "server1:11211") {
					keysOnNode1BeforeRemoval.push(`key-${i}`);
				}
			}

			// Remove node1
			distribution.removeNode("server1:11211");

			// All keys previously on node1 should now be on node2
			for (const key of keysOnNode1BeforeRemoval) {
				const nodes = distribution.getNodesByKey(key);
				expect(nodes[0]?.id).toBe("server2:11211");
			}
		});
	});
});
