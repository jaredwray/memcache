import { describe, expect, it } from "vitest";
import { ModulaHash } from "../src/modula.js";
import { MemcacheNode } from "../src/node.js";
import { generateKey } from "./test-utils.js";

describe("ModulaHash", () => {
	describe("constructor", () => {
		it("should create instance with default hash function", () => {
			const distribution = new ModulaHash();
			expect(distribution.name).toBe("modula");
			expect(distribution.nodes).toEqual([]);
		});

		it("should create instance with custom hash algorithm", () => {
			const distribution = new ModulaHash("md5");
			expect(distribution.name).toBe("modula");
		});

		it("should create instance with custom hash function", () => {
			const customHash = (buf: Buffer) => buf.readUInt32BE(0);
			const distribution = new ModulaHash(customHash);
			expect(distribution.name).toBe("modula");
		});
	});

	describe("nodes getter", () => {
		it("should return empty array when no nodes added", () => {
			const distribution = new ModulaHash();
			expect(distribution.nodes).toEqual([]);
		});

		it("should return all added nodes", () => {
			const distribution = new ModulaHash();
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
			const distribution = new ModulaHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			expect(distribution.nodes.length).toBe(1);
			expect(distribution.nodes[0]).toBe(node);
		});

		it("should add node with custom weight", () => {
			const distribution = new ModulaHash();
			const node = new MemcacheNode("localhost", 11211, { weight: 3 });

			distribution.addNode(node);
			expect(distribution.nodes.length).toBe(1);
		});

		it("should handle multiple nodes", () => {
			const distribution = new ModulaHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);

			expect(distribution.nodes.length).toBe(2);
		});
	});

	describe("removeNode", () => {
		it("should remove node by ID", () => {
			const distribution = new ModulaHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			expect(distribution.nodes.length).toBe(1);

			distribution.removeNode(node.id);
			expect(distribution.nodes.length).toBe(0);
		});

		it("should be no-op when removing non-existent node", () => {
			const distribution = new ModulaHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			distribution.removeNode("nonexistent:11211");
			expect(distribution.nodes.length).toBe(1);
		});

		it("should remove all weighted entries for a node", () => {
			const distribution = new ModulaHash();
			const node = new MemcacheNode("localhost", 11211, { weight: 3 });

			distribution.addNode(node);
			distribution.removeNode(node.id);

			// After removal, getNodesByKey should return empty
			const key = generateKey("removed");
			const nodes = distribution.getNodesByKey(key);
			expect(nodes).toEqual([]);
		});
	});

	describe("getNode", () => {
		it("should get node by ID", () => {
			const distribution = new ModulaHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			const retrieved = distribution.getNode("localhost:11211");

			expect(retrieved).toBe(node);
		});

		it("should return undefined for non-existent node", () => {
			const distribution = new ModulaHash();
			const retrieved = distribution.getNode("nonexistent:11211");

			expect(retrieved).toBeUndefined();
		});

		it("should distinguish between different node IDs", () => {
			const distribution = new ModulaHash();
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
			const distribution = new ModulaHash();
			const node1 = new MemcacheNode("localhost", 11211);
			const node2 = new MemcacheNode("localhost", 11212);

			distribution.addNode(node1);
			distribution.addNode(node2);

			const key = generateKey("modula");
			const nodes = distribution.getNodesByKey(key);
			expect(nodes.length).toBe(1);
			expect([node1, node2]).toContain(nodes[0]);
		});

		it("should return consistent node for same key", () => {
			const distribution = new ModulaHash();
			const node1 = new MemcacheNode("localhost", 11211);
			const node2 = new MemcacheNode("localhost", 11212);

			distribution.addNode(node1);
			distribution.addNode(node2);

			const key = generateKey("consistent");
			const nodes1 = distribution.getNodesByKey(key);
			const nodes2 = distribution.getNodesByKey(key);
			expect(nodes1[0]).toBe(nodes2[0]);
		});

		it("should return empty array when no nodes available", () => {
			const distribution = new ModulaHash();
			const key = generateKey("empty");
			const nodes = distribution.getNodesByKey(key);
			expect(nodes).toEqual([]);
		});

		it("should distribute keys across nodes", () => {
			const distribution = new ModulaHash();
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
			const distribution = new ModulaHash();
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

			// Heavy node should handle more keys than light node (roughly 3x)
			expect(heavyCount).toBeGreaterThan(lightCount);
		});

		it("should work with single node", () => {
			const distribution = new ModulaHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);

			// All keys should go to the single node
			for (let i = 0; i < 10; i++) {
				const nodes = distribution.getNodesByKey(`key-${i}`);
				expect(nodes.length).toBe(1);
				expect(nodes[0]).toBe(node);
			}
		});
	});

	describe("integration", () => {
		it("should handle add, get, and remove operations", () => {
			const distribution = new ModulaHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			// Add nodes
			distribution.addNode(node1);
			distribution.addNode(node2);
			expect(distribution.nodes.length).toBe(2);

			// Get by key
			const key = generateKey("integration");
			const nodeForKey = distribution.getNodesByKey(key);
			expect(nodeForKey.length).toBe(1);

			// Get by ID
			const retrievedNode = distribution.getNode("server1:11211");
			expect(retrievedNode).toBe(node1);

			// Remove node
			distribution.removeNode("server1:11211");
			expect(distribution.nodes.length).toBe(1);
			expect(distribution.getNode("server1:11211")).toBeUndefined();
		});

		it("should redistribute all keys when nodes change (unlike ketama)", () => {
			const distribution = new ModulaHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);

			// Record which keys go to which node
			const keyAssignments = new Map<string, string>();
			for (let i = 0; i < 100; i++) {
				const key = `key-${i}`;
				const nodes = distribution.getNodesByKey(key);
				keyAssignments.set(key, nodes[0].id);
			}

			// Add a third node - this will change the modulo base
			const node3 = new MemcacheNode("server3", 11211);
			distribution.addNode(node3);

			// Count how many keys changed nodes
			let changedCount = 0;
			for (let i = 0; i < 100; i++) {
				const key = `key-${i}`;
				const nodes = distribution.getNodesByKey(key);
				if (nodes[0].id !== keyAssignments.get(key)) {
					changedCount++;
				}
			}

			// With modulo hashing, many keys will move (not minimal like ketama)
			// This is expected behavior - verifying modulo characteristics
			expect(changedCount).toBeGreaterThan(0);
		});

		it("should work with different hash algorithms", () => {
			const sha1Distribution = new ModulaHash("sha1");
			const md5Distribution = new ModulaHash("md5");

			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			sha1Distribution.addNode(node1);
			sha1Distribution.addNode(node2);

			md5Distribution.addNode(node1);
			md5Distribution.addNode(node2);

			// Both should work and distribute keys
			const key = generateKey("hash-algo");
			const sha1Nodes = sha1Distribution.getNodesByKey(key);
			const md5Nodes = md5Distribution.getNodesByKey(key);

			expect(sha1Nodes.length).toBe(1);
			expect(md5Nodes.length).toBe(1);
		});
	});
});
