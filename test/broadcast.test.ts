import { describe, expect, it } from "vitest";
import { BroadcastHash } from "../src/broadcast.js";
import { MemcacheNode } from "../src/node.js";
import { generateKey } from "./test-utils.js";

describe("BroadcastHash", () => {
	describe("constructor", () => {
		it("should create instance with correct name", () => {
			const distribution = new BroadcastHash();
			expect(distribution.name).toBe("broadcast");
			expect(distribution.nodes).toEqual([]);
		});
	});

	describe("nodes getter", () => {
		it("should return empty array when no nodes added", () => {
			const distribution = new BroadcastHash();
			expect(distribution.nodes).toEqual([]);
		});

		it("should return all added nodes", () => {
			const distribution = new BroadcastHash();
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
			const distribution = new BroadcastHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			expect(distribution.nodes.length).toBe(1);
			expect(distribution.nodes[0]).toBe(node);
		});

		it("should handle multiple nodes", () => {
			const distribution = new BroadcastHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);

			expect(distribution.nodes.length).toBe(2);
		});
	});

	describe("removeNode", () => {
		it("should remove node by ID", () => {
			const distribution = new BroadcastHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			expect(distribution.nodes.length).toBe(1);

			distribution.removeNode(node.id);
			expect(distribution.nodes.length).toBe(0);
		});

		it("should be no-op when removing non-existent node", () => {
			const distribution = new BroadcastHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			distribution.removeNode("nonexistent:11211");
			expect(distribution.nodes.length).toBe(1);
		});
	});

	describe("getNode", () => {
		it("should get node by ID", () => {
			const distribution = new BroadcastHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);
			const retrieved = distribution.getNode("localhost:11211");

			expect(retrieved).toBe(node);
		});

		it("should return undefined for non-existent node", () => {
			const distribution = new BroadcastHash();
			const retrieved = distribution.getNode("nonexistent:11211");

			expect(retrieved).toBeUndefined();
		});

		it("should distinguish between different node IDs", () => {
			const distribution = new BroadcastHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);

			expect(distribution.getNode("server1:11211")).toBe(node1);
			expect(distribution.getNode("server2:11211")).toBe(node2);
		});
	});

	describe("getNodesByKey", () => {
		it("should return all nodes for any key", () => {
			const distribution = new BroadcastHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);
			const node3 = new MemcacheNode("server3", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);
			distribution.addNode(node3);

			const key = generateKey("broadcast");
			const nodes = distribution.getNodesByKey(key);
			expect(nodes.length).toBe(3);
			expect(nodes).toContain(node1);
			expect(nodes).toContain(node2);
			expect(nodes).toContain(node3);
		});

		it("should return empty array when no nodes available", () => {
			const distribution = new BroadcastHash();
			const key = generateKey("empty");
			const nodes = distribution.getNodesByKey(key);
			expect(nodes).toEqual([]);
		});

		it("should return same nodes regardless of key", () => {
			const distribution = new BroadcastHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			distribution.addNode(node1);
			distribution.addNode(node2);

			const nodes1 = distribution.getNodesByKey("key-a");
			const nodes2 = distribution.getNodesByKey("key-b");
			const nodes3 = distribution.getNodesByKey("completely-different");

			expect(nodes1).toEqual(nodes2);
			expect(nodes2).toEqual(nodes3);
		});

		it("should work with single node", () => {
			const distribution = new BroadcastHash();
			const node = new MemcacheNode("localhost", 11211);

			distribution.addNode(node);

			const nodes = distribution.getNodesByKey("any-key");
			expect(nodes.length).toBe(1);
			expect(nodes[0]).toBe(node);
		});

		it("should reflect node additions and removals", () => {
			const distribution = new BroadcastHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			distribution.addNode(node1);
			expect(distribution.getNodesByKey("key").length).toBe(1);

			distribution.addNode(node2);
			expect(distribution.getNodesByKey("key").length).toBe(2);

			distribution.removeNode(node1.id);
			expect(distribution.getNodesByKey("key").length).toBe(1);
			expect(distribution.getNodesByKey("key")[0]).toBe(node2);
		});
	});

	describe("integration", () => {
		it("should handle add, get, and remove operations", () => {
			const distribution = new BroadcastHash();
			const node1 = new MemcacheNode("server1", 11211);
			const node2 = new MemcacheNode("server2", 11211);

			// Add nodes
			distribution.addNode(node1);
			distribution.addNode(node2);
			expect(distribution.nodes.length).toBe(2);

			// Get by key returns all nodes
			const key = generateKey("integration");
			const nodeForKey = distribution.getNodesByKey(key);
			expect(nodeForKey.length).toBe(2);

			// Get by ID
			const retrievedNode = distribution.getNode("server1:11211");
			expect(retrievedNode).toBe(node1);

			// Remove node
			distribution.removeNode("server1:11211");
			expect(distribution.nodes.length).toBe(1);
			expect(distribution.getNode("server1:11211")).toBeUndefined();

			// After removal, getNodesByKey returns remaining nodes
			const remaining = distribution.getNodesByKey(key);
			expect(remaining.length).toBe(1);
			expect(remaining[0]).toBe(node2);
		});
	});
});
