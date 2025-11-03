import { describe, expect, it } from "vitest";
import Memcache, { createNode } from "../src/index";

describe("MemcacheNode Instances Support", () => {
	describe("addNode with MemcacheNode instances", () => {
		it("should accept MemcacheNode instances via addNode", async () => {
			const client = new Memcache({ timeout: 5000 });

			// Create a MemcacheNode instance using createNode
			const node1 = createNode("localhost", 11212, { weight: 2 });
			const node2 = createNode("localhost", 11213, { weight: 3 });

			// Add the node instances directly
			await client.addNode(node1);
			await client.addNode(node2);

			// Verify nodes were added
			expect(client.nodeIds).toHaveLength(3); // 2 + default
			expect(client.nodeIds).toContain("localhost:11212");
			expect(client.nodeIds).toContain("localhost:11213");

			// Verify the nodes retain their properties
			const addedNode1 = client.getNode("localhost:11212");
			expect(addedNode1).toBeDefined();
			expect(addedNode1?.weight).toBe(2);

			const addedNode2 = client.getNode("localhost:11213");
			expect(addedNode2).toBeDefined();
			expect(addedNode2?.weight).toBe(3);
		});

		it("should throw error when adding duplicate MemcacheNode instance", async () => {
			const client = new Memcache({ timeout: 5000 });

			// Create a node instance
			const node = createNode("localhost", 11212);

			// Add it once
			await client.addNode(node);
			expect(client.nodeIds).toContain("localhost:11212");

			// Try to add it again - should throw error
			await expect(client.addNode(node)).rejects.toThrow(
				"Node localhost:11212 already exists",
			);
		});
	});

	describe("Constructor with MemcacheNode instances", () => {
		it("should initialize with MemcacheNode instances in options", () => {
			const node1 = createNode("server1", 11211, { weight: 2 });
			const node2 = createNode("server2", 11211, { weight: 3 });

			const testClient = new Memcache({
				nodes: [node1, node2],
			});

			expect(testClient.nodeIds).toHaveLength(2);
			expect(testClient.nodeIds).toContain("server1:11211");
			expect(testClient.nodeIds).toContain("server2:11211");

			// Verify nodes retain their properties
			const addedNode1 = testClient.getNode("server1:11211");
			expect(addedNode1?.weight).toBe(2);

			const addedNode2 = testClient.getNode("server2:11211");
			expect(addedNode2?.weight).toBe(3);
		});

		it("should initialize with mixed string URIs and MemcacheNode instances", () => {
			const node1 = createNode("server1", 11211, { weight: 5 });

			const testClient = new Memcache({
				nodes: ["localhost:11211", node1, "server2:11212"],
			});

			expect(testClient.nodeIds).toHaveLength(3);
			expect(testClient.nodeIds).toContain("localhost:11211");
			expect(testClient.nodeIds).toContain("server1:11211");
			expect(testClient.nodeIds).toContain("server2:11212");

			// Verify the MemcacheNode instance retained its weight
			const addedNode = testClient.getNode("server1:11211");
			expect(addedNode?.weight).toBe(5);
		});

		it("should work with only MemcacheNode instances (no string URIs)", () => {
			const node1 = createNode("host1", 11211);
			const node2 = createNode("host2", 11212);
			const node3 = createNode("host3", 11213);

			const testClient = new Memcache({
				nodes: [node1, node2, node3],
			});

			expect(testClient.nodeIds).toHaveLength(3);
			expect(testClient.nodeIds).toContain("host1:11211");
			expect(testClient.nodeIds).toContain("host2:11212");
			expect(testClient.nodeIds).toContain("host3:11213");
		});
	});
});
