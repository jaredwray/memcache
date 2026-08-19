import { afterEach, describe, expect, it, vi } from "vitest";
import { AutoDiscovery } from "../src/auto-discovery.js";
import Memcache from "../src/index.js";
import { MemcacheNode } from "../src/node.js";
import type { ClusterConfig } from "../src/types.js";
import { MemcacheEvents } from "../src/types.js";
import { FakeConfigServer } from "./fake-config-server.js";

// Helper to wait for a condition with polling
async function waitFor(
	fn: () => boolean,
	timeoutMs = 5000,
	intervalMs = 25,
): Promise<void> {
	const start = Date.now();
	while (!fn()) {
		if (Date.now() - start > timeoutMs) {
			throw new Error("waitFor timed out");
		}
		await new Promise((r) => setTimeout(r, intervalMs));
	}
}

describe("AutoDiscovery", () => {
	describe("parseNodeEntry", () => {
		it("should parse a standard hostname|ip|port entry", () => {
			const result = AutoDiscovery.parseNodeEntry(
				"myCluster.pc4ldq.0001.use1.cache.amazonaws.com|10.82.235.120|11211",
			);
			expect(result).toEqual({
				hostname: "myCluster.pc4ldq.0001.use1.cache.amazonaws.com",
				ip: "10.82.235.120",
				port: 11211,
			});
		});

		it("should parse entry with missing IP", () => {
			const result = AutoDiscovery.parseNodeEntry(
				"myCluster.pc4ldq.0001.use1.cache.amazonaws.com||11211",
			);
			expect(result).toEqual({
				hostname: "myCluster.pc4ldq.0001.use1.cache.amazonaws.com",
				ip: "",
				port: 11211,
			});
		});

		it("should parse entry with IPv6 address", () => {
			const result = AutoDiscovery.parseNodeEntry(
				"myCluster.pc4ldq.0001.use1.cache.amazonaws.com|2001:db8::1|11211",
			);
			expect(result).toEqual({
				hostname: "myCluster.pc4ldq.0001.use1.cache.amazonaws.com",
				ip: "2001:db8::1",
				port: 11211,
			});
		});

		it("should throw on entry with wrong number of fields", () => {
			expect(() => AutoDiscovery.parseNodeEntry("hostname|ip")).toThrow(
				"Invalid node entry format",
			);
			expect(() =>
				AutoDiscovery.parseNodeEntry("hostname|ip|port|extra"),
			).toThrow("Invalid node entry format");
		});

		it("should throw on entry with empty hostname", () => {
			expect(() => AutoDiscovery.parseNodeEntry("|10.0.0.1|11211")).toThrow(
				"missing hostname",
			);
		});

		it("should throw on entry with invalid port", () => {
			expect(() => AutoDiscovery.parseNodeEntry("hostname|10.0.0.1|0")).toThrow(
				"Invalid port",
			);
			expect(() =>
				AutoDiscovery.parseNodeEntry("hostname|10.0.0.1|99999"),
			).toThrow("Invalid port");
			expect(() =>
				AutoDiscovery.parseNodeEntry("hostname|10.0.0.1|abc"),
			).toThrow("Invalid port");
		});
	});

	describe("parseConfigResponse", () => {
		it("should parse a valid multi-node response", () => {
			const rawData = [
				"12\nmyCluster.pc4ldq.0001.use1.cache.amazonaws.com|10.82.235.120|11211 myCluster.pc4ldq.0002.use1.cache.amazonaws.com|10.80.249.27|11211\n",
			];
			const result = AutoDiscovery.parseConfigResponse(rawData);
			expect(result.version).toBe(12);
			expect(result.nodes).toHaveLength(2);
			expect(result.nodes[0].hostname).toBe(
				"myCluster.pc4ldq.0001.use1.cache.amazonaws.com",
			);
			expect(result.nodes[0].ip).toBe("10.82.235.120");
			expect(result.nodes[0].port).toBe(11211);
			expect(result.nodes[1].hostname).toBe(
				"myCluster.pc4ldq.0002.use1.cache.amazonaws.com",
			);
			expect(result.nodes[1].ip).toBe("10.80.249.27");
			expect(result.nodes[1].port).toBe(11211);
		});

		it("should parse a single node response", () => {
			const rawData = [
				"1\nmyCluster.pc4ldq.0001.use1.cache.amazonaws.com|10.82.235.120|11211\n",
			];
			const result = AutoDiscovery.parseConfigResponse(rawData);
			expect(result.version).toBe(1);
			expect(result.nodes).toHaveLength(1);
		});

		it("should parse response with missing IP addresses", () => {
			const rawData = [
				"5\nmyCluster.pc4ldq.0001.use1.cache.amazonaws.com||11211\n",
			];
			const result = AutoDiscovery.parseConfigResponse(rawData);
			expect(result.version).toBe(5);
			expect(result.nodes).toHaveLength(1);
			expect(result.nodes[0].ip).toBe("");
		});

		it("should handle multiple raw data segments", () => {
			const rawData = ["3\n", "host1|10.0.0.1|11211 host2|10.0.0.2|11211\n"];
			const result = AutoDiscovery.parseConfigResponse(rawData);
			expect(result.version).toBe(3);
			expect(result.nodes).toHaveLength(2);
		});

		it("should throw on empty response", () => {
			expect(() => AutoDiscovery.parseConfigResponse([])).toThrow(
				"Empty config response",
			);
		});

		it("should throw on response with only version", () => {
			expect(() => AutoDiscovery.parseConfigResponse(["12\n"])).toThrow(
				"expected version and node list",
			);
		});

		it("should throw on invalid version number", () => {
			expect(() =>
				AutoDiscovery.parseConfigResponse(["abc\nhost|ip|11211\n"]),
			).toThrow("Invalid config version");
		});
	});

	describe("nodeId", () => {
		it("should return ip:port when IP is available", () => {
			expect(
				AutoDiscovery.nodeId({
					hostname: "myhost.cache.amazonaws.com",
					ip: "10.0.0.1",
					port: 11211,
				}),
			).toBe("10.0.0.1:11211");
		});

		it("should return hostname:port when IP is empty", () => {
			expect(
				AutoDiscovery.nodeId({
					hostname: "myhost.cache.amazonaws.com",
					ip: "",
					port: 11211,
				}),
			).toBe("myhost.cache.amazonaws.com:11211");
		});

		it("should bracket IPv6 addresses", () => {
			expect(
				AutoDiscovery.nodeId({
					hostname: "myhost.cache.amazonaws.com",
					ip: "2001:db8::1",
					port: 11211,
				}),
			).toBe("[2001:db8::1]:11211");
		});

		it("should bracket IPv6 hostname when IP is empty", () => {
			expect(
				AutoDiscovery.nodeId({
					hostname: "::1",
					ip: "",
					port: 11211,
				}),
			).toBe("[::1]:11211");
		});
	});

	describe("constructor and properties", () => {
		it("should initialize with correct defaults", () => {
			const discovery = new AutoDiscovery({
				configEndpoint: "myhost:11211",
				pollingInterval: 30000,
				useLegacyCommand: false,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			expect(discovery.configVersion).toBe(-1);
			expect(discovery.isRunning).toBe(false);
			expect(discovery.configEndpoint).toBe("myhost:11211");
		});
	});

	describe("start and stop", () => {
		let server: FakeConfigServer;
		let discovery: AutoDiscovery;

		afterEach(async () => {
			if (discovery?.isRunning) {
				await discovery.stop();
			}
			if (server) {
				await server.stop();
			}
		});

		it("should throw if started twice", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 60000,
				useLegacyCommand: false,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			await discovery.start();
			expect(discovery.isRunning).toBe(true);

			await expect(discovery.start()).rejects.toThrow(
				"Auto discovery is already running",
			);
		});

		it("should reset isRunning when start fails and allow retry", async () => {
			discovery = new AutoDiscovery({
				configEndpoint: "127.0.0.1:1", // unreachable port
				pollingInterval: 60000,
				useLegacyCommand: false,
				timeout: 500,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			await expect(discovery.start()).rejects.toThrow();
			expect(discovery.isRunning).toBe(false);

			// Should be able to start again after failure
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			// Reconfigure to point at the working server
			// @ts-expect-error - accessing private field for testing
			discovery._configEndpoint = server.endpoint;

			const config = await discovery.start();
			expect(discovery.isRunning).toBe(true);
			expect(config.version).toBe(1);
		}, 10000);

		it("should stop correctly", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 60000,
				useLegacyCommand: false,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			await discovery.start();
			expect(discovery.isRunning).toBe(true);

			await discovery.stop();
			expect(discovery.isRunning).toBe(false);
		});

		it("should emit autoDiscover event on initial discovery", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 60000,
				useLegacyCommand: false,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			const emittedConfigs: ClusterConfig[] = [];
			discovery.on("autoDiscover", (config: ClusterConfig) => {
				emittedConfigs.push(config);
			});

			const config = await discovery.start();
			expect(config.version).toBe(1);
			expect(config.nodes).toHaveLength(1);
			expect(emittedConfigs).toHaveLength(1);
			expect(emittedConfigs[0].version).toBe(1);
		});
	});

	describe("discover", () => {
		let server: FakeConfigServer;

		afterEach(async () => {
			if (server) {
				await server.stop();
			}
		});

		it("should return undefined when version has not changed", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			const discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 60000,
				useLegacyCommand: false,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			await discovery.start();

			// Second discover with same version should return undefined
			const result = await discovery.discover();
			expect(result).toBeUndefined();

			await discovery.stop();
		});

		it("should return config when version changes", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			const discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 60000,
				useLegacyCommand: false,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			await discovery.start();
			expect(discovery.configVersion).toBe(1);

			// Update the fake server's config
			server.version = 2;
			server.nodes = ["host1|10.0.0.1|11211", "host2|10.0.0.2|11211"];

			const result = await discovery.discover();
			expect(result).toBeDefined();
			expect(result?.version).toBe(2);
			expect(result?.nodes).toHaveLength(2);
			expect(discovery.configVersion).toBe(2);

			await discovery.stop();
		});
	});

	describe("polling", () => {
		let server: FakeConfigServer;
		let discovery: AutoDiscovery;

		afterEach(async () => {
			if (discovery?.isRunning) {
				await discovery.stop();
			}
			if (server) {
				await server.stop();
			}
		});

		it("should poll and detect version changes", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 50,
				useLegacyCommand: false,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			await discovery.start();
			expect(discovery.configVersion).toBe(1);

			// Update config on the server
			server.version = 2;
			server.nodes = ["host1|10.0.0.1|11211", "host2|10.0.0.2|11211"];

			// Wait for polling to pick up the change
			await waitFor(() => discovery.configVersion === 2);
			expect(discovery.configVersion).toBe(2);
		});

		it("should emit autoDiscoverUpdate on version change during poll", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 50,
				useLegacyCommand: false,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			const updates: ClusterConfig[] = [];
			discovery.on("autoDiscoverUpdate", (config: ClusterConfig) => {
				updates.push(config);
			});

			await discovery.start();

			server.version = 2;
			server.nodes = ["host1|10.0.0.1|11211", "host2|10.0.0.2|11211"];

			await waitFor(() => updates.length >= 1);
			expect(updates[0].version).toBe(2);
			expect(updates[0].nodes).toHaveLength(2);
		});

		it("should emit autoDiscoverError on poll failure", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 50,
				useLegacyCommand: false,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			const errors: Error[] = [];
			discovery.on("autoDiscoverError", (error: Error) => {
				errors.push(error);
			});

			await discovery.start();

			// Switch to error mode
			server.errorResponse = "SERVER_ERROR simulated failure";

			await waitFor(() => errors.length >= 1);
			expect(errors.length).toBeGreaterThanOrEqual(1);
		});
	});

	describe("legacy command", () => {
		let server: FakeConfigServer;

		afterEach(async () => {
			if (server) {
				await server.stop();
			}
		});

		it("should use get AmazonElastiCache:cluster when useLegacyCommand is true", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			const discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 60000,
				useLegacyCommand: true,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			const config = await discovery.start();
			expect(config.version).toBe(1);
			expect(config.nodes).toHaveLength(1);

			await discovery.stop();
		});

		it("should throw when legacy command returns no data", async () => {
			server = new FakeConfigServer({
				version: 1,
				nodes: [],
				respondEmpty: true,
			});
			await server.start();

			const discovery = new AutoDiscovery({
				configEndpoint: server.endpoint,
				pollingInterval: 60000,
				useLegacyCommand: true,
				timeout: 5000,
				keepAlive: true,
				keepAliveDelay: 1000,
			});

			await expect(discovery.start()).rejects.toThrow(
				"No config data received from legacy command",
			);
		});
	});
});

describe("Memcache AutoDiscovery Integration", () => {
	describe("constructor", () => {
		it("should store autoDiscover options", () => {
			const client = new Memcache({
				nodes: ["localhost:11211"],
				autoDiscover: {
					enabled: true,
					configEndpoint: "myhost.cfg:11211",
					pollingInterval: 30000,
				},
			});

			// Verify the client was created successfully
			expect(client).toBeDefined();
			expect(client.nodeIds).toContain("localhost:11211");
		});

		it("should work without autoDiscover option", () => {
			const client = new Memcache({
				nodes: ["localhost:11211"],
			});
			expect(client).toBeDefined();
		});
	});

	describe("connect with auto discovery", () => {
		it("should emit AUTO_DISCOVER_ERROR when discovery fails", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				autoDiscover: {
					enabled: true,
					configEndpoint: "nonexistent:11211",
					timeout: 500,
				},
			});

			const errors: unknown[] = [];
			client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, (error: unknown) => {
				errors.push(error);
			});

			// connect will try to start auto discovery which will fail
			// since there's no actual server
			await client.connect();

			// Error should be emitted (connection refused)
			expect(errors.length).toBeGreaterThanOrEqual(1);
		}, 10000);

		it("should successfully discover nodes from a config server", async () => {
			const server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				autoDiscover: {
					enabled: true,
					configEndpoint: server.endpoint,
				},
			});

			const configs: ClusterConfig[] = [];
			client.on(MemcacheEvents.AUTO_DISCOVER, (config: ClusterConfig) => {
				configs.push(config);
			});
			// Suppress errors from trying to connect to discovered node 10.0.0.1
			client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, () => {});
			client.on(MemcacheEvents.ERROR, () => {});

			await client.connect();

			expect(configs).toHaveLength(1);
			expect(configs[0].version).toBe(1);
			expect(client.nodeIds).toContain("10.0.0.1:11211");

			await client.disconnect();
			await server.stop();
		});
	});

	describe("disconnect stops auto discovery", () => {
		it("should handle disconnect cleanly with autoDiscover enabled", async () => {
			const server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				autoDiscover: {
					enabled: true,
					configEndpoint: server.endpoint,
				},
			});

			// Suppress discovery errors
			client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, () => {});
			client.on(MemcacheEvents.ERROR, () => {});

			await client.connect();
			await client.disconnect();

			// Should not throw
			expect(client.isConnected()).toBe(false);

			await server.stop();
		});
	});

	describe("quit stops auto discovery", () => {
		it("should stop auto discovery when quit is called", async () => {
			const server = new FakeConfigServer({
				version: 1,
				nodes: ["host1|10.0.0.1|11211"],
			});
			await server.start();

			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				autoDiscover: {
					enabled: true,
					configEndpoint: server.endpoint,
				},
			});

			client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, () => {});
			client.on(MemcacheEvents.ERROR, () => {});

			await client.connect();
			await client.quit();

			// Auto discovery should be stopped
			// @ts-expect-error - accessing private field for testing
			expect(client._autoDiscovery).toBeUndefined();

			await server.stop();
		});
	});

	describe("applyClusterConfig", () => {
		it("should add new nodes from discovered config", async () => {
			const client = new Memcache({
				nodes: ["10.0.0.1:11211"],
				lazyConnect: true,
			});

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [
					{ hostname: "host1", ip: "10.0.0.1", port: 11211 },
					{ hostname: "host2", ip: "10.0.0.2", port: 11211 },
				],
			});

			expect(client.nodeIds).toContain("10.0.0.1:11211");
			expect(client.nodeIds).toContain("10.0.0.2:11211");
		});

		it("should remove nodes not in discovered config", async () => {
			const client = new Memcache({
				nodes: ["10.0.0.1:11211", "10.0.0.2:11211"],
				lazyConnect: true,
			});

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 2,
				nodes: [{ hostname: "host1", ip: "10.0.0.1", port: 11211 }],
			});

			expect(client.nodeIds).toContain("10.0.0.1:11211");
			expect(client.nodeIds).not.toContain("10.0.0.2:11211");
		});

		it("should not remove nodes when config returns empty list", async () => {
			const client = new Memcache({
				nodes: ["10.0.0.1:11211"],
				lazyConnect: true,
			});

			const errors: unknown[] = [];
			client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, (error: unknown) => {
				errors.push(error);
			});

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 3,
				nodes: [],
			});

			// Should keep existing nodes
			expect(client.nodeIds).toContain("10.0.0.1:11211");
			// Should emit error
			expect(errors).toHaveLength(1);
		});

		it("should use hostname when IP is empty", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
			});

			// Remove the default localhost node
			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [
					{
						hostname: "myhost.cache.amazonaws.com",
						ip: "",
						port: 11211,
					},
				],
			});

			expect(client.nodeIds).toContain("myhost.cache.amazonaws.com:11211");
		});

		it("should handle no changes gracefully", async () => {
			const client = new Memcache({
				nodes: ["10.0.0.1:11211"],
				lazyConnect: true,
			});

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [{ hostname: "host1", ip: "10.0.0.1", port: 11211 }],
			});

			expect(client.nodeIds).toEqual(["10.0.0.1:11211"]);
		});

		it("should bracket IPv6 addresses when adding nodes", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
			});

			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [
					{ hostname: "host1", ip: "2001:db8::1", port: 11211 },
					{ hostname: "host2", ip: "10.0.0.1", port: 11211 },
				],
			});

			expect(client.nodeIds).toContain("[2001:db8::1]:11211");
			expect(client.nodeIds).toContain("10.0.0.1:11211");
		});

		it("should apply TLS and SNI hostname to discovered nodes", async () => {
			const client = new Memcache({
				nodes: ["10.0.0.1:11211"],
				lazyConnect: true,
				tls: true,
			});

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [
					{ hostname: "host1", ip: "10.0.0.1", port: 11211 },
					{
						hostname: "host2.cache.amazonaws.com",
						ip: "10.0.0.2",
						port: 11211,
					},
				],
			});

			const existing = client.getNode("10.0.0.1:11211");
			expect(existing?.tlsEnabled).toBe(true);
			expect(existing?.tls).toBe(true);

			const discovered = client.getNode("10.0.0.2:11211");
			expect(discovered?.tlsEnabled).toBe(true);
			expect(discovered?.tls).toEqual({
				servername: "host2.cache.amazonaws.com",
			});
			expect(discovered?.uri).toBe("memcaches://10.0.0.2:11211");
		});

		it("should keep caller-supplied servername on discovered TLS nodes", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				tls: { servername: "custom.example.com" },
			});
			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [
					{
						hostname: "host1.cache.amazonaws.com",
						ip: "10.0.0.5",
						port: 11211,
					},
				],
			});

			expect(client.getNode("10.0.0.5:11211")?.tls).toEqual({
				servername: "custom.example.com",
			});
		});

		it("should not set SNI when discovered hostname is the connecting IP", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				tls: true,
			});
			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [{ hostname: "10.0.0.8", ip: "10.0.0.8", port: 11211 }],
			});

			expect(client.getNode("10.0.0.8:11211")?.tls).toBe(true);
		});

		it("should apply TLS to IPv6 discovered nodes and set SNI", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				tls: true,
			});
			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [
					{
						hostname: "host1.example.com",
						ip: "2001:db8::1",
						port: 11211,
					},
				],
			});

			const discovered = client.getNode("[2001:db8::1]:11211");
			expect(discovered?.tlsEnabled).toBe(true);
			expect(discovered?.tls).toEqual({ servername: "host1.example.com" });
		});

		it("should not set SNI when discovered hostname is an IP", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				tls: true,
			});
			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [{ hostname: "10.0.0.1", ip: "10.0.0.2", port: 11211 }],
			});

			expect(client.getNode("10.0.0.2:11211")?.tls).toBe(true);
		});

		it("should not set SNI when discovered hostname is empty", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				tls: true,
			});
			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [{ hostname: "", ip: "10.0.0.4", port: 11211 }],
			});

			expect(client.getNode("10.0.0.4:11211")?.tls).toBe(true);
		});

		it("should keep hostname as TLS target when discovered IP is empty", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				tls: true,
			});
			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [
					{
						hostname: "myhost.cache.amazonaws.com",
						ip: "",
						port: 11211,
					},
				],
			});

			const discovered = client.getNode("myhost.cache.amazonaws.com:11211");
			expect(discovered?.tlsEnabled).toBe(true);
			expect(discovered?.tls).toBe(true);
		});

		it("should not set SNI when discovered hostname is IPv6", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				tls: true,
			});
			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [{ hostname: "2001:db8::1", ip: "2001:db8::2", port: 11211 }],
			});

			expect(client.getNode("[2001:db8::2]:11211")?.tls).toBe(true);
		});

		it("should merge SNI into existing tls ConnectionOptions", async () => {
			const client = new Memcache({
				nodes: [],
				lazyConnect: true,
				tls: { minVersion: "TLSv1.2" },
			});
			await client.removeNode("localhost:11211");

			// @ts-expect-error - accessing private method for testing
			await client.applyClusterConfig({
				version: 1,
				nodes: [
					{
						hostname: "node.cache.amazonaws.com",
						ip: "10.0.0.6",
						port: 11211,
					},
				],
			});

			expect(client.getNode("10.0.0.6:11211")?.tls).toEqual({
				minVersion: "TLSv1.2",
				servername: "node.cache.amazonaws.com",
			});
		});
	});
});

describe("AutoDiscovery parseEndpoint", () => {
	it("should parse standard host:port", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "myhost:11211",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		const result = discovery.parseEndpoint("myhost:11211");
		expect(result).toEqual({ host: "myhost", port: 11211 });
	});

	it("should parse host-only without port (default to 11211)", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "myhost",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		const result = discovery.parseEndpoint("myhost");
		expect(result).toEqual({ host: "myhost", port: 11211 });
	});

	it("should parse IPv6 with brackets and port", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "[::1]:11211",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		const result = discovery.parseEndpoint("[::1]:11211");
		expect(result).toEqual({ host: "::1", port: 11211 });
	});

	it("should parse IPv6 with brackets without port", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "[::1]",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		const result = discovery.parseEndpoint("[::1]");
		expect(result).toEqual({ host: "::1", port: 11211 });
	});

	it("should parse IPv6 with brackets and trailing colon only", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "[::1]:",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		const result = discovery.parseEndpoint("[::1]:");
		expect(result).toEqual({ host: "::1", port: 11211 });
	});

	it("should throw on IPv6 with missing closing bracket", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "[::1",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		expect(() => discovery.parseEndpoint("[::1")).toThrow(
			"missing closing bracket",
		);
	});

	it("should default to 11211 for IPv6 with non-colon remainder", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "[::1]foo",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		const result = discovery.parseEndpoint("[::1]foo");
		expect(result).toEqual({ host: "::1", port: 11211 });
	});

	it("should default to 11211 for IPv6 with non-numeric port", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "[::1]:abc",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		const result = discovery.parseEndpoint("[::1]:abc");
		expect(result).toEqual({ host: "::1", port: 11211 });
	});

	it("should default to 11211 for non-numeric port in standard format", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "host:abc",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		const result = discovery.parseEndpoint("host:abc");
		expect(result).toEqual({ host: "host", port: 11211 });
	});

	it("should parse memcaches:// as secure", () => {
		const discovery = new AutoDiscovery({
			configEndpoint: "memcaches://myhost:11211",
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// @ts-expect-error - accessing private method for testing
		expect(discovery.parseEndpoint("memcaches://myhost:11211")).toEqual({
			host: "myhost",
			port: 11211,
			secure: true,
		});
		// @ts-expect-error - accessing private method for testing
		expect(discovery.parseEndpoint("memcache://myhost:11211")).toEqual({
			host: "myhost",
			port: 11211,
		});
		// @ts-expect-error - accessing private method for testing
		expect(discovery.parseEndpoint("memcaches://[::1]:21211")).toEqual({
			host: "::1",
			port: 21211,
			secure: true,
		});
	});
});

describe("AutoDiscovery TLS", () => {
	let server: FakeConfigServer;

	afterEach(async () => {
		if (server) {
			await server.stop();
		}
	});

	it("should pass tls options to the config node", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		const discovery = new AutoDiscovery({
			configEndpoint: server.endpoint,
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 500,
			keepAlive: true,
			keepAliveDelay: 1000,
			tls: true,
		});

		await expect(discovery.start()).rejects.toThrow();
		expect(discovery.tls).toBe(true);
		// @ts-expect-error - accessing private field for testing
		expect(discovery._configNode?.tlsEnabled).toBe(true);
	});

	it("should enable TLS for memcaches:// config endpoints", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		const discovery = new AutoDiscovery({
			configEndpoint: `memcaches://${server.endpoint}`,
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 500,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		await expect(discovery.start()).rejects.toThrow();
		expect(discovery.tls).toBe(true);
		// @ts-expect-error - accessing private field for testing
		expect(discovery._configNode?.tlsEnabled).toBe(true);
	});

	it("should keep explicit tls options when the endpoint is memcaches://", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		const tls = { servername: "cfg.example.com" };
		const discovery = new AutoDiscovery({
			configEndpoint: `memcaches://${server.endpoint}`,
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 500,
			keepAlive: true,
			keepAliveDelay: 1000,
			tls,
		});

		await expect(discovery.start()).rejects.toThrow();
		expect(discovery.tls).toEqual(tls);
	});
});

describe("MemcacheNode CONFIG response parsing", () => {
	let node: MemcacheNode;

	afterEach(async () => {
		if (node.isConnected()) {
			await node.disconnect();
		}
	});

	it("should parse CONFIG response with data via fake server", async () => {
		const server = new FakeConfigServer({
			version: 12,
			nodes: [
				"myCluster.0001.use1.cache.amazonaws.com|10.82.235.120|11211",
				"myCluster.0002.use1.cache.amazonaws.com|10.80.249.27|11211",
			],
		});
		await server.start();

		node = new MemcacheNode("127.0.0.1", server.port, { timeout: 5000 });
		await node.connect();

		const result = await node.command("config get cluster", {
			isConfig: true,
		});

		expect(result).toBeDefined();
		expect(result).toHaveLength(1);
		// Parse the result to verify it's valid
		const config = AutoDiscovery.parseConfigResponse(result);
		expect(config.version).toBe(12);
		expect(config.nodes).toHaveLength(2);

		await server.stop();
	});

	it("should resolve with undefined when CONFIG has no data before END", async () => {
		const server = new FakeConfigServer({
			respondEmpty: true,
		});
		await server.start();

		node = new MemcacheNode("127.0.0.1", server.port, { timeout: 5000 });
		await node.connect();

		const result = await node.command("config get cluster", {
			isConfig: true,
		});

		expect(result).toBeUndefined();

		await server.stop();
	});

	it("should reject on ERROR response for config command", async () => {
		const server = new FakeConfigServer({
			errorResponse: "ERROR",
		});
		await server.start();

		node = new MemcacheNode("127.0.0.1", server.port, { timeout: 5000 });
		await node.connect();

		const commandPromise = node.command("config get cluster", {
			isConfig: true,
		});

		await expect(commandPromise).rejects.toThrow("ERROR");

		await server.stop();
	});

	it("should reject on CLIENT_ERROR response for config command", async () => {
		const server = new FakeConfigServer({
			errorResponse: "CLIENT_ERROR bad command",
		});
		await server.start();

		node = new MemcacheNode("127.0.0.1", server.port, { timeout: 5000 });
		await node.connect();

		const commandPromise = node.command("config get cluster", {
			isConfig: true,
		});

		await expect(commandPromise).rejects.toThrow("CLIENT_ERROR");

		await server.stop();
	});

	it("should reject on SERVER_ERROR response for config command", async () => {
		const server = new FakeConfigServer({
			errorResponse: "SERVER_ERROR out of memory",
		});
		await server.start();

		node = new MemcacheNode("127.0.0.1", server.port, { timeout: 5000 });
		await node.connect();

		const commandPromise = node.command("config get cluster", {
			isConfig: true,
		});

		await expect(commandPromise).rejects.toThrow("SERVER_ERROR");

		await server.stop();
	});
});

describe("AutoDiscovery poll reconnect", () => {
	let server: FakeConfigServer;
	let discovery: AutoDiscovery;

	afterEach(async () => {
		if (discovery?.isRunning) {
			await discovery.stop();
		}
		if (server) {
			await server.stop();
		}
	});

	it("should recover after error and detect new config", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		discovery = new AutoDiscovery({
			configEndpoint: server.endpoint,
			pollingInterval: 50,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		const errors: Error[] = [];
		discovery.on("autoDiscoverError", (error: Error) => {
			errors.push(error);
		});

		await discovery.start();
		expect(discovery.configVersion).toBe(1);

		// Switch server to error mode to simulate failures
		server.errorResponse = "SERVER_ERROR simulated failure";

		// Wait for at least one error from the failed poll
		await waitFor(() => errors.length >= 1);

		// Switch back to normal mode with new version
		server.errorResponse = undefined;
		server.version = 2;
		server.nodes = ["host1|10.0.0.1|11211", "host2|10.0.0.2|11211"];

		// Wait for recovery - the poll should pick up the new config
		await waitFor(() => discovery.configVersion === 2);
		expect(discovery.configVersion).toBe(2);
	});

	it("should handle persistent connection failure gracefully", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		discovery = new AutoDiscovery({
			configEndpoint: server.endpoint,
			pollingInterval: 50,
			useLegacyCommand: false,
			timeout: 500,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		const errors: Error[] = [];
		discovery.on("autoDiscoverError", (error: Error) => {
			errors.push(error);
		});

		await discovery.start();

		// Stop the server entirely - reconnection attempts will fail
		await server.stop();

		// Wait for error events from failed polls
		await waitFor(() => errors.length >= 1, 5000);
		expect(errors.length).toBeGreaterThanOrEqual(1);

		// Discovery should still be running (errors are non-fatal)
		expect(discovery.isRunning).toBe(true);
	}, 10000);
});

describe("Memcache applyClusterConfig error paths", () => {
	it("should emit ERROR when addNode throws (duplicate node)", async () => {
		const client = new Memcache({
			nodes: ["10.0.0.1:11211"],
			lazyConnect: true,
		});

		const errors: unknown[] = [];
		client.on(MemcacheEvents.ERROR, (...args: unknown[]) => {
			errors.push(args);
		});

		// Mock addNode to throw for the new node
		vi.spyOn(client, "addNode").mockRejectedValueOnce(new Error("Add failed"));

		// @ts-expect-error - accessing private method for testing
		await client.applyClusterConfig({
			version: 1,
			nodes: [
				{ hostname: "host1", ip: "10.0.0.1", port: 11211 },
				{ hostname: "host2", ip: "10.0.0.2", port: 11211 },
			],
		});

		// 10.0.0.1 is already in the set, so addNode is only called for 10.0.0.2
		// But we mocked it to fail
		expect(errors).toHaveLength(1);

		vi.restoreAllMocks();
	});

	it("should emit ERROR when removeNode throws", async () => {
		const client = new Memcache({
			nodes: ["10.0.0.1:11211", "10.0.0.2:11211"],
			lazyConnect: true,
		});

		const errors: unknown[] = [];
		client.on(MemcacheEvents.ERROR, (...args: unknown[]) => {
			errors.push(args);
		});

		vi.spyOn(client, "removeNode").mockRejectedValueOnce(
			new Error("Remove failed"),
		);

		// @ts-expect-error - accessing private method for testing
		await client.applyClusterConfig({
			version: 2,
			nodes: [{ hostname: "host1", ip: "10.0.0.1", port: 11211 }],
		});

		// 10.0.0.2 should be removed but the mock throws
		expect(errors).toHaveLength(1);

		vi.restoreAllMocks();
	});
});

describe("Memcache startAutoDiscovery", () => {
	let server: FakeConfigServer;

	afterEach(async () => {
		if (server) {
			await server.stop();
		}
	});

	it("should use first node as config endpoint when configEndpoint is not specified", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: [], // Will be set after port is known
		});
		await server.start();

		// Make the discovered node match the fake server endpoint
		// so applyClusterConfig doesn't remove the initial node
		server.nodes = [`host1|127.0.0.1|${server.port}`];

		const client = new Memcache({
			nodes: [server.endpoint],
			autoDiscover: {
				enabled: true,
				// No configEndpoint — should use first node
			},
		});

		// Suppress errors
		client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, () => {});
		client.on(MemcacheEvents.ERROR, () => {});

		await client.connect();

		// The first node was used as config endpoint and discovery succeeded
		expect(client.nodeIds).toContain(server.endpoint);

		await client.disconnect();
	});

	it("should emit AUTO_DISCOVER_UPDATE when polling detects changes", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		const client = new Memcache({
			nodes: ["10.0.0.1:11211"],
			lazyConnect: true,
			autoDiscover: {
				enabled: true,
				configEndpoint: server.endpoint,
				pollingInterval: 50,
			},
		});

		const updates: ClusterConfig[] = [];
		client.on(MemcacheEvents.AUTO_DISCOVER_UPDATE, (config: ClusterConfig) => {
			updates.push(config);
		});
		client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, () => {});
		client.on(MemcacheEvents.ERROR, () => {});

		// Mock node connections since they point to fake IPs
		for (const node of client.nodes) {
			vi.spyOn(node, "connect").mockResolvedValue();
		}

		await client.connect();

		// Update server config
		server.version = 2;
		server.nodes = ["host1|10.0.0.1|11211", "host2|10.0.0.3|11211"];

		await waitFor(() => updates.length >= 1, 5000);
		expect(updates[0].version).toBe(2);

		await client.disconnect();

		vi.restoreAllMocks();
	});

	it("should pass client-level tls to AutoDiscovery", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		const client = new Memcache({
			nodes: ["10.0.0.1:11211"],
			lazyConnect: true,
			timeout: 500,
			tls: true,
			autoDiscover: {
				enabled: true,
				configEndpoint: server.endpoint,
			},
		});
		client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, () => {});
		client.on(MemcacheEvents.ERROR, () => {});
		for (const node of client.nodes) {
			vi.spyOn(node, "connect").mockResolvedValue();
		}

		await client.connect();
		// @ts-expect-error - accessing private field for testing
		expect(client._autoDiscovery?.tls).toBe(true);

		await client.disconnect();
		vi.restoreAllMocks();
	});

	it("should inherit tls from the first node when client tls is unset", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		const tlsNode = new MemcacheNode("10.0.0.1", 11211, { tls: true });
		vi.spyOn(tlsNode, "connect").mockResolvedValue();
		const client = new Memcache({
			nodes: [tlsNode],
			lazyConnect: true,
			timeout: 500,
			autoDiscover: {
				enabled: true,
				configEndpoint: server.endpoint,
			},
		});
		client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, () => {});
		client.on(MemcacheEvents.ERROR, () => {});

		await client.connect();
		// @ts-expect-error - accessing private field for testing
		expect(client._autoDiscovery?.tls).toBe(true);

		await client.disconnect();
		vi.restoreAllMocks();
	});

	it("should pass tls: false through to AutoDiscovery", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: [],
		});
		await server.start();
		server.nodes = [`host1|127.0.0.1|${server.port}`];

		const client = new Memcache({
			nodes: [server.endpoint],
			lazyConnect: true,
			tls: false,
			autoDiscover: {
				enabled: true,
				configEndpoint: server.endpoint,
			},
		});
		client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, () => {});
		client.on(MemcacheEvents.ERROR, () => {});

		await client.connect();
		// @ts-expect-error - accessing private field for testing
		expect(client._autoDiscovery?.tls).toBe(false);

		await client.disconnect();
	});

	it("should apply inferred memcaches:// TLS to discovered nodes", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		const client = new Memcache({
			nodes: ["10.0.0.1:11211"],
			lazyConnect: true,
			timeout: 500,
			autoDiscover: {
				enabled: true,
				configEndpoint: `memcaches://${server.endpoint}`,
			},
		});
		client.on(MemcacheEvents.AUTO_DISCOVER_ERROR, () => {});
		client.on(MemcacheEvents.ERROR, () => {});
		for (const node of client.nodes) {
			vi.spyOn(node, "connect").mockResolvedValue();
		}

		await client.connect();
		// @ts-expect-error - accessing private field for testing
		expect(client._autoDiscovery?.tls).toBe(true);

		// @ts-expect-error - accessing private method for testing
		await client.applyClusterConfig({
			version: 1,
			nodes: [
				{ hostname: "host1", ip: "10.0.0.1", port: 11211 },
				{
					hostname: "host2.example.com",
					ip: "10.0.0.9",
					port: 11211,
				},
			],
		});

		expect(client.getNode("10.0.0.9:11211")?.tls).toEqual({
			servername: "host2.example.com",
		});

		await client.disconnect();
		vi.restoreAllMocks();
	});
});

describe("AutoDiscovery additional coverage", () => {
	let server: FakeConfigServer;

	afterEach(async () => {
		if (server) {
			await server.stop();
		}
	});

	it("should reuse existing config node when already connected (ensureConfigNode early return)", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		const discovery = new AutoDiscovery({
			configEndpoint: server.endpoint,
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// First discover creates the config node
		const config1 = await discovery.discover();
		expect(config1).toBeDefined();
		expect(config1?.version).toBe(1);

		// Second discover reuses the existing connected node
		server.version = 2;
		server.nodes = ["host1|10.0.0.1|11211", "host2|10.0.0.2|11211"];

		const config2 = await discovery.discover();
		expect(config2).toBeDefined();
		expect(config2?.version).toBe(2);

		// @ts-expect-error - accessing private field for testing
		const configNode = discovery._configNode;
		expect(configNode).toBeDefined();
		expect(configNode?.isConnected()).toBe(true);

		await discovery.stop();
	});

	it("should connect disconnected node in fetchConfig", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
		});
		await server.start();

		const discovery = new AutoDiscovery({
			configEndpoint: server.endpoint,
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// Create a real MemcacheNode pointing at the fake server, but not connected
		const node = new MemcacheNode("127.0.0.1", server.port, { timeout: 5000 });
		expect(node.isConnected()).toBe(false);

		// @ts-expect-error - accessing private method for testing
		const result = await discovery.fetchConfig(node);
		expect(result.version).toBe(1);

		// fetchConfig should have connected it
		expect(node.isConnected()).toBe(true);

		await node.disconnect();
	});

	it("should throw when config get cluster returns empty result", async () => {
		server = new FakeConfigServer({
			respondEmpty: true,
		});
		await server.start();

		const discovery = new AutoDiscovery({
			configEndpoint: server.endpoint,
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		await expect(discovery.start()).rejects.toThrow("No config data received");
	});

	it("should prevent overlapping polls via isPolling guard", async () => {
		// Use a slow server to make polls take a long time
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|10.0.0.1|11211"],
			responseDelay: 200,
		});
		await server.start();

		const discovery = new AutoDiscovery({
			configEndpoint: server.endpoint,
			pollingInterval: 50, // faster than response delay
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		// Need to start without the delay for the initial fetch
		server.responseDelay = 0;
		await discovery.start();

		// Now enable the delay so polls take a long time
		server.responseDelay = 200;

		// Wait for enough time that multiple polls would have fired
		await new Promise((r) => setTimeout(r, 300));

		// The isPolling guard should have prevented overlapping polls
		// We can verify this by checking that the discovery is still running
		// (if polls overlapped and broke state, it could crash)
		expect(discovery.isRunning).toBe(true);

		await discovery.stop();
	});

	it("should discover and parse IPv6 nodes end-to-end", async () => {
		server = new FakeConfigServer({
			version: 1,
			nodes: ["host1|2001:db8::1|11211", "host2|10.0.0.1|11212"],
		});
		await server.start();

		const discovery = new AutoDiscovery({
			configEndpoint: server.endpoint,
			pollingInterval: 60000,
			useLegacyCommand: false,
			timeout: 5000,
			keepAlive: true,
			keepAliveDelay: 1000,
		});

		const config = await discovery.start();
		expect(config.version).toBe(1);
		expect(config.nodes).toHaveLength(2);
		expect(config.nodes[0].ip).toBe("2001:db8::1");

		// Verify nodeId brackets the IPv6 address
		expect(AutoDiscovery.nodeId(config.nodes[0])).toBe("[2001:db8::1]:11211");
		expect(AutoDiscovery.nodeId(config.nodes[1])).toBe("10.0.0.1:11212");

		await discovery.stop();
	});
});
