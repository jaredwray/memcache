import { afterEach, describe, expect, it } from "vitest";
import { createNode, Memcache, MemcacheNode } from "../src/index.js";
import { generateKey, generateValue } from "./test-utils.js";

const SASL_HOST = "localhost";
const SASL_PORT = 11215;
// The username includes the realm as created by saslpasswd2
const TEST_USER = "testuser@localhost";
const TEST_PASS = "testpass";

describe("SASL Authentication", () => {
	describe("MemcacheNode with SASL", () => {
		let node: MemcacheNode;

		afterEach(async () => {
			if (node?.isConnected()) {
				await node.disconnect();
			}
		});

		it("should authenticate with valid credentials", async () => {
			node = new MemcacheNode(SASL_HOST, SASL_PORT, {
				sasl: { username: TEST_USER, password: TEST_PASS },
			});

			await node.connect();
			expect(node.isConnected()).toBe(true);
			expect(node.isAuthenticated).toBe(true);
		});

		it("should fail with invalid credentials", async () => {
			node = new MemcacheNode(SASL_HOST, SASL_PORT, {
				sasl: { username: "wrong", password: "wrong" },
			});

			await expect(node.connect()).rejects.toThrow(
				"SASL authentication failed",
			);
			expect(node.isConnected()).toBe(false);
			expect(node.isAuthenticated).toBe(false);
		});

		it("should execute commands after authentication using binary protocol", async () => {
			node = new MemcacheNode(SASL_HOST, SASL_PORT, {
				sasl: { username: TEST_USER, password: TEST_PASS },
			});

			await node.connect();

			const key = generateKey("sasl");
			const value = generateValue();

			// Use binary protocol methods for SASL-enabled servers
			const setResult = await node.binarySet(key, value);
			expect(setResult).toBe(true);

			const getResult = await node.binaryGet(key);
			expect(getResult).toBe(value);

			// Cleanup
			await node.binaryDelete(key);
		});

		it("should emit authenticated event", async () => {
			node = new MemcacheNode(SASL_HOST, SASL_PORT, {
				sasl: { username: TEST_USER, password: TEST_PASS },
			});

			let authenticated = false;
			node.on("authenticated", () => {
				authenticated = true;
			});

			await node.connect();
			expect(authenticated).toBe(true);
		});

		it("should report hasSaslCredentials correctly", () => {
			const nodeWithSasl = new MemcacheNode(SASL_HOST, SASL_PORT, {
				sasl: { username: TEST_USER, password: TEST_PASS },
			});
			expect(nodeWithSasl.hasSaslCredentials).toBe(true);

			const nodeWithoutSasl = new MemcacheNode(SASL_HOST, SASL_PORT);
			expect(nodeWithoutSasl.hasSaslCredentials).toBe(false);
		});

		it("should report isAuthenticated as false before connecting", () => {
			node = new MemcacheNode(SASL_HOST, SASL_PORT, {
				sasl: { username: TEST_USER, password: TEST_PASS },
			});
			expect(node.isAuthenticated).toBe(false);
		});

		it("should reset authentication state on reconnect", async () => {
			node = new MemcacheNode(SASL_HOST, SASL_PORT, {
				sasl: { username: TEST_USER, password: TEST_PASS },
			});

			await node.connect();
			expect(node.isAuthenticated).toBe(true);

			await node.reconnect();
			expect(node.isConnected()).toBe(true);
			expect(node.isAuthenticated).toBe(true);
		});
	});

	describe("Memcache client with SASL", () => {
		let client: Memcache;

		afterEach(async () => {
			if (client?.isConnected()) {
				await client.disconnect();
			}
		});

		it("should authenticate with SASL credentials in options", async () => {
			client = new Memcache({
				nodes: [`${SASL_HOST}:${SASL_PORT}`],
				sasl: { username: TEST_USER, password: TEST_PASS },
			});

			await client.connect();
			expect(client.isConnected()).toBe(true);
		});

		it("should perform get/set operations with SASL using binary protocol", async () => {
			client = new Memcache({
				nodes: [`${SASL_HOST}:${SASL_PORT}`],
				sasl: { username: TEST_USER, password: TEST_PASS },
			});

			await client.connect();

			const key = generateKey("sasl-client");
			const value = generateValue();

			// For SASL-enabled servers, use binary protocol through the node directly
			const nodes = client.nodes;
			expect(nodes.length).toBe(1);

			const setResult = await nodes[0].binarySet(key, value);
			expect(setResult).toBe(true);

			const getResult = await nodes[0].binaryGet(key);
			expect(getResult).toBe(value);

			// Cleanup
			await nodes[0].binaryDelete(key);
		});

		it("should fail connection with invalid SASL credentials", async () => {
			client = new Memcache({
				nodes: [`${SASL_HOST}:${SASL_PORT}`],
				sasl: { username: "invalid", password: "invalid" },
			});

			await expect(client.connect()).rejects.toThrow(
				"SASL authentication failed",
			);
		});

		it("should perform incr/decr operations with SASL using binary protocol", async () => {
			client = new Memcache({
				nodes: [`${SASL_HOST}:${SASL_PORT}`],
				sasl: { username: TEST_USER, password: TEST_PASS },
			});

			await client.connect();

			const key = generateKey("sasl-counter");
			const node = client.nodes[0];

			// Set initial value using binary protocol
			await node.binarySet(key, "10");

			// Increment - binary incr returns the new value
			// Note: binary incr with initial=0 will create key if not exists
			const incrResult = await node.binaryIncr(key, 5);
			expect(incrResult).toBe(15);

			// Decrement
			const decrResult = await node.binaryDecr(key, 3);
			expect(decrResult).toBe(12);

			// Cleanup
			await node.binaryDelete(key);
		});

		it("should perform add/replace operations with SASL using binary protocol", async () => {
			client = new Memcache({
				nodes: [`${SASL_HOST}:${SASL_PORT}`],
				sasl: { username: TEST_USER, password: TEST_PASS },
			});

			await client.connect();

			const key = generateKey("sasl-add-replace");
			const value1 = generateValue();
			const value2 = generateValue();
			const node = client.nodes[0];

			// Add should succeed for new key
			const addResult = await node.binaryAdd(key, value1);
			expect(addResult).toBe(true);

			// Add should fail for existing key
			const addResult2 = await node.binaryAdd(key, value2);
			expect(addResult2).toBe(false);

			// Replace should succeed for existing key
			const replaceResult = await node.binaryReplace(key, value2);
			expect(replaceResult).toBe(true);

			// Verify the replaced value
			const getResult = await node.binaryGet(key);
			expect(getResult).toBe(value2);

			// Cleanup
			await node.binaryDelete(key);
		});
	});

	describe("createNode factory with SASL", () => {
		it("should create node with SASL options", () => {
			const node = createNode(SASL_HOST, SASL_PORT, {
				sasl: { username: TEST_USER, password: TEST_PASS },
			});

			expect(node.hasSaslCredentials).toBe(true);
		});

		it("should create node without SASL options", () => {
			const node = createNode(SASL_HOST, SASL_PORT);

			expect(node.hasSaslCredentials).toBe(false);
		});
	});
});
