import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer as createTlsServer } from "node:tls";
import { afterEach, describe, expect, it, vi } from "vitest";
import Memcache, { createNode, MemcacheNode } from "../src/index";
import { generateKey, generateValue } from "./test-utils.js";

/**
 * These tests require a TLS-only memcached server (ascii protocol, no UDP)
 * whose certificate is signed by the CA at MEMCACHE_TLS_CA.
 * Defaults match the compose stack's memcached-tls service (localhost:21211).
 */
const TLS_HOST = process.env.MEMCACHE_TLS_HOST ?? "localhost";
const TLS_PORT = Number(process.env.MEMCACHE_TLS_PORT ?? "21211");
const TLS_URI = `${TLS_HOST}:${TLS_PORT}`;
const PLAIN_URI = process.env.MEMCACHE_PLAIN_URI ?? "localhost:11211";
const CA_PATH =
	process.env.MEMCACHE_TLS_CA ??
	new URL("./certs/cacert.pem", import.meta.url).pathname;

const ca = readFileSync(CA_PATH);

const createTlsClient = (options?: { timeout?: number }) =>
	new Memcache({
		nodes: [TLS_URI],
		tls: { ca },
		timeout: options?.timeout,
	});

describe("TLS", () => {
	describe("basic operations", () => {
		it("should set and get a value over TLS", async () => {
			const client = createTlsClient();
			const key = generateKey("tls-roundtrip");
			const value = generateValue();

			expect(await client.set(key, value, 60)).toBe(true);
			expect(await client.get(key)).toBe(value);

			await client.disconnect();
		});

		it("should multi-get values over TLS", async () => {
			const client = createTlsClient();
			const keys = [
				generateKey("tls-multi"),
				generateKey("tls-multi"),
				generateKey("tls-multi"),
			];
			const missingKey = generateKey("tls-multi-missing");

			for (const key of keys) {
				expect(await client.set(key, `value-${key}`, 60)).toBe(true);
			}

			const results = await client.gets([...keys, missingKey]);
			for (const key of keys) {
				expect(results.get(key)).toBe(`value-${key}`);
			}
			expect(results.has(missingKey)).toBe(false);

			await client.disconnect();
		});

		it("should delete a value over TLS", async () => {
			const client = createTlsClient();
			const key = generateKey("tls-delete");

			expect(await client.set(key, generateValue(), 60)).toBe(true);
			expect(await client.delete(key)).toBe(true);
			expect(await client.get(key)).toBeUndefined();

			await client.disconnect();
		});
	});

	describe("pipelining safety", () => {
		it("should resolve concurrent operations fired immediately on a fresh client", async () => {
			// No prior connect/await: the very first writes race the TLS
			// handshake. If readiness resolved on "connect" instead of
			// "secureConnect", commands would be written into an unfinished
			// handshake and fail or hang.
			const client = createTlsClient();
			const prefix = generateKey("tls-burst");
			const count = 20;

			const setResults = await Promise.all(
				Array.from({ length: count }, (_, i) =>
					client.set(`${prefix}:${i}`, `value-${i}`, 60),
				),
			);
			expect(setResults.every((r) => r === true)).toBe(true);

			const getResults = await Promise.all(
				Array.from({ length: count }, (_, i) => client.get(`${prefix}:${i}`)),
			);
			for (let i = 0; i < count; i++) {
				expect(getResults[i]).toBe(`value-${i}`);
			}

			await client.disconnect();
		});

		it("should pipeline concurrent operations on an established TLS connection", async () => {
			const client = createTlsClient();
			await client.connect();
			const prefix = generateKey("tls-pipeline");
			const count = 50;

			// Interleave sets and gets so responses of different shapes are
			// parsed back-to-back from the same TLS socket (FIFO pipelining).
			const seedKey = `${prefix}:seed`;
			expect(await client.set(seedKey, "seed-value", 60)).toBe(true);

			const results = await Promise.all(
				Array.from({ length: count }, (_, i) =>
					i % 2 === 0
						? client.set(`${prefix}:${i}`, `value-${i}`, 60)
						: client.get(seedKey),
				),
			);
			for (let i = 0; i < count; i++) {
				expect(results[i]).toBe(i % 2 === 0 ? true : "seed-value");
			}

			await client.disconnect();
		});
	});

	describe("failure modes", () => {
		it("should reject when the CA cannot verify the server certificate", async () => {
			// tls: true uses Node's default trust store, which does not
			// include the local test CA.
			const client = new Memcache({
				nodes: [TLS_URI],
				tls: true,
				timeout: 2000,
			});

			await expect(
				client.set(generateKey("tls-bad-ca"), generateValue(), 60),
			).rejects.toThrow();

			await client.disconnect();
		});

		it("should reject when connecting with TLS to a plain-text port", async () => {
			const client = new Memcache({
				nodes: [PLAIN_URI],
				tls: { ca },
				timeout: 2000,
			});

			await expect(
				client.set(generateKey("tls-plain-port"), generateValue(), 60),
			).rejects.toThrow();

			await client.disconnect();
		});

		it("should fail cleanly when connecting without TLS to a TLS-only port", async () => {
			const client = new Memcache({
				nodes: [TLS_URI],
				timeout: 2000,
			});

			// The TCP connection succeeds, so the failure surfaces when the
			// server drops the plain-text command mid-handshake. The node
			// rejects with "Connection closed" and the client resolves the
			// operation unsuccessfully (post-connect command failures are
			// swallowed by design) — the important part is no hang.
			const node = client.nodes[0];
			await node.connect();
			await expect(node.command("version")).rejects.toThrow(
				"Connection closed",
			);

			expect(
				await client.set(generateKey("tls-no-tls"), generateValue(), 60),
			).toBe(false);

			await client.disconnect();
		});
	});

	describe("reconnect", () => {
		it("should reconnect over TLS after the socket is destroyed", async () => {
			// Simulates the server killing an idle connection (e.g.
			// idle_timeout) without waiting for a real idle period.
			// TODO: exercise a true server-side idle FIN/RST (e.g. via
			// toxiproxy reset_peer) in a chaos suite; too slow for unit tests.
			const client = createTlsClient();
			const key = generateKey("tls-reconnect");
			const value = generateValue();

			expect(await client.set(key, value, 60)).toBe(true);

			client.nodes[0].socket?.destroy();
			await vi.waitFor(() => {
				expect(client.nodes[0].isConnected()).toBe(false);
			});

			expect(await client.get(key)).toBe(value);

			await client.disconnect();
		});
	});

	describe("memcaches:// scheme", () => {
		it("should parse memcaches:// URIs as secure", () => {
			const client = new Memcache({ nodes: [PLAIN_URI] });

			expect(client.parseUri(`memcaches://${TLS_URI}`)).toEqual({
				host: TLS_HOST,
				port: TLS_PORT,
				secure: true,
			});
			expect(client.parseUri("memcaches://localhost")).toEqual({
				host: "localhost",
				port: 11211,
				secure: true,
			});
			expect(client.parseUri("memcaches://[::1]:21211")).toEqual({
				host: "::1",
				port: 21211,
				secure: true,
			});
			expect(client.parseUri("memcaches://[::1]")).toEqual({
				host: "::1",
				port: 11211,
				secure: true,
			});
			expect(client.parseUri(`memcache://${TLS_URI}`)).toEqual({
				host: TLS_HOST,
				port: TLS_PORT,
			});
		});

		it("should connect with client-level TLS options for memcaches:// nodes", async () => {
			const client = new Memcache({
				nodes: [`memcaches://${TLS_URI}`],
				tls: { ca },
			});
			const key = generateKey("tls-scheme");
			const value = generateValue();

			expect(await client.set(key, value, 60)).toBe(true);
			expect(await client.get(key)).toBe(value);

			await client.disconnect();
		});

		it("should default to the system trust store for memcaches:// nodes without TLS options", async () => {
			// The local test CA is not in the default trust store, so the
			// handshake must fail — proving the scheme alone enables TLS.
			const client = new Memcache({
				nodes: [`memcaches://${TLS_URI}`],
				timeout: 2000,
			});

			await expect(
				client.set(generateKey("tls-scheme-trust"), generateValue(), 60),
			).rejects.toThrow();

			await client.disconnect();
		});

		it("should emit memcaches:// from node.uri when client-level TLS is set", async () => {
			const client = createTlsClient();
			expect(client.nodes[0].uri).toBe(`memcaches://${TLS_URI}`);
			await client.disconnect();
		});

		it("should enable TLS when constructing from a TLS node's uri without client tls", async () => {
			const source = createTlsClient();
			const uri = source.nodes[0].uri;
			await source.disconnect();

			// Round-trip node.uri into a client with no tls option. The
			// handshake must fail against the test CA — proving the scheme
			// from uri kept TLS on.
			const client = new Memcache({
				nodes: [uri],
				timeout: 2000,
			});

			await expect(
				client.set(generateKey("tls-uri-roundtrip"), generateValue(), 60),
			).rejects.toThrow();

			await client.disconnect();
		});

		it("should connect when a TLS node's uri is passed with matching CA options", async () => {
			const source = createTlsClient();
			const client = new Memcache({
				nodes: [source.nodes[0].uri],
				tls: { ca },
			});
			const key = generateKey("tls-uri-connect");
			const value = generateValue();

			expect(await client.set(key, value, 60)).toBe(true);
			expect(await client.get(key)).toBe(value);

			await client.disconnect();
			await source.disconnect();
		});

		it("should parse memcaches:// Unix socket URIs as secure", () => {
			const client = new Memcache({ nodes: [PLAIN_URI] });
			expect(client.parseUri("memcaches:///var/run/memcached.sock")).toEqual({
				host: "/var/run/memcached.sock",
				port: 0,
				secure: true,
			});
			expect(client.parseUri("memcache:///var/run/memcached.sock")).toEqual({
				host: "/var/run/memcached.sock",
				port: 0,
			});
		});
	});

	describe("MemcacheNode with TLS", () => {
		let node: MemcacheNode;

		afterEach(async () => {
			if (node?.isConnected()) {
				await node.disconnect();
			}
		});

		it("should connect with valid CA options", async () => {
			node = new MemcacheNode(TLS_HOST, TLS_PORT, { tls: { ca } });
			await node.connect();
			expect(node.isConnected()).toBe(true);
			expect(node.tlsEnabled).toBe(true);
			const version = await node.command("version");
			expect(version).toContain("VERSION");
		});

		it("should fail handshake with the system trust store", async () => {
			node = new MemcacheNode(TLS_HOST, TLS_PORT, {
				tls: true,
				timeout: 2000,
			});
			await expect(node.connect()).rejects.toThrow();
			expect(node.isConnected()).toBe(false);
		});

		it("should emit connect after the TLS handshake completes", async () => {
			node = new MemcacheNode(TLS_HOST, TLS_PORT, { tls: { ca } });
			let connected = false;
			node.on("connect", () => {
				connected = true;
			});
			await node.connect();
			expect(connected).toBe(true);
		});

		it("should reconnect over TLS", async () => {
			node = new MemcacheNode(TLS_HOST, TLS_PORT, { tls: { ca } });
			await node.connect();
			await node.reconnect();
			expect(node.isConnected()).toBe(true);
			const version = await node.command("version");
			expect(version).toContain("VERSION");
		});

		it("should ignore host/port overrides in tls options", async () => {
			node = new MemcacheNode(TLS_HOST, TLS_PORT, {
				tls: { ca, host: "example.com", port: 443 },
			});
			await node.connect();
			expect(node.isConnected()).toBe(true);
		});
	});

	describe("createNode factory with TLS", () => {
		it("should create a node with TLS options", () => {
			const node = createNode(TLS_HOST, TLS_PORT, { tls: { ca } });
			expect(node.tlsEnabled).toBe(true);
			expect(node.tls).toEqual({ ca });
		});

		it("should create a node without TLS options", () => {
			const node = createNode(TLS_HOST, TLS_PORT);
			expect(node.tlsEnabled).toBe(false);
			expect(node.tls).toBeUndefined();
		});
	});

	describe("client operations over TLS", () => {
		it("should perform add/replace over TLS", async () => {
			const client = createTlsClient();
			const key = generateKey("tls-add-replace");
			const value1 = generateValue();
			const value2 = generateValue();

			expect(await client.add(key, value1, 60)).toBe(true);
			expect(await client.add(key, value2, 60)).toBe(false);
			expect(await client.replace(key, value2, 60)).toBe(true);
			expect(await client.get(key)).toBe(value2);

			await client.disconnect();
		});

		it("should perform append/prepend over TLS", async () => {
			const client = createTlsClient();
			const key = generateKey("tls-append-prepend");

			expect(await client.set(key, "middle", 60)).toBe(true);
			expect(await client.append(key, "-suffix")).toBe(true);
			expect(await client.prepend(key, "prefix-")).toBe(true);
			expect(await client.get(key)).toBe("prefix-middle-suffix");

			await client.disconnect();
		});

		it("should perform incr/decr over TLS", async () => {
			const client = createTlsClient();
			const key = generateKey("tls-counter");

			expect(await client.set(key, "10", 60)).toBe(true);
			expect(await client.incr(key, 5)).toBe(15);
			expect(await client.decr(key, 3)).toBe(12);

			await client.disconnect();
		});

		it("should perform touch over TLS", async () => {
			const client = createTlsClient();
			const key = generateKey("tls-touch");

			expect(await client.set(key, "touchme", 60)).toBe(true);
			expect(await client.touch(key, 3600)).toBe(true);
			expect(await client.get(key)).toBe("touchme");

			await client.disconnect();
		});

		it("should get version over TLS", async () => {
			const client = createTlsClient();
			const versions = await client.version();
			expect(versions.size).toBeGreaterThan(0);
			for (const version of versions.values()) {
				expect(version.length).toBeGreaterThan(0);
			}
			await client.disconnect();
		});

		it("should get stats over TLS", async () => {
			const client = createTlsClient();
			const stats = await client.stats();
			expect(stats.size).toBeGreaterThan(0);
			for (const nodeStats of stats.values()) {
				expect(nodeStats.pid).toBeDefined();
			}
			await client.disconnect();
		});
	});

	describe("Unix socket TLS", () => {
		it("should complete a TLS handshake on a Unix socket path", async () => {
			const dir = mkdtempSync(join(tmpdir(), "memcache-tls-"));
			const socketPath = join(dir, "memcached.sock");
			const key = readFileSync(
				new URL("./certs/server_key.pem", import.meta.url),
			);
			const cert = readFileSync(
				new URL("./certs/server_crt.pem", import.meta.url),
			);

			const server = createTlsServer({ key, cert }, (socket) => {
				socket.end();
			});
			await new Promise<void>((resolve, reject) => {
				server.once("error", reject);
				server.listen(socketPath, () => resolve());
			});

			const node = new MemcacheNode(socketPath, 0, {
				tls: { ca, checkServerIdentity: () => undefined },
				timeout: 2000,
			});
			try {
				await node.connect();
				expect(node.isConnected()).toBe(true);
				expect(node.uri).toBe(`memcaches://${socketPath}`);
			} finally {
				await node.disconnect();
				await new Promise<void>((resolve) => server.close(() => resolve()));
				rmSync(dir, { recursive: true, force: true });
			}
		});
	});

	describe("TLS + SASL", () => {
		const TLS_SASL_HOST = process.env.MEMCACHE_TLS_SASL_HOST ?? "localhost";
		const TLS_SASL_PORT = Number(process.env.MEMCACHE_TLS_SASL_PORT ?? "21215");
		const TEST_USER = "testuser@localhost";
		const TEST_PASS = "testpass";

		let node: MemcacheNode;

		afterEach(async () => {
			if (node?.isConnected()) {
				await node.disconnect();
			}
		});

		it("should authenticate over TLS with valid credentials", async () => {
			node = new MemcacheNode(TLS_SASL_HOST, TLS_SASL_PORT, {
				tls: { ca },
				sasl: { username: TEST_USER, password: TEST_PASS },
			});
			await node.connect();
			expect(node.isConnected()).toBe(true);
			expect(node.isAuthenticated).toBe(true);
			expect(node.tlsEnabled).toBe(true);
		});

		it("should fail SASL over TLS with invalid credentials", async () => {
			node = new MemcacheNode(TLS_SASL_HOST, TLS_SASL_PORT, {
				tls: { ca },
				sasl: { username: "wrong", password: "wrong" },
				timeout: 2000,
			});
			await expect(node.connect()).rejects.toThrow(
				"SASL authentication failed",
			);
			expect(node.isConnected()).toBe(false);
			expect(node.isAuthenticated).toBe(false);
		});

		it("should execute binary commands after TLS+SASL authentication", async () => {
			node = new MemcacheNode(TLS_SASL_HOST, TLS_SASL_PORT, {
				tls: { ca },
				sasl: { username: TEST_USER, password: TEST_PASS },
			});
			await node.connect();

			const key = generateKey("tls-sasl");
			const value = generateValue();
			expect(await node.binarySet(key, value)).toBe(true);
			expect(await node.binaryGet(key)).toBe(value);
			await node.binaryDelete(key);
		});

		it("should authenticate over TLS via the client", async () => {
			const client = new Memcache({
				nodes: [`${TLS_SASL_HOST}:${TLS_SASL_PORT}`],
				tls: { ca },
				sasl: { username: TEST_USER, password: TEST_PASS },
				lazyConnect: true,
			});
			await client.connect();
			expect(client.isConnected()).toBe(true);
			expect(client.nodes[0].isAuthenticated).toBe(true);
			expect(client.nodes[0].tlsEnabled).toBe(true);
			await client.disconnect();
		});
	});
});
