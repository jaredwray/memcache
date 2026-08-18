import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import Memcache from "../src/index";
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
	});
});
