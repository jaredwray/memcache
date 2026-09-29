import { readFileSync } from "node:fs";
import { isIP } from "node:net";
import pkg from "../package.json" with { type: "json" };
import { Memcache } from "../src/index.js";

/**
 * Performance suite for the client itself: throughput by requests in flight,
 * large multi-gets, large values over TCP and TLS, bursts of concurrent
 * requests, and sockets opened by a cold client.
 *
 * Defaults target the compose `bench` profile (`pnpm benchmark:services:start`).
 * On Linux, published Docker ports go through docker-proxy, which adds
 * per-packet overhead. For numbers closer to a real network, point the
 * variables below at the container IPs with port 11211.
 */
const HOST = process.env.MEMCACHE_BENCH_HOST ?? "localhost";
const PORT = Number(process.env.MEMCACHE_BENCH_PORT ?? "11216");
const TLS_HOST = process.env.MEMCACHE_BENCH_TLS_HOST ?? "localhost";
const TLS_PORT = Number(process.env.MEMCACHE_BENCH_TLS_PORT ?? "21216");
// Set to "1" to leave out the TLS measurements (for servers without TLS).
const SKIP_TLS = process.env.MEMCACHE_BENCH_SKIP_TLS === "1";

const RUNS = 3;
const MB = 1024 * 1024;

/**
 * The defaults fit the bench TLS container, which uses the test certificate
 * (issued for "localhost"), so IP targets are verified against that name. For
 * a server with its own certificate, set MEMCACHE_BENCH_TLS_CA to its CA
 * bundle, and MEMCACHE_BENCH_TLS_SERVERNAME if the name differs from the host.
 */
function tlsOptions() {
	return {
		ca: readFileSync(
			process.env.MEMCACHE_BENCH_TLS_CA ??
				new URL("../test/certs/cacert.pem", import.meta.url),
		),
		servername:
			process.env.MEMCACHE_BENCH_TLS_SERVERNAME ??
			(isIP(TLS_HOST) ? "localhost" : TLS_HOST),
	};
}

function createClient(secure = false): Memcache {
	return new Memcache({
		nodes: [secure ? `${TLS_HOST}:${TLS_PORT}` : `${HOST}:${PORT}`],
		maxValueSize: 32 * MB,
		tls: secure ? tlsOptions() : undefined,
	});
}

function median(values: number[]): number {
	const sorted = [...values].sort((a, b) => a - b);
	return sorted[Math.floor(sorted.length / 2)];
}

function format(value: number, digits = 0): string {
	return value.toLocaleString("en-US", {
		minimumFractionDigits: digits,
		maximumFractionDigits: digits,
	});
}

function table(headers: string[], rows: string[][]): string {
	return [
		`| ${headers.join(" | ")} |`,
		`|${headers.map((_, i) => (i === 0 ? "---" : "---:")).join("|")}|`,
		...rows.map((row) => `| ${row.join(" | ")} |`),
	].join("\n");
}

/**
 * Runs `total` operations with `concurrency` workers, each issuing one request
 * at a time (like concurrent request handlers). Returns operations per second.
 */
async function closedLoop(
	concurrency: number,
	total: number,
	op: (i: number) => Promise<unknown>,
): Promise<number> {
	let next = 0;
	const worker = async () => {
		while (next < total) {
			await op(next++);
		}
	};
	const start = performance.now();
	await Promise.all(Array.from({ length: concurrency }, worker));
	return total / ((performance.now() - start) / 1000);
}

/** Average milliseconds per call over `reps` sequential calls, after one warmup call. */
async function averageMs(
	reps: number,
	call: () => Promise<unknown>,
): Promise<number> {
	await call();
	const start = performance.now();
	for (let i = 0; i < reps; i++) {
		await call();
	}
	return (performance.now() - start) / reps;
}

async function setAll(
	client: Memcache,
	keys: string[],
	value: string,
): Promise<void> {
	const results = await Promise.all(keys.map((key) => client.set(key, value)));
	if (results.includes(false)) {
		throw new Error("Failed to store benchmark keys");
	}
}

async function throughput(): Promise<string> {
	const client = createClient();
	await client.connect();
	const value = "x".repeat(100);
	const keys = Array.from({ length: 1000 }, (_, i) => `bench:tp:${i}`);
	await setAll(client, keys, value);

	// Check every result so a change that returns misses or failed stores
	// can't look like a speedup.
	const operations = [
		async (i: number) => {
			if ((await client.get(keys[i % keys.length])) !== value) {
				throw new Error("get() returned the wrong value");
			}
		},
		async (i: number) => {
			if (!(await client.set(keys[i % keys.length], value))) {
				throw new Error("set() failed");
			}
		},
	];
	const rows: string[][] = [];
	for (const concurrency of [1, 10, 100, 500]) {
		const total = concurrency === 1 ? 10_000 : 50_000;
		const row = [format(concurrency)];
		for (const operation of operations) {
			await closedLoop(concurrency, 2_000, operation);
			const runs: number[] = [];
			for (let run = 0; run < RUNS; run++) {
				runs.push(await closedLoop(concurrency, total, operation));
			}
			row.push(format(median(runs)));
		}
		rows.push(row);
	}
	await client.disconnect();

	return `### Throughput by requests in flight (ops/s, median of ${RUNS} runs)\n\n${table(["In flight", "get", "set"], rows)}`;
}

async function multiGet(): Promise<string> {
	const client = createClient();
	await client.connect();
	const rows: string[][] = [];
	for (const count of [100, 1_000, 10_000]) {
		const keys = Array.from(
			{ length: count },
			(_, i) => `bench:user:profile:${i}`,
		);
		await setAll(client, keys, "0123456789");
		const ms = await averageMs(count >= 10_000 ? 3 : 10, async () => {
			const values = await client.gets(keys);
			if (values.size !== count) {
				throw new Error(`gets() returned ${values.size} of ${count} keys`);
			}
		});
		rows.push([format(count), format(ms, 2)]);
	}
	await client.disconnect();

	return `### Multi-get: \`gets()\`, all hits (ms per call)\n\n${table(["Keys", "ms"], rows)}`;
}

async function largeValueGets(secure: boolean, sizes: number[]) {
	const client = createClient(secure);
	await client.connect();
	const results: number[] = [];
	for (const size of sizes) {
		const key = `bench:large:${size}`;
		await setAll(client, [key], "v".repeat(size));
		results.push(
			await averageMs(10, async () => {
				const value = await client.get(key);
				if (value?.length !== size) {
					throw new Error(`get() returned the wrong value for ${key}`);
				}
			}),
		);
	}
	await client.disconnect();
	return results;
}

async function largeValues(): Promise<string> {
	const sizes = [256 * 1024, MB, 4 * MB];
	const tcp = await largeValueGets(false, sizes);
	const tls = SKIP_TLS
		? sizes.map(() => "skipped")
		: (await largeValueGets(true, sizes)).map((ms) => format(ms, 2));
	const rows = sizes.map((size, i) => [
		`${size / 1024} KB`,
		format(tcp[i], 2),
		tls[i],
	]);

	return `### Large values: \`get\` (ms per call)\n\n${table(["Value", "TCP", "TLS"], rows)}`;
}

async function bursts(): Promise<string> {
	const client = createClient();
	await client.connect();
	const keys = Array.from({ length: 1000 }, (_, i) => `bench:burst:${i}`);
	await setAll(client, keys, "0123456789");
	const rows: string[][] = [];
	for (const count of [10_000, 30_000, 100_000]) {
		const start = performance.now();
		const values = await Promise.all(
			Array.from({ length: count }, (_, i) => client.get(keys[i % 1000])),
		);
		const ms = performance.now() - start;
		if (values.some((value) => value !== "0123456789")) {
			throw new Error("get() returned the wrong value during a burst");
		}
		rows.push([format(count), format(ms), format((ms * 1000) / count, 1)]);
	}
	await client.disconnect();

	return `### Bursts of concurrent gets\n\n${table(["Requests", "Total ms", "µs per request"], rows)}`;
}

async function coldStart(): Promise<string> {
	const monitor = createClient();
	const totalConnections = async () => {
		const stats = await monitor.stats();
		return Number(stats.get(monitor.nodes[0].id)?.total_connections);
	};
	const before = await totalConnections();
	const client = createClient();
	await Promise.all(
		Array.from({ length: 50 }, (_, i) => client.get(`bench:cold:${i}`)),
	);
	const opened = (await totalConnections()) - before;

	return `### Cold start: lazy connect\n\n${table(["Concurrent first requests", "Sockets opened"], [["50", format(opened)]])}`;
}

const info = createClient();
const versions = await info.version();
await info.disconnect();
const serverVersion = [...versions.values()][0]?.replace(/^VERSION /, "");

console.log(`## memcache v${pkg.version} performance\n`);
console.log(`- Node.js ${process.version}`);
console.log(
	`- memcached ${serverVersion} at ${HOST}:${PORT} (TLS at ${TLS_HOST}:${TLS_PORT})\n`,
);
for (const section of [throughput, multiGet, largeValues, bursts, coldStart]) {
	console.log(`${await section()}\n`);
}

// A cold start can leave extra sockets open, which would keep the process alive.
process.exit(0);
