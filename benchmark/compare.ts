import Memcached from "memcached";
import memjs from "memjs";
import { Bench } from "tinybench";
import pkg from "../package.json" with { type: "json" };
import { cleanVersion, createClient, HOST, PORT, setAll } from "./utils.js";

// The same work through each client against the same server, with every
// result checked. Clients take turns within each row, so drift on the machine
// affects them alike. Each cell is the median of tinybench's samples.
const REQUESTS = 500;
const IN_FLIGHT = [1, 10, 100, 500];
const MULTI_GET_KEYS = 10_000;
const BATCH_SIZES = [100, 1_000, 10_000];

type Client = {
	label: string;
	get(key: string): Promise<string | undefined>;
	set(key: string, value: string): Promise<boolean>;
	/** Fetches the keys and resolves with how many were found. */
	getMany(keys: string[]): Promise<number>;
	close(): Promise<void>;
};

const memcacheClient = createClient();
await memcacheClient.connect();

// Generous timeouts on every client, so a slow machine shows up in the
// numbers instead of failing the run. Otherwise each client keeps its
// defaults: memcached pools up to 10 connections, the others use one.
const memjsClient = memjs.Client.create(`${HOST}:${PORT}`, { timeout: 10 });
const memcachedClient = new Memcached(`${HOST}:${PORT}`, { timeout: 10_000 });

async function memjsGet(key: string): Promise<string | undefined> {
	const { value } = await memjsClient.get(key);
	return value?.toString();
}

const clients: Client[] = [
	{
		label: `${pkg.name} (v${pkg.version})`,
		get: (key) => memcacheClient.get(key),
		set: (key, value) => memcacheClient.set(key, value),
		getMany: async (keys) => (await memcacheClient.gets(keys)).size,
		close: () => memcacheClient.disconnect(),
	},
	{
		label: `memjs (v${cleanVersion(pkg.devDependencies.memjs)})`,
		get: memjsGet,
		set: (key, value) => memjsClient.set(key, value, { expires: 0 }),
		// memjs has no multi-get: one get per key, all in flight at once
		getMany: async (keys) =>
			(await Promise.all(keys.map(memjsGet))).filter((v) => v !== undefined)
				.length,
		close: async () => memjsClient.close(),
	},
	{
		label: `memcached (v${cleanVersion(pkg.devDependencies.memcached)})`,
		get: (key) =>
			new Promise((resolve, reject) => {
				memcachedClient.get(key, (err, data) => {
					if (err) reject(err);
					else resolve(data as string | undefined);
				});
			}),
		set: (key, value) =>
			new Promise((resolve, reject) => {
				memcachedClient.set(key, value, 0, (err) => {
					if (err) reject(err);
					else resolve(true);
				});
			}),
		getMany: (keys) =>
			new Promise((resolve, reject) => {
				memcachedClient.getMulti(keys, (err, data) => {
					if (err) reject(err);
					else resolve(Object.keys(data).length);
				});
			}),
		close: async () => memcachedClient.end(),
	},
];

/** Each row lets a different client go first. */
function inTurn(row: number): Client[] {
	return clients.map((_, i) => clients[(row + i) % clients.length]);
}

// Concurrent gets and sets: REQUESTS per run, with 1 to 500 in flight
const value = "x".repeat(100);
const keys = Array.from({ length: 1000 }, (_, i) => `bench:compare:${i}`);
await setAll(memcacheClient, keys, value);

async function send(inFlight: number, request: (key: string) => Promise<void>) {
	let next = 0;
	const worker = async () => {
		while (next < REQUESTS) {
			await request(keys[next++ % keys.length]);
		}
	};
	await Promise.all(Array.from({ length: inFlight }, worker));
}

const concurrency = new Bench({ throws: true });
const concurrencyRows: string[] = [];
for (const name of ["gets", "sets"] as const) {
	for (const inFlight of IN_FLIGHT) {
		const row = `${name}, ${inFlight} in flight`;
		for (const client of inTurn(concurrencyRows.length)) {
			const request =
				name === "gets"
					? async (key: string) => {
							if ((await client.get(key)) !== value) {
								throw new Error(
									`${client.label}: get returned the wrong value`,
								);
							}
						}
					: async (key: string) => {
							if (!(await client.set(key, value))) {
								throw new Error(`${client.label}: set failed`);
							}
						};
			concurrency.add(`${row} | ${client.label}`, () =>
				send(inFlight, request),
			);
		}
		concurrencyRows.push(row);
	}
}

// Multi-get: the same 10,000 keys in batches of different sizes
const multiGetKeys = Array.from(
	{ length: MULTI_GET_KEYS },
	(_, i) => `bench:user:profile:${i}`,
);
await setAll(memcacheClient, multiGetKeys, "0123456789");

const multiGet = new Bench({
	iterations: 3,
	warmupIterations: 1,
	time: 3000,
	throws: true,
});
const multiGetRows: string[] = [];
for (const size of BATCH_SIZES) {
	const batches = Array.from({ length: MULTI_GET_KEYS / size }, (_, i) =>
		multiGetKeys.slice(i * size, (i + 1) * size),
	);
	const row = `${batches.length} × ${size.toLocaleString("en-US")} keys`;
	for (const client of inTurn(multiGetRows.length)) {
		multiGet.add(`${row} | ${client.label}`, async () => {
			for (const batch of batches) {
				const found = await client.getMany(batch);
				if (found !== batch.length) {
					throw new Error(
						`${client.label}: found ${found} of ${batch.length} keys`,
					);
				}
			}
		});
	}
	multiGetRows.push(row);
}

try {
	await concurrency.run();
	await multiGet.run();
} finally {
	for (const client of clients) {
		await client.close();
	}
}

/** Median milliseconds per run of a task. */
function median(bench: Bench, row: string, client: Client): number {
	const { result } = bench.getTask(`${row} | ${client.label}`) ?? {};
	if (result?.state !== "completed") {
		throw new Error(`${row} | ${client.label} did not complete`);
	}
	return result.latency.p50;
}

function table(
	header: string,
	rows: string[],
	cell: (row: string, client: Client) => number,
	format: (value: number) => string,
	best: (values: number[]) => number,
): string {
	const lines = [
		`| ${header} | ${clients.map((client) => client.label).join(" | ")} |`,
		`|---|${clients.map(() => "--:").join("|")}|`,
	];
	for (const row of rows) {
		const values = clients.map((client) => cell(row, client));
		const winner = best(values);
		const cells = values.map((v) =>
			v === winner ? `**${format(v)}**` : format(v),
		);
		lines.push(`| ${row} | ${cells.join(" | ")} |`);
	}
	return lines.join("\n");
}

const perSecond = (n: number) =>
	n >= 1000 ? `${(n / 1000).toFixed(1)}K` : `${Math.round(n)}`;
const duration = (ms: number) =>
	ms >= 1000
		? `${(ms / 1000).toFixed(2)} s`
		: `${ms >= 10 ? Math.round(ms) : ms.toFixed(1)} ms`;

console.log("");
console.log("## Compared with memjs and memcached");
console.log("");
console.log(
	`Requests per second, ${REQUESTS} requests per run (higher is better). memcached pools up to 10 connections; the other clients use one.`,
);
console.log("");
console.log(
	table(
		"Workload",
		concurrencyRows,
		(row, client) => (REQUESTS * 1000) / median(concurrency, row, client),
		perSecond,
		(values) => Math.max(...values),
	),
);
console.log("");
console.log(
	`Time to fetch ${MULTI_GET_KEYS.toLocaleString("en-US")} keys in batches (lower is better). memjs has no multi-get, so it sends one get per key, all at once.`,
);
console.log("");
console.log(
	table(
		"Batches",
		multiGetRows,
		(row, client) => median(multiGet, row, client),
		duration,
		(values) => Math.min(...values),
	),
);
console.log("");
