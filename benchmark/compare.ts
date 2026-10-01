import Memcached from "memcached";
import memjs from "memjs";
import { Bench, type BenchOptions } from "tinybench";
import pkg from "../package.json" with { type: "json" };
import {
	cleanVersion,
	comparisonTable,
	createClient,
	duration,
	HOST,
	PORT,
	setAll,
} from "./utils.js";

// The same work through each client against the same server, with every
// result checked. Clients take turns within each row, and each cell is the
// median over ROUNDS rounds of tinybench's median.
const REQUESTS = 500;
const IN_FLIGHT = [1, 10, 100, 500];
const MULTI_GET_KEYS = 10_000;
const BATCH_SIZES = [100, 1_000, 10_000];
const ROUNDS = 3;

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

/** Each row and round lets a different client go first. */
function inTurn(turn: number): Client[] {
	return clients.map((_, i) => clients[(turn + i) % clients.length]);
}

type Row = { name: string; task: (client: Client) => () => Promise<void> };

/** Per row and client, the median milliseconds per run in each round. */
const results = new Map<string, number[]>();
const taskName = (row: string, client: Client) => `${row} | ${client.label}`;

/**
 * One machine drifts between runs, so the tables report the median of
 * ROUNDS rounds, each starting with a different client.
 */
async function runRounds(options: BenchOptions, rows: Row[]): Promise<void> {
	for (let round = 0; round < ROUNDS; round++) {
		const bench = new Bench({ ...options, throws: true });
		for (const [i, row] of rows.entries()) {
			for (const client of inTurn(i + round)) {
				bench.add(taskName(row.name, client), row.task(client));
			}
		}
		await bench.run();
		for (const task of bench.tasks) {
			if (task.result.state !== "completed") {
				throw new Error(`${task.name} did not complete`);
			}
			results.set(task.name, [
				...(results.get(task.name) ?? []),
				task.result.latency.p50,
			]);
		}
	}
}

function median(row: string, client: Client): number {
	const values = (results.get(taskName(row, client)) ?? []).sort(
		(a, b) => a - b,
	);
	return values[Math.floor(values.length / 2)];
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

const concurrencyRows: Row[] = [];
for (const name of ["gets", "sets"] as const) {
	for (const inFlight of IN_FLIGHT) {
		concurrencyRows.push({
			name: `${name}, ${inFlight} in flight`,
			task: (client) => {
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
				return () => send(inFlight, request);
			},
		});
	}
}

// Multi-get: the same 10,000 keys in batches of different sizes
const multiGetKeys = Array.from(
	{ length: MULTI_GET_KEYS },
	(_, i) => `bench:user:profile:${i}`,
);
await setAll(memcacheClient, multiGetKeys, "0123456789");

const multiGetRows: Row[] = BATCH_SIZES.map((size) => {
	const batches = Array.from({ length: MULTI_GET_KEYS / size }, (_, i) =>
		multiGetKeys.slice(i * size, (i + 1) * size),
	);
	return {
		name: `${batches.length} × ${size.toLocaleString("en-US")} keys`,
		task: (client) => async () => {
			for (const batch of batches) {
				const found = await client.getMany(batch);
				if (found !== batch.length) {
					throw new Error(
						`${client.label}: found ${found} of ${batch.length} keys`,
					);
				}
			}
		},
	};
});

try {
	await runRounds(
		{ iterations: 16, time: 500, warmupIterations: 4, warmupTime: 100 },
		concurrencyRows,
	);
	await runRounds(
		{ iterations: 3, time: 1000, warmupIterations: 1, warmupTime: 0 },
		multiGetRows,
	);
} finally {
	for (const client of clients) {
		await client.close();
	}
}

function table(
	header: string,
	rows: Row[],
	cell: (row: string, client: Client) => number,
	format: (value: number) => string,
	best: (values: number[]) => number,
): string {
	return comparisonTable(
		header,
		clients.map((client) => client.label),
		rows.map(({ name }) => ({
			name,
			values: clients.map((client) => cell(name, client)),
		})),
		format,
		best,
	);
}

const perSecond = (n: number) =>
	n >= 1000 ? `${(n / 1000).toFixed(1)}K` : `${Math.round(n)}`;

console.log("");
console.log("## Compared with memjs and memcached");
console.log("");
console.log(
	`Each cell is the median of ${ROUNDS} rounds, with the clients taking turns. 🥇 marks the fastest client in each row, and the percentages compare the other clients with it.`,
);
console.log("");
console.log(
	`Requests per second, ${REQUESTS} requests per run (higher is better). memcached pools up to 10 connections; the other clients use one.`,
);
console.log("");
console.log(
	table(
		"Workload",
		concurrencyRows,
		(row, client) => (REQUESTS * 1000) / median(row, client),
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
		(row, client) => median(row, client),
		duration,
		(values) => Math.min(...values),
	),
);
console.log("");
