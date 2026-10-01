import { randomUUID } from "node:crypto";
import Memcached from "memcached";
import memjs from "memjs";
import pkg from "../package.json" with { type: "json" };
import {
	cleanVersion,
	comparisonTable,
	createClient,
	duration,
	HOST,
	PORT,
} from "./utils.js";

// Each library runs TASKS tasks that set, get and delete one key, and TASKS
// that set, get and delete MULTI_KEYS keys. The tasks of all the libraries
// go into one queue in random order and run one at a time, so no library
// always goes first and anything that drifts during the run hits them all
// alike. Every result is checked. A few slow tasks (a GC pause, a late
// packet) can move a library's total by several percent, so the queue runs
// ROUNDS times, shuffled each time, and the table shows the median.
const TASKS = 1000;
const MULTI_KEYS = 10;
const WARMUP_TASKS = 100;
const ROUNDS = 5;

type Library = {
	label: string;
	set(key: string, value: string): Promise<boolean>;
	get(key: string): Promise<string | undefined>;
	/** Resolves with the values of the keys, in order. */
	getMany(keys: string[]): Promise<Array<string | undefined>>;
	delete(key: string): Promise<boolean>;
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

const libraries: Library[] = [
	{
		label: `${pkg.name} (v${pkg.version})`,
		set: (key, value) => memcacheClient.set(key, value),
		get: (key) => memcacheClient.get(key),
		getMany: async (keys) => {
			const values = await memcacheClient.gets(keys);
			return keys.map((key) => values.get(key));
		},
		delete: (key) => memcacheClient.delete(key),
		close: () => memcacheClient.disconnect(),
	},
	{
		label: `memjs (v${cleanVersion(pkg.devDependencies.memjs)})`,
		set: (key, value) => memjsClient.set(key, value, { expires: 0 }),
		get: memjsGet,
		// memjs has no multi-get: one get per key, all at once
		getMany: (keys) => Promise.all(keys.map(memjsGet)),
		delete: (key) => memjsClient.delete(key),
		close: async () => memjsClient.close(),
	},
	{
		label: `memcached (v${cleanVersion(pkg.devDependencies.memcached)})`,
		set: (key, value) =>
			new Promise((resolve, reject) => {
				memcachedClient.set(key, value, 0, (err, result) => {
					if (err) reject(err);
					else resolve(result);
				});
			}),
		get: (key) =>
			new Promise((resolve, reject) => {
				memcachedClient.get(key, (err, data) => {
					if (err) reject(err);
					else resolve(data as string | undefined);
				});
			}),
		getMany: (keys) =>
			new Promise((resolve, reject) => {
				memcachedClient.getMulti(keys, (err, data) => {
					if (err) reject(err);
					else resolve(keys.map((key) => data[key] as string | undefined));
				});
			}),
		delete: (key) =>
			new Promise((resolve, reject) => {
				memcachedClient.del(key, (err, result) => {
					if (err) reject(err);
					else resolve(result);
				});
			}),
		close: async () => memcachedClient.end(),
	},
];

type Kind = "single" | "multi";
type Task = {
	library: Library;
	kind: Kind;
	keys: string[];
	values: string[];
};

function fail(library: Library, message: string): never {
	throw new Error(`${library.label}: ${message}`);
}

async function run({ library, kind, keys, values }: Task): Promise<void> {
	if (kind === "single") {
		if (!(await library.set(keys[0], values[0]))) {
			fail(library, "set failed");
		}
		if ((await library.get(keys[0])) !== values[0]) {
			fail(library, "get returned the wrong value");
		}
		if (!(await library.delete(keys[0]))) {
			fail(library, "delete failed");
		}
		return;
	}

	// The text protocol has no multi-set or multi-delete, so those send one
	// command per key, all at once
	const stored = await Promise.all(
		keys.map((key, i) => library.set(key, values[i])),
	);
	if (stored.includes(false)) {
		fail(library, "set failed");
	}
	const found = await library.getMany(keys);
	if (found.some((value, i) => value !== values[i])) {
		fail(library, "get returned the wrong value");
	}
	const deleted = await Promise.all(keys.map((key) => library.delete(key)));
	if (deleted.includes(false)) {
		fail(library, "delete failed");
	}
}

let nextKey = 0;

/** `count` tasks of each kind for every library, in random order. */
function queue(count: number): Task[] {
	const tasks: Task[] = [];
	for (const library of libraries) {
		for (const kind of ["single", "multi"] as const) {
			for (let i = 0; i < count; i++) {
				const keys = Array.from(
					{ length: kind === "single" ? 1 : MULTI_KEYS },
					() => `bench:set-get:${nextKey++}`,
				);
				const values = keys.map(() => randomUUID());
				tasks.push({ library, kind, keys, values });
			}
		}
	}
	// Fisher-Yates shuffle
	for (let i = tasks.length - 1; i > 0; i--) {
		const j = Math.floor(Math.random() * (i + 1));
		[tasks[i], tasks[j]] = [tasks[j], tasks[i]];
	}
	return tasks;
}

/** Per round, the milliseconds spent per library and kind of task. */
const rounds: Array<Map<string, number>> = [];
const spentName = (library: Library, kind: Kind) => `${library.label} ${kind}`;

try {
	// Connections and warm-up, not timed
	for (const task of queue(WARMUP_TASKS)) {
		await run(task);
	}
	for (let round = 0; round < ROUNDS; round++) {
		const spent = new Map<string, number>();
		for (const task of queue(TASKS)) {
			const start = performance.now();
			await run(task);
			const name = spentName(task.library, task.kind);
			spent.set(name, (spent.get(name) ?? 0) + performance.now() - start);
		}
		rounds.push(spent);
	}
} finally {
	for (const library of libraries) {
		await library.close();
	}
}

/** The median over the rounds of a library's time on these kinds of task. */
function time(library: Library, kinds: Kind[]): number {
	const values = rounds
		.map((spent) =>
			kinds.reduce(
				(sum, kind) => sum + (spent.get(spentName(library, kind)) ?? 0),
				0,
			),
		)
		.sort((a, b) => a - b);
	return values[Math.floor(values.length / 2)];
}

const tasks = TASKS.toLocaleString("en-US");
const rows = [
	{
		name: `${tasks} × set, get, delete 1 key`,
		values: libraries.map((library) => time(library, ["single"])),
	},
	{
		name: `${tasks} × set, get, delete ${MULTI_KEYS} keys`,
		values: libraries.map((library) => time(library, ["multi"])),
	},
	{
		name: `All ${(2 * TASKS).toLocaleString("en-US")} tasks`,
		values: libraries.map((library) => time(library, ["single", "multi"])),
	},
];

console.log("");
console.log("## Set, Get and Delete (one task at a time, in random order)");
console.log("");
console.log(
	`Each library runs ${tasks} tasks that set, get and delete one key, and ${tasks} that set, get and delete ${MULTI_KEYS} keys. The tasks of all ${libraries.length} libraries go into one queue in random order and run one at a time. The queue runs ${ROUNDS} times, shuffled each time, after an untimed warm-up of ${WARMUP_TASKS} tasks of each kind per library. Every result is checked.`,
);
console.log("");
console.log(
	`Total time per library, the median of the ${ROUNDS} runs (lower is better). 🥇 marks the fastest library in each row, and the percentages compare the others with it. The text protocol has no multi-set or multi-delete, so for ${MULTI_KEYS} keys every library sends its ${MULTI_KEYS} sets, and then its ${MULTI_KEYS} deletes, at once. memjs has no multi-get either, so it does the same with its gets. memcached pools up to 10 connections; the other clients use one.`,
);
console.log("");
console.log(
	comparisonTable(
		"Tasks",
		libraries.map((library) => library.label),
		rows,
		duration,
		(values) => Math.min(...values),
	),
);
console.log("");
