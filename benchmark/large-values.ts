import { tinybenchPrinter } from "@monstermann/tinybench-pretty-printer";
import { Bench } from "tinybench";
import type { Memcache } from "../src/index.js";
import { createClient, MB, SKIP_TLS, setAll } from "./utils.js";

// Every row moves the same 4 MB, as values of different sizes, over TCP and
// TLS: the gets read it and the sets write it.
const TOTAL = 4 * MB;
const bench = new Bench({
	name: "Large Values (4 MB per operation)",
	iterations: 16,
	warmupIterations: 4,
	throws: true,
});

const sizes = [256 * 1024, MB, 4 * MB];
const clients: Memcache[] = [];

for (const secure of SKIP_TLS ? [false] : [false, true]) {
	const client = createClient(secure);
	await client.connect();
	clients.push(client);
	for (const size of sizes) {
		const count = TOTAL / size;
		const keys = Array.from(
			{ length: count },
			(_, i) => `bench:large:${size}:${i}`,
		);
		const value = "v".repeat(size);
		await setAll(client, keys, value);
		const label = size >= MB ? `${size / MB} MB` : `${size / 1024} KB`;
		const name = `${secure ? "TLS" : "TCP"}: ${count} × ${label}`;
		bench.add(`${name} gets`, async () => {
			for (const key of keys) {
				if ((await client.get(key))?.length !== size) {
					throw new Error(`get() returned the wrong value for ${key}`);
				}
			}
		});
		bench.add(`${name} sets`, async () => {
			for (const key of keys) {
				if (!(await client.set(key, value))) {
					throw new Error(`set() failed for ${key}`);
				}
			}
		});
	}
}

try {
	await bench.run();
} finally {
	await Promise.all(clients.map((client) => client.disconnect()));
}

const cli = tinybenchPrinter.toMarkdown(bench);
console.log("");
console.log(`## ${bench.name}`);
console.log(cli);
console.log("");
