import { tinybenchPrinter } from "@monstermann/tinybench-pretty-printer";
import { Bench } from "tinybench";
import type { Memcache } from "../src/index.js";
import { createClient, MB, SKIP_TLS, setAll } from "./utils.js";

// Every row reads the same 4 MB, as values of different sizes, over TCP and TLS.
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
		await setAll(client, keys, "v".repeat(size));
		const label = size >= MB ? `${size / MB} MB` : `${size / 1024} KB`;
		bench.add(`${secure ? "TLS" : "TCP"}: ${count} × ${label}`, async () => {
			for (const key of keys) {
				if ((await client.get(key))?.length !== size) {
					throw new Error(`get() returned the wrong value for ${key}`);
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
