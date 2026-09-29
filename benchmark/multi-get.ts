import { tinybenchPrinter } from "@monstermann/tinybench-pretty-printer";
import { Bench } from "tinybench";
import { createClient, setAll } from "./utils.js";

// Every row fetches the same 10,000 keys, in batches of different sizes.
// Keys share a long prefix, like real namespaced keys.
const TOTAL_KEYS = 10_000;
const bench = new Bench({
	name: `Multi-Get (${TOTAL_KEYS.toLocaleString("en-US")} keys per operation)`,
	iterations: 3,
	warmupIterations: 1,
	time: 3000,
	throws: true,
});

const client = createClient();
await client.connect();
const keys = Array.from(
	{ length: TOTAL_KEYS },
	(_, i) => `bench:user:profile:${i}`,
);
await setAll(client, keys, "0123456789");

for (const size of [100, 1_000, 10_000]) {
	const batches = Array.from({ length: TOTAL_KEYS / size }, (_, i) =>
		keys.slice(i * size, (i + 1) * size),
	);
	bench.add(
		`${batches.length} × gets() of ${size.toLocaleString("en-US")} keys`,
		async () => {
			for (const batch of batches) {
				const values = await client.gets(batch);
				if (values.size !== batch.length) {
					throw new Error(
						`gets() returned ${values.size} of ${batch.length} keys`,
					);
				}
			}
		},
	);
}

try {
	await bench.run();
} finally {
	await client.disconnect();
}

const cli = tinybenchPrinter.toMarkdown(bench);
console.log("");
console.log(`## ${bench.name}`);
console.log(cli);
console.log("");
