import { tinybenchPrinter } from "@monstermann/tinybench-pretty-printer";
import { Bench } from "tinybench";
import { createClient, setAll } from "./utils.js";

// Every row sends the same 60,000 gets, as bursts of different sizes fired
// all at once (for example, a batch job or a traffic spike).
const TOTAL = 60_000;
const bench = new Bench({
	name: `Bursts (${TOTAL.toLocaleString("en-US")} gets per operation)`,
	iterations: 3,
	warmupIterations: 1,
	time: 5000,
	throws: true,
});

const client = createClient();
await client.connect();
const value = "0123456789";
const keys = Array.from({ length: 1000 }, (_, i) => `bench:burst:${i}`);
await setAll(client, keys, value);

for (const burst of [10_000, 30_000, 60_000]) {
	bench.add(
		`${TOTAL / burst} × ${burst.toLocaleString("en-US")} concurrent gets`,
		async () => {
			for (let sent = 0; sent < TOTAL; sent += burst) {
				const values = await Promise.all(
					Array.from({ length: burst }, (_, i) =>
						client.get(keys[i % keys.length]),
					),
				);
				if (values.some((result) => result !== value)) {
					throw new Error("get() returned the wrong value during a burst");
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
