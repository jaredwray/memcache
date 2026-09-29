import { tinybenchPrinter } from "@monstermann/tinybench-pretty-printer";
import { Bench } from "tinybench";
import { createClient, setAll } from "./utils.js";

// Every row sends the same 500 requests with a different number in flight,
// like 1 to 500 concurrent request handlers sharing one client.
const REQUESTS = 500;
const bench = new Bench({
	name: `Concurrent Requests (${REQUESTS} per operation)`,
	throws: true,
});

const client = createClient();
await client.connect();
const value = "x".repeat(100);
const keys = Array.from({ length: 1000 }, (_, i) => `bench:concurrency:${i}`);
await setAll(client, keys, value);

async function send(inFlight: number, request: (key: string) => Promise<void>) {
	let next = 0;
	const worker = async () => {
		while (next < REQUESTS) {
			await request(keys[next++ % keys.length]);
		}
	};
	await Promise.all(Array.from({ length: inFlight }, worker));
}

async function get(key: string) {
	if ((await client.get(key)) !== value) {
		throw new Error("get() returned the wrong value");
	}
}

async function set(key: string) {
	if (!(await client.set(key, value))) {
		throw new Error("set() failed");
	}
}

for (const [name, request] of [
	["gets", get],
	["sets", set],
] as const) {
	for (const inFlight of [1, 10, 100, 500]) {
		bench.add(`${REQUESTS} ${name}, ${inFlight} in flight`, () =>
			send(inFlight, request),
		);
	}
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
