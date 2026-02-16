import { tinybenchPrinter } from "@monstermann/tinybench-pretty-printer";
import { faker } from "@faker-js/faker";
import { Bench } from "tinybench";
import { Memcache } from "../src/index.js";
import pkg from "../package.json" with { type: "json" };

const bench = new Bench({ name: "set-get", iterations: 10_000 });
const client = new Memcache("localhost:11211");

// Pre-generate keys and values
const keys = Array.from({ length: 10_000 }, (_, i) => `bench-${i}-${faker.string.alphanumeric(8)}`);
const values = Array.from({ length: 10_000 }, () => faker.lorem.word());

await client.connect();

let index = 0;
bench.add(`${pkg.name} set/get (v${pkg.version})`, async () => {
	const i = index % keys.length;
	await client.set(keys[i], values[i]);
	await client.get(keys[i]);
	index++;
});

await bench.run();

const output = tinybenchPrinter.toMarkdown(bench);
console.log(output);
console.log("");

await client.flush();
await client.disconnect();
