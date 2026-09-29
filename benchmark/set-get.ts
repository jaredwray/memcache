import { faker } from "@faker-js/faker";
import { tinybenchPrinter } from "@monstermann/tinybench-pretty-printer";
import Memcached from "memcached";
import memjs from "memjs";
import { Bench } from "tinybench";
import pkg from "../package.json" with { type: "json" };
import { cleanVersion, createClient, HOST, PORT } from "./utils.js";

const memcachedVersion = cleanVersion(pkg.devDependencies.memcached);
const memjsVersion = cleanVersion(pkg.devDependencies.memjs);

const ITERATIONS = 10_000;
const bench = new Bench({
	name: "Set and Get (one request at a time)",
	iterations: ITERATIONS,
	throws: true,
});

// Clients
const memcacheClient = createClient();
const memjsClient = memjs.Client.create(`${HOST}:${PORT}`);
const memcachedClient = new Memcached(`${HOST}:${PORT}`);

// Promisify memcached (callback-based)
function memcachedSet(key: string, value: string): Promise<boolean> {
	return new Promise((resolve, reject) => {
		memcachedClient.set(key, value, 0, (err) => {
			if (err) reject(err);
			else resolve(true);
		});
	});
}

function memcachedGet(key: string): Promise<string | undefined> {
	return new Promise((resolve, reject) => {
		memcachedClient.get(key, (err, data) => {
			if (err) reject(err);
			else resolve(data as string | undefined);
		});
	});
}

function check(actual: string | undefined, expected: string, client: string) {
	if (actual !== expected) {
		throw new Error(`${client} get returned the wrong value`);
	}
}

// Pre-generate keys and values
const keys = Array.from(
	{ length: ITERATIONS },
	(_, i) => `bench-${i}-${faker.string.alphanumeric(8)}`,
);
const values = Array.from({ length: ITERATIONS }, () => faker.lorem.word());

await memcacheClient.connect();

let memcacheIndex = 0;
bench.add(`${pkg.name} set/get (v${pkg.version})`, async () => {
	const i = memcacheIndex % keys.length;
	await memcacheClient.set(keys[i], values[i]);
	check(await memcacheClient.get(keys[i]), values[i], pkg.name);
	memcacheIndex++;
});

let memjsIndex = 0;
bench.add(`memjs set/get (v${memjsVersion})`, async () => {
	const i = memjsIndex % keys.length;
	await memjsClient.set(keys[i], values[i], { expires: 0 });
	const { value } = await memjsClient.get(keys[i]);
	check(value?.toString(), values[i], "memjs");
	memjsIndex++;
});

let memcachedIndex = 0;
bench.add(`memcached set/get (v${memcachedVersion})`, async () => {
	const i = memcachedIndex % keys.length;
	await memcachedSet(keys[i], values[i]);
	check(await memcachedGet(keys[i]), values[i], "memcached");
	memcachedIndex++;
});

try {
	await bench.run();
} finally {
	await memcacheClient.disconnect();
	memjsClient.close();
	memcachedClient.end();
}

const cli = tinybenchPrinter.toMarkdown(bench);
console.log("");
console.log(`## ${bench.name}`);
console.log(cli);
console.log("");
