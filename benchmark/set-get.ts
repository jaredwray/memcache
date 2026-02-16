import { faker } from "@faker-js/faker";
import { tinybenchPrinter } from "@monstermann/tinybench-pretty-printer";
import Memcached from "memcached";
import memjs from "memjs";
import { Bench } from "tinybench";
import pkg from "../package.json" with { type: "json" };
import { Memcache } from "../src/index.js";

function cleanVersion(version: string): string {
	return version.replace(/^\D*/, "").replace(/[^\d.]*$/, "");
}

const memcachedVersion = cleanVersion(pkg.devDependencies.memcached);
const memjsVersion = cleanVersion(pkg.devDependencies.memjs);

const bench = new Bench({ name: "set-get", iterations: 10_000 });

// Clients
const memcacheClient = new Memcache("localhost:11211");
const memjsClient = memjs.Client.create("localhost:11211");
const memcachedClient = new Memcached("localhost:11211");

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

// Pre-generate keys and values
const keys = Array.from(
	{ length: 10_000 },
	(_, i) => `bench-${i}-${faker.string.alphanumeric(8)}`,
);
const values = Array.from({ length: 10_000 }, () => faker.lorem.word());

await memcacheClient.connect();

let memcacheIndex = 0;
bench.add(`${pkg.name} set/get (v${pkg.version})`, async () => {
	const i = memcacheIndex % keys.length;
	await memcacheClient.set(keys[i], values[i]);
	await memcacheClient.get(keys[i]);
	memcacheIndex++;
});

let memjsIndex = 0;
bench.add(`memjs set/get (v${memjsVersion})`, async () => {
	const i = memjsIndex % keys.length;
	await memjsClient.set(keys[i], values[i], { expires: 0 });
	await memjsClient.get(keys[i]);
	memjsIndex++;
});

let memcachedIndex = 0;
bench.add(`memcached set/get (v${memcachedVersion})`, async () => {
	const i = memcachedIndex % keys.length;
	await memcachedSet(keys[i], values[i]);
	await memcachedGet(keys[i]);
	memcachedIndex++;
});

await bench.run();

const output = tinybenchPrinter.toMarkdown(bench);
console.log(output);
console.log("");

await memcacheClient.flush();
await memcacheClient.disconnect();
memjsClient.close();
memcachedClient.end();
