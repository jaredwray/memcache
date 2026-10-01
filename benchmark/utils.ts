import { readFileSync } from "node:fs";
import { isIP } from "node:net";
import { Memcache } from "../src/index.js";

export function cleanVersion(version: string): string {
	return version.replace(/^\D*/, "").replace(/[^\d.]*$/, "");
}

/**
 * Benchmark targets. The defaults are the compose `bench` profile
 * (`pnpm benchmark:services:start`). On Linux, published Docker ports go
 * through docker-proxy, which adds per-packet overhead; for numbers closer to
 * a real network, point these at the container IPs with port 11211.
 */
export const HOST = process.env.MEMCACHE_BENCH_HOST ?? "localhost";
export const PORT = Number(process.env.MEMCACHE_BENCH_PORT ?? "11216");
export const TLS_HOST = process.env.MEMCACHE_BENCH_TLS_HOST ?? "localhost";
export const TLS_PORT = Number(process.env.MEMCACHE_BENCH_TLS_PORT ?? "21216");
/** Set to "1" to leave out TLS measurements (for servers without TLS). */
export const SKIP_TLS = process.env.MEMCACHE_BENCH_SKIP_TLS === "1";

export const MB = 1024 * 1024;

/**
 * The defaults fit the bench TLS container, which uses the test certificate
 * (issued for "localhost"), so IP targets are verified against that name. For
 * a server with its own certificate, set MEMCACHE_BENCH_TLS_CA to its CA
 * bundle, and MEMCACHE_BENCH_TLS_SERVERNAME if the name differs from the host.
 */
function tlsOptions() {
	return {
		ca: readFileSync(
			process.env.MEMCACHE_BENCH_TLS_CA ??
				new URL("../test/certs/cacert.pem", import.meta.url),
		),
		servername:
			process.env.MEMCACHE_BENCH_TLS_SERVERNAME ??
			(isIP(TLS_HOST) ? "localhost" : TLS_HOST),
	};
}

/**
 * A client for the benchmark server. The long timeout keeps idle connections
 * open between tasks, so reconnects don't land inside measurements.
 */
export function createClient(secure = false): Memcache {
	return new Memcache({
		nodes: [secure ? `${TLS_HOST}:${TLS_PORT}` : `${HOST}:${PORT}`],
		timeout: 60_000,
		maxValueSize: 32 * MB,
		tls: secure ? tlsOptions() : undefined,
	});
}

export async function setAll(
	client: Memcache,
	keys: string[],
	value: string,
): Promise<void> {
	const results = await Promise.all(keys.map((key) => client.set(key, value)));
	if (results.includes(false)) {
		throw new Error("Failed to store benchmark keys");
	}
}

/**
 * A markdown table comparing clients, one column each. The best value in
 * each row gets a medal; every other cell shows how far its value is from
 * the best, so the sign follows the unit (-46% fewer requests per second,
 * +309% more time).
 */
export function comparisonTable(
	header: string,
	columns: string[],
	rows: Array<{ name: string; values: number[] }>,
	format: (value: number) => string,
	best: (values: number[]) => number,
): string {
	const lines = [
		`| ${header} | ${columns.join(" | ")} |`,
		`|---|${columns.map(() => "--:").join("|")}|`,
	];
	for (const { name, values } of rows) {
		const winner = best(values);
		const cells = values.map((v) =>
			v === winner
				? `🥇 **${format(v)}**`
				: `${format(v)} (${percent((v / winner - 1) * 100)})`,
		);
		lines.push(`| ${name} | ${cells.join(" | ")} |`);
	}
	return lines.join("\n");
}

function percent(change: number): string {
	const size = Math.abs(change);
	const digits = size < 10 ? size.toFixed(1) : Math.round(size).toString();
	return `${change < 0 ? "-" : "+"}${digits}%`;
}

export const duration = (ms: number) =>
	ms >= 1000
		? `${(ms / 1000).toFixed(2)} s`
		: `${ms >= 10 ? Math.round(ms) : ms.toFixed(1)} ms`;
