import { spawnSync } from "node:child_process";
import fs from "node:fs";

type BenchmarkSection = {
	id: string;
	script: string;
};

const SECTIONS: BenchmarkSection[] = [
	{ id: "set-get", script: "benchmark/set-get.ts" },
	{ id: "compare", script: "benchmark/compare.ts" },
	{ id: "concurrency", script: "benchmark/concurrency.ts" },
	{ id: "multi-get", script: "benchmark/multi-get.ts" },
	{ id: "large-values", script: "benchmark/large-values.ts" },
	{ id: "bursts", script: "benchmark/bursts.ts" },
	{ id: "cold-start", script: "benchmark/cold-start.ts" },
];

const README_PATH = "README.md";

function runBenchmark(script: string): string {
	const result = spawnSync("pnpm", ["exec", "tsx", script], {
		encoding: "utf8",
		stdio: ["inherit", "pipe", "inherit"],
		shell: true,
	});
	if (result.error) {
		throw result.error;
	}
	if (result.status !== 0) {
		throw new Error(`${script} exited with status ${result.status}`);
	}
	return result.stdout.trim();
}

function replaceSection(readme: string, id: string, content: string): string {
	const startMarker = `<!-- BENCHMARK:${id}:START -->`;
	const endMarker = `<!-- BENCHMARK:${id}:END -->`;
	const startIdx = readme.indexOf(startMarker);
	const endIdx = readme.indexOf(endMarker);
	if (startIdx === -1 || endIdx === -1) {
		throw new Error(
			`Markers ${startMarker}/${endMarker} not found in ${README_PATH}`,
		);
	}
	const before = readme.slice(0, startIdx + startMarker.length);
	const after = readme.slice(endIdx);
	return `${before}\n${content}\n${after}`;
}

// Section ids on the command line update only those sections
const only = process.argv.slice(2);
const unknown = only.filter((id) => !SECTIONS.some((s) => s.id === id));
if (unknown.length > 0) {
	throw new Error(`Unknown benchmark section: ${unknown.join(", ")}`);
}
const selected =
	only.length > 0 ? SECTIONS.filter((s) => only.includes(s.id)) : SECTIONS;

let readme = fs.readFileSync(README_PATH, "utf8");

for (const { id, script } of selected) {
	console.log(`\n▶ ${script}`);
	const output = runBenchmark(script);
	process.stdout.write(`${output}\n`);
	readme = replaceSection(readme, id, output);
}

fs.writeFileSync(README_PATH, readme);
console.log(`\n✅ Updated benchmark sections in ${README_PATH}`);
