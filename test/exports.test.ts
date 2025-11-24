import { describe, expect, it } from "vitest";
import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const require = createRequire(import.meta.url);

describe("Package Exports", () => {
	it("should export main class from ESM", async () => {
		// Import from the built package
		const module = await import("../dist/index.js");

		expect(module).toBeDefined();
		expect(module.default).toBeDefined();
		expect(typeof module.default).toBe("function");
	});

	it("should export named exports from ESM", async () => {
		const module = await import("../dist/index.js");

		expect(module.createNode).toBeDefined();
		expect(typeof module.createNode).toBe("function");
	});

	it("should have valid TypeScript declarations for ESM", () => {
		const dtsPath = join(__dirname, "../dist/index.d.ts");
		const dtsContent = readFileSync(dtsPath, "utf-8");

		expect(dtsContent).toBeDefined();
		expect(dtsContent.length).toBeGreaterThan(0);
		// Check for key exports
		expect(dtsContent).toContain("export");
		expect(dtsContent).toContain("Memcache");
	});

	it("should export main class from CommonJS", () => {
		// Use require for CommonJS
		const module = require("../dist/index.cjs");

		expect(module).toBeDefined();
		expect(module.default).toBeDefined();
		expect(typeof module.default).toBe("function");
	});

	it("should export named exports from CommonJS", () => {
		const module = require("../dist/index.cjs");

		expect(module.createNode).toBeDefined();
		expect(typeof module.createNode).toBe("function");
	});

	it("should have valid TypeScript declarations for CommonJS", () => {
		const dctsPath = join(__dirname, "../dist/index.d.cts");
		const dctsContent = readFileSync(dctsPath, "utf-8");

		expect(dctsContent).toBeDefined();
		expect(dctsContent.length).toBeGreaterThan(0);
		// Check for key exports
		expect(dctsContent).toContain("export");
		expect(dctsContent).toContain("Memcache");
	});

	it("should have matching type declarations between ESM and CommonJS", () => {
		const dtsPath = join(__dirname, "../dist/index.d.ts");
		const dctsPath = join(__dirname, "../dist/index.d.cts");

		const dtsContent = readFileSync(dtsPath, "utf-8");
		const dctsContent = readFileSync(dctsPath, "utf-8");

		// Both files should have the same content
		expect(dtsContent).toBe(dctsContent);
	});

	it("should validate package.json exports configuration", () => {
		const packageJsonPath = join(__dirname, "../package.json");
		const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf-8"));

		// Check main fields
		expect(packageJson.main).toBe("dist/index.js");
		expect(packageJson.types).toBe("dist/index.d.ts");
		expect(packageJson.type).toBe("module");

		// Check exports
		expect(packageJson.exports).toBeDefined();
		expect(packageJson.exports["."]).toBeDefined();

		// Check import condition
		expect(packageJson.exports["."].import).toBeDefined();
		expect(packageJson.exports["."].import.types).toBe("./dist/index.d.ts");
		expect(packageJson.exports["."].import.default).toBe("./dist/index.js");

		// Check require condition
		expect(packageJson.exports["."].require).toBeDefined();
		expect(packageJson.exports["."].require.types).toBe("./dist/index.d.cts");
		expect(packageJson.exports["."].require.default).toBe("./dist/index.cjs");
	});
});
