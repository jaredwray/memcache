import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import type { DoculaConsole, DoculaOptions } from "docula";

export const options: Partial<DoculaOptions> = {
	githubPath: "jaredwray/memcache",
	siteTitle: "Memcache",
	siteDescription: "Node.js Memcache Client",
	siteUrl: "https://memcachejs.org",
	output: "./site/dist",
	sitePath: "./site",
	themeMode: "light",
	// Keep the branded hero + Documentation CTA on `/`. Docs live at `/docs/`.
	autoReadme: false,
	enableReleaseChangelog: true,
	enableLlmsTxt: true,
	enableSearch: true,
	editPageUrl: "https://github.com/jaredwray/memcache/blob/main/site/docs",
	sections: [{ name: "Project Guidelines", path: "project-guidelines", order: 20 }],
	headerLinks: [
		{
			label: "GitHub",
			url: "https://github.com/jaredwray/memcache",
			icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.17 6.839 9.49.5.092.682-.217.682-.482 0-.237-.008-.866-.013-1.7-2.782.603-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.463-1.11-1.463-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.646.35-1.086.636-1.336-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.269 2.75 1.025A9.578 9.578 0 0 1 12 6.836c.85.004 1.705.114 2.504.336 1.909-1.294 2.747-1.025 2.747-1.025.546 1.377.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.167 22 16.418 22 12c0-5.523-4.477-10-10-10z"/></svg>',
		},
		{
			label: "npm",
			url: "https://www.npmjs.com/package/memcache",
		},
	],
};

async function copyWithFrontMatter(
	sourcePath: string,
	destPath: string,
	title: string,
	order: number,
): Promise<void> {
	const content = await fs.promises.readFile(sourcePath, "utf8");
	const frontMatter = `---\ntitle: ${title}\norder: ${order}\n---\n\n`;
	await fs.promises.writeFile(destPath, frontMatter + content);
}

export const onPrepare = async (
	config: DoculaOptions,
	console: DoculaConsole,
): Promise<void> => {
	console.step("Preparing project guidelines...");
	const guidelinesDir = path.join(config.sitePath, "docs", "project-guidelines");
	await fs.promises.mkdir(guidelinesDir, { recursive: true });

	const rootDir = process.cwd();

	await Promise.all([
		copyWithFrontMatter(
			path.join(rootDir, "CONTRIBUTING.md"),
			path.join(guidelinesDir, "contributing.md"),
			"Contributing",
			1,
		),
		copyWithFrontMatter(
			path.join(rootDir, "CODE_OF_CONDUCT.md"),
			path.join(guidelinesDir, "code-of-conduct.md"),
			"Code of Conduct",
			2,
		),
		copyWithFrontMatter(
			path.join(rootDir, "SECURITY.md"),
			path.join(guidelinesDir, "security.md"),
			"Security",
			3,
		),
	]);

	console.success("Project guidelines prepared");
};
