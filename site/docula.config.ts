import type { DoculaConsole, DoculaOptions } from "docula";

export const options: Partial<DoculaOptions> = {
	githubPath: "jaredwray/memcache",
	siteTitle: "Memcache",
	siteDescription: "Node.js Memcache Client",
	siteUrl: "https://memcachejs.org",
	output: "./site/dist",
	sitePath: "./site",
	themeMode: "light",
	autoReadme: true,
	enableReleaseChangelog: true,
	enableLlmsTxt: true,
};

// The docula template already renders the site logo in the header. The project
// README.md also opens with a logo image, which `autoReadme` would render a
// second time in the home page body — producing a duplicate logo. Strip that
// leading logo from the rendered README so only the header logo remains.
//
// README.md on disk is left untouched, so the logo still shows on GitHub and npm.
export const onAutoReadme = (
	content: string,
	sourcePath: string,
	console: DoculaConsole,
): string => {
	console.info(`Cleaning up README at ${sourcePath}`);
	return content.replace(
		/^(#\s+[^\n]*\r?\n\s*)?(?:\[<img[^>]+>\]\([^)]+\)|!\[[^\]]*\]\([^)]+\))[ \t]*\r?\n+/,
		"$1",
	);
};
