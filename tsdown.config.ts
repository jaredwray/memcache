import {defineConfig} from 'tsdown';

export default defineConfig({
	entry: ['src/index.ts'],
	format: ['cjs', 'esm'],
	dts: true,
	clean: true,
	inputOptions: {
		onLog(level, log, defaultHandler) {
			if (log.code === 'MIXED_EXPORTS') {
				return;
			}
			defaultHandler(level, log);
		},
	},
});
