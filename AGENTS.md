# AGENTS.md

Guidelines for AI coding agents (Claude, Gemini, Codex).

## Mandatory with all changes

- `pnpm build` must be successful
- `pnpm test` must be successful with 100% code coverage

## Commands

- `pnpm install` - Install dependencies
- `pnpm build` - Build for production (ESM + CJS + type definitions)
- `pnpm lint` - Run Biome linter with auto-fix
- `pnpm test` - Run linter and Vitest with coverage
- `pnpm test:ci` - CI-specific testing (strict linting + coverage)
- `pnpm test:services:start` - Start Docker memcached (required for integration tests)
- `pnpm test:services:stop` - Stop test services
- `pnpm clean` - Remove node_modules, coverage, and dist directories

**Use pnpm, not npm.**

## Development Rules

1. **Start test services first** - Run `pnpm test:services:start` before running tests
2. **Always run `pnpm build` before committing** - Build must succeed
3. **Always run `pnpm test` before committing** - All tests must pass
4. **Follow existing code style** - Biome enforces formatting and linting
5. **Mirror source structure in tests** - Test files go in `test/` matching `src/` structure

## Structure

- `src/index.ts` - Main Memcache client class with all protocol operations
- `src/node.ts` - MemcacheNode class for single server TCP connections
- `src/ketama.ts` - Consistent hashing implementation (Ketama algorithm)
- `test/` - Test files (Vitest)

## Cursor Cloud specific instructions

This is a pure Node.js library (no long-running app server); "running" it means building and exercising the client against local memcached. Standard commands live in the `## Commands` section above and in `package.json`.

- Node version: the repo requires Node `>=22.19.0`. The system node at `/exec-daemon/node` is too old; `~/.bashrc` is configured to prioritize nvm's default Node 24 and `pnpm` comes from corepack. Login shells (the default) already resolve the correct `node`/`pnpm`, so no manual `nvm use` is needed.
- Docker is required for tests but the daemon does NOT auto-start. Before running integration tests, start it once per session and make the socket usable by the repo's non-sudo scripts:
  - `sudo dockerd > /tmp/dockerd.log 2>&1 &`
  - `sudo chmod 666 /var/run/docker.sock`
  - Then `pnpm test:services:start` (docker compose) brings up memcached on ports `11211`, `11212`, `11213` and a SASL server on `11215`. `pnpm test` / `pnpm test:ci` need these running or most suites fail.
- Docker note: the daemon is configured with the `fuse-overlayfs` storage driver and `containerd-snapshotter` disabled (required for Docker 29 in this VM). This is already set in `/etc/docker/daemon.json`.
- Known environment-only test failures: the two `should handle connection timeout` tests (`test/index.test.ts`, `test/node.test.ts`) fail here because outbound TCP to the reserved TEST-NET-1 address `192.0.2.0` connects instantly in this sandbox instead of timing out. This is a network-environment quirk, not a code bug; these pass on GitHub CI. All other tests (610) pass.

## Safe Chain

Package installs in this environment go through Aikido Safe Chain shims. Never bypass them:

- Keep `~/.safe-chain/shims` first on `PATH`.
- Do not call unshimmed `npm`, `pnpm`, `npx`, or `pnpx`.
- Do not install packages with `curl | sh` or by pointing at a package manager outside the shim directory.
