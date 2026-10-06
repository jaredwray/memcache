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
- `pnpm test:services:stop` - Stop test services (and the benchmark services, if running)
- `pnpm benchmark:services:start` / `pnpm benchmark:services:stop` - Start or stop the memcached containers used by the benchmarks
- `pnpm benchmark` - Run all benchmarks (each also runs alone, e.g. `pnpm benchmark:multi-get`)
- `pnpm benchmark:readme` - Run the benchmarks and update the README tables (pass section ids, e.g. `pnpm benchmark:readme compare`, to update only those)
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
  - Then `pnpm test:services:start` (docker compose) brings up memcached on ports `11211`, `11212`, `11213`, a server reserved for flush tests on `11214`, a SASL server on `11215`, a TLS-only server on `21211`, and a TLS+SASL server on `21215`. `pnpm test` / `pnpm test:ci` need these running or most suites fail.
- Docker note: the daemon is configured with the `fuse-overlayfs` storage driver and `containerd-snapshotter` disabled (required for Docker 29 in this VM). This is already set in `/etc/docker/daemon.json`.
- Known environment-only test failures: the two `should handle connection timeout` tests (`test/index.test.ts`, `test/node.test.ts`) fail here because outbound TCP to the reserved TEST-NET-1 address `192.0.2.0` connects instantly in this sandbox instead of timing out. This is a network-environment quirk, not a code bug; these pass on GitHub CI. All other tests (610) pass.

## Safe Chain

Aikido Safe Chain shims examine each package that a package manager installs in this environment.
Never bypass the shims.

- Keep `~/.safe-chain/shims` first on `PATH`.
- Run `npm`, `npx`, `pnpm`, and `pnpx` only through the shims. Do not run a different copy by its
  full path.
- Do not install a package with `curl | sh` or with a package manager that has no shim.
- If Safe Chain blocks a package, stop. Do not use a different command, path, or package to get the
  same code. Tell the user the package name and the Safe Chain message.

## Simplified Technical English

Write in ASD-STE100 Simplified Technical English (STE). Get the current issue of the specification
free of charge from <https://www.asd-ste100.org>. STE applies to all text that you write for this
repository, its pull requests, and its issues. This text includes documentation, code comments,
commit messages, review replies, and changelog entries.

- Use approved STE words with their approved meanings. You can also use technical names and
  technical verbs. If you cannot confirm that a word is approved, use a short, common word with one
  meaning.
- Use one word for one meaning.
- Write an instruction in the imperative. Write one instruction in each sentence.
- Do not write more than 20 words in an instruction or 25 words in a descriptive sentence.
- Use the active voice.
- Use only the simple present, simple past, or simple future tense. Do not use the present perfect.
- Do not use the "-ing" form of a verb, except in a technical name.
- Do not write a noun cluster of more than three words.
- Do not omit articles, verbs, or subjects to make a sentence shorter.
- Write one topic in each paragraph. Do not write more than six sentences in a paragraph.
- Use a vertical list for steps, conditions, and other complex text.
- Do not change code, commands, identifiers, file paths, or quoted text to make them STE.
- Use STE for the text that you add or change. Do not rewrite other text only to make it STE.

## Test audit

Apply this gate to each test that a pull request adds, changes, or deletes. Apply it before you open
or update the pull request. This gate is the authoring gate of the `test-audit` skill in
`jaredwray/agentic`. If that skill is installed, use it.

Keep a new or changed test only if you can answer all four questions:

1. Which observable behavior, invariant, or contract does the test protect?
2. Which credible regression makes the test fail?
3. Why does the current coverage not find that regression? If a test or its table owns the
   contract, extend it. Do not add a near-duplicate test.
4. Does the test need an export, flag, or hook that no production caller uses? If yes, test at the
   real boundary.

- A regression test for a bug fix must fail on the code before the fix, for the intended reason.
- Do not keep a test that asserts nothing, restates the implementation, repeats the type checker,
  or only proves a mock.
- A coverage target does not lower this bar. Reach an uncovered line through its public entry
  point. If no caller can reach a branch, remove the branch. Do not add a test for it.
- Delete a test only when a different test proves its contract, or when the contract is gone. Do
  not delete a test because it fails.
- Record the gate in the verification list of the pull request body. List each test that you did
  not keep, rewrote, or deleted, and give the reason. For a deleted test, name the test that proves
  its contract, or show that the contract is gone.
- Do not audit tests that this pull request does not touch. Audit them in a separate pull request.

## Pull requests

The task does not stop when you open a pull request. Do these steps before you start a different
task:

1. Wait approximately 20 minutes for automated and human code reviews.
2. Read each new comment. Find each comment that has a finding, a question, or a change request.
3. Examine the code for each finding or change request. Decide if it is correct. Do not decide from
   who the reviewer is.
4. If it is correct, change the code. Run the same checks that CI runs. Push the change. On the
   thread, reply with what changed and the commit SHA.
5. If it is not correct, reply on the thread with the reason. Give the file and line. Do not
   resolve the thread. The reviewer closes it.
6. If a comment asks a question, answer it on the thread.
7. If CI fails, find the root cause and fix it. Do not skip or disable a test to make CI pass.
8. If you pushed a change or CI did not finish, do steps 1 to 7 again. Stop when CI passes and each
   finding, question, and change request has a reply.

Do not reply to a comment that needs no answer: your own replies, approvals, thanks, and bot status
notices. A reply to one of these comments starts the loop again.
