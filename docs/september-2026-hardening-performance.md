# September 2026 Hardening & Performance Plan

**Status:** Proposed · **Created:** 2026-09-29 · **Scope:** `src/node.ts`, `src/index.ts`, `src/binary-protocol.ts`, `src/ketama.ts`, tests, benchmarks, README

This plan turns a full review of the client, plus an independent second audit, into a sequence of small changes that can each be merged on its own. Unless marked as code reading, every finding below was reproduced against a real memcached 1.6.45 server using the real dependencies. Where a prototype of the fix was built, the numbers compare the current code with that prototype.

Line numbers refer to `main` when the plan was written (commit `2a55872`); they drift as items land.

## Summary

| ID | Issue | Measured on real memcached 1.6.45 | Size |
|---|---|---|---|
| H1 | Concurrent SASL/binary requests receive each other's responses | 3 concurrent `binaryGet`s for `a`, `b`, `c` all returned `a`'s value | M |
| H2 | Concurrent connects open one socket each; stale sockets act on the live one | 50 concurrent cold requests opened 50 sockets; +49 leaked per idle→busy cycle; in 2 of 6 runs a request never settled, in 3 of 6 a request returned `undefined` for a key that exists | M |
| H3 | `timeout` is an idle timer: it drops idle connections and never fires for a stalled server while the client keeps writing | idle connections dropped after `timeout`; with a write every 100 ms, a request to a stalled server was still pending after 3 s (`timeout: 500`) | M |
| P1 | One `write()` syscall per command | +17% throughput at 10 in flight, +85% at 100, +115% at 500 | S |
| P2 | O(requested × found) multi-get miss check inside the data handler | 10k-key `gets()`: 1,722 → 38 ms | XS |
| P3 | Whole buffer re-copied on every chunk of a large value | TCP 16 MB: 517 → 48 ms; TLS 4 MB: 110 → 9.5 ms | S |
| P4 | `Array.shift()` queue degrades to O(n) when deep | burst of 100k gets: 18.1 → 0.84 s | S |

Supporting work: T1 (flaky shared-server tests), B1 (benchmarks that can see these problems), N1–N6 (next-tier optimizations), R1 (docs and release).

## Ground rules for every item

- One PR per item ID, in the order below unless noted. Each PR can be merged and reverted on its own.
- `pnpm build` and `pnpm test` pass with 100% coverage (see `AGENTS.md`).
- Each fix lands with a regression test that fails on the unfixed code.
- Performance PRs include before/after numbers from the B1 benchmark suite in the PR description.
- No public API is removed. Observable behavior changes are called out in the PR, the README and the release notes.
- Each PR updates the [Tracking](#tracking) table.

## Execution order

| # | ID | Title | Depends on | Behavior change |
|---|---|---|---|---|
| 1 | T1 | Isolate tests that flush the shared server | — | No (tests only) |
| 2 | B1 | Benchmarks for concurrency, multi-get, large values and bursts | — | No |
| 3 | H1 | Binary/SASL request queue | — | No |
| 4 | H2 | Single-flight connect, socket-scoped handlers | — | No (bug fix) |
| 5 | H3 | Connect timeout plus command deadline; no idle teardown | H2 | **Yes** |
| 6 | P1 | Coalesce socket writes per tick | B1, H1 | No |
| 7 | P2 | Linear multi-get miss detection | B1 | No |
| 8 | P3 | Buffer large values without re-copying | B1 | No |
| 9 | P4 | O(1) command queue | B1, H3 | Minor (`commandQueue` returns a snapshot) |
| 10 | N1–N6 | Next-tier optimizations | P1–P4 | No |
| 11 | R1 | Docs and release | all | — |

T1 and B1 are small and make every later PR easier to trust. H1 has no dependencies, so it can go first if the most severe issue should land first.

---

## Phase 0 — Foundations

### T1 — Isolate tests that flush the shared server

**Problem.** Vitest runs test files in parallel against the same memcached (`localhost:11211`). Some tests clear the whole server: `client.flush()` and a delayed `flush(1)` (`test/index.test.ts:1724-1753`) and `node.command("flush_all")` (`test/node.test.ts:441-442`). A test in another file that is between writing a key and reading it back can lose the key.

**Evidence.** Rare but real. 2 of 7 early full runs failed, each on a test whose key had disappeared: `should handle async append hooks` (`test/index.test.ts:2589`, `append` returned `false`) and `should execute prepend on all replica nodes` (`test/index.test.ts:3588`, `get` returned `undefined`). The next 52 runs passed, with warm and cold caches and with and without coverage. Sending `flush_all` to the shared server while the suite runs reproduces the first failure on demand, with the same assertion.

**Fix (done).**
- A `memcached-flush` compose service on port 11214. The three flush tests use it; no other test sends server-wide commands.
- Four hook tests (`add`, `replace`, `append`, `prepend`) used fixed key names and only passed because a flush test had cleared the shared server earlier in the same run. Once flushes moved, they failed on every run after the first. They now use `generateKey()`, like the rest of the suite.

**Result.** The shared server receives 0 `flush_all` commands per run (was 4), and 20 consecutive `vitest run --coverage` runs pass.

### B1 — Benchmarks that can see these problems

**Problem.** `benchmark/set-get.ts:53-58` measures one set→get at a time, so none of the issues in this plan show up in it.

**Fix (done).**
- The benchmarks use the same tooling as [qrbit](https://github.com/jaredwray/qrbit): one tinybench script per scenario in `benchmark/`, each printing a `tinybench-pretty-printer` table. `pnpm benchmark` runs them all (each also has its own `benchmark:<name>` script), and `pnpm benchmark:readme` regenerates the README tables between `<!-- BENCHMARK:* -->` markers.
- In each table, every row does the same total work in a different shape, so tinybench's summary column compares like with like:
  - `concurrency`: 500 gets or sets with 1 / 10 / 100 / 500 in flight
  - `multi-get`: 10,000 keys as `gets()` batches of 100 / 1,000 / 10,000
  - `large-values`: 4 MB read as 256 KB / 1 MB / 4 MB values, over TCP and TLS
  - `bursts`: 60,000 gets as bursts of 10,000 / 30,000 / 60,000
  - `cold-start`: sockets opened by 50 concurrent first requests (a count, not a timing)
  - `set-get` still compares this client with memjs and memcached.
- `memcached-bench` (port 11216) and `memcached-bench-tls` (port 21216) compose services, without `-vv` (it logs every command and caps throughput) and with `-I 32m` (for large values). They sit in a `bench` profile, so `test:services:start` and CI don't start them; use `pnpm benchmark:services:start` / `stop`. `test:services:stop` enables the profile so it tears everything down.
- `MEMCACHE_BENCH_*` variables override the targets and TLS settings (see the README). Every timed operation checks its result, so a change that returns misses can't pass as a speedup. On Linux, published ports go through `docker-proxy`, which adds per-packet work and inflates write-heavy results. The numbers in this plan were measured against the container IPs.
- Multi-get keys share a long prefix (`bench:user:profile:<id>`), the common real-world shape. Comparing such keys takes longer, so the quadratic miss check shows at 1,000 keys, not only at 10,000.

**Result.** The suite runs in about two minutes, and the baseline for `main` is in the README tables and the B1 PR description.

---

## Phase 1 — Correctness and hardening

### H1 — Binary/SASL request queue (wrong values under concurrency)

**Severity: critical.** A caller can receive another key's value, which can leak data between users.

**Problem.** `binaryRequest` (`src/node.ts:457-495`) adds a new `data` listener for each request (`:492`) and resolves with the first complete packet it sees. When requests overlap, every listener consumes the first response and the later responses are dropped. `binaryStats` (`:679-747`) and SASL authentication (`:448`) use the same pattern. SASL servers can only be used through these `binary*` methods (`README.md:863`).

**Evidence.** On the SASL test server (`:11215`):
- `Promise.all([binaryGet("a"), binaryGet("b"), binaryGet("c")])` returned `["value-of-a", "value-of-a", "value-of-a"]`.
- In a mixed batch, `binaryIncr` returned 1986096245 (the GET response read as a counter) and `binaryVersion` returned the GET body, flags included.
- A `binaryGet` sent alongside `binaryStats` returned the first stat's value.

**Fix (done).**
- One `data` handler per socket frames binary packets (24-byte header plus `totalBodyLength`). Chunks are kept in a list with a running byte count and joined once a whole packet has arrived, so a large value is copied once instead of on every chunk.
- Pending binary requests wait in a FIFO. memcached answers binary requests in order on a connection (no quiet opcodes are used), so FIFO matching is enough. Each request also carries a sequence number in `opaque`. A response with the wrong `opaque`, or one that doesn't start with the response magic byte, rejects every pending binary request and closes the connection instead of returning the wrong data.
- `binaryStats` is a queue entry that collects `STAT` packets until the empty terminator or an error packet. Before, an error (for example, on an unauthenticated connection) left it waiting forever.
- SASL authentication goes through the same queue. Before, a server that closed the connection during authentication left `connect()` pending forever.
- `binaryQuit` queues its request too, so memcached's reply to `QUIT` can't be taken for a later request's response.
- Pending binary requests are rejected when the socket closes, as text commands already are. Binary requests on a closed node reject instead of waiting forever.
- SASL connections parse everything as binary. Other connections parse binary only while binary requests are waiting. Before, binary responses on a non-SASL connection also went to the text parser and stayed in its buffer, so a later text command on that connection read them as part of its response.

**Tests.**
- A concurrent mix of `binaryGet` / `binarySet` / `binaryIncr` / `binaryDecr` / `binaryStats` / `binaryVersion` on the SASL server returns each caller's own result, and so does a concurrent batch over TLS + SASL that includes a 200 KB value.
- Mock-socket tests: several packets in one chunk, a header split across chunks, stats spanning chunks, a stats error followed by another response, `opaque` mismatch and bad magic byte reject and close, close rejects everything pending, the `QUIT` reply is consumed, and a 256 KB value in 1 KB chunks is joined with one `Buffer.concat`.
- A server that closes during SASL authentication makes `connect()` reject, and a binary request after the connection closes rejects.

**Result.** 14 of the 18 new tests fail on `main` (the other 4 cover error branches that already behaved correctly). With the fix, the full suite passed 5 consecutive runs.

**Compatibility.** No API change. Binary methods on a node that isn't connected now reject with the same message as text commands (`Not connected to memcache server <host:port>`), and `binaryStats()` resolves with the stats received so far (normally `{}`) when the server answers with an error.

### H2 — Single-flight connect and socket-scoped handlers

**Severity: high.** Affects the default configuration (`lazyConnect: true`) and every idle→busy transition.

**Problem.**
- `connect()` (`src/node.ts:242-320`) only returns early once connected (`:244`). Every caller that arrives while a connection is being opened creates another socket (`:249-260`) and overwrites `this._socket`. Callers that do this under concurrency: `getNodesByKey` (`src/index.ts:1352-1372`), `gets` (`:798`), `flush` / `stats` / `version` (`:1237`, `:1262`, `:1285`) and retries (`:1594`).
- The handlers act on instance state instead of the socket they belong to. The timeout handler destroys `this._socket` (`:316`), which can be a different, healthy socket. The close handler (`:307-312`) marks the node disconnected and rejects the shared queue.
- A socket destroyed before it connects never settles its `connect()` promise, so the waiting request hangs forever (there is no per-request timeout; see H3).
- Parser state (`_buffer`, `_pendingValueBytes`, `_multilineData`) is reset by `reconnect()` (`:346-351`) but not on close, so a disconnect mid-response can corrupt parsing on the next connection (code reading).

**Evidence** (real memcached; `timeout` lowered to 1–1.5 s to keep the test short):
- 50 concurrent first requests opened 50 TCP connections.
- Each idle→burst cycle of 50 requests leaked 49 more sockets: 99 → 148 → 197 → 246 open. `disconnect()` closed one (245 stayed open), and the leaked sockets keep the process alive. memcached's default connection limit is 1024.
- In every run, the leaked sockets' timeouts destroyed the active socket. In 2 of 6 runs a request never settled; in 3 of 6 a request returned `undefined` for a key that exists.
- With a prototype of the fix: 1 socket, 1 open across cycles, 0 after `disconnect()`, and all 6 runs clean.
- Re-measured before the fix on the benchmark server (`timeout: 1000`, four idle→burst cycles of 50 gets, 3 runs): 50 sockets per burst, 50 → 99 → 148 → 197 open, 200 `connect` events, 196 still open after `disconnect()`. The wrong-result and never-settled cases didn't show up in these runs; they depend on a stale socket timing out mid-burst, so the tests below cover them directly.

**Fix (done).**
1. `connect()` returns a shared in-flight promise (`this._connecting`), cleared in `finally` unless `disconnect()` has already started another attempt. A `connect()` during SASL authentication waits for it instead of returning early.
2. Socket setup moved into `openSocket()`, which captures `const socket`. Its handlers only change the node's state (connected flag, pending commands, parser) while `socket` is still the node's socket, and data from a replaced socket is dropped. The timeout handler destroys `socket`, not `this._socket`. `connect`, `close`, `error` and `timeout` events are still emitted for every socket.
3. A `close` before the socket is ready rejects the connect promise.
4. `disconnect()` fails pending commands itself instead of waiting for the old socket's `close`, which may now arrive after a new socket has replaced it. `reconnect()` fails them with its own message first, as before.
5. Rejecting pending commands also drops any partly received response, so every path that abandons a connection (close, `disconnect()`, `reconnect()`, SASL failure) resets the parser, not only `reconnect()`.

**Tests.**
- An in-process server that counts connections: 50 concurrent `get()`s on a lazy client open exactly one.
- Two concurrent `connect()` calls share one socket and emit one `connect` event; three concurrent `connect()` calls on a SASL node authenticate once.
- `disconnect()` before the socket is ready makes `connect()` reject instead of hang, and a new `connect()` doesn't wait on the abandoned one. (A TLS server that closes before `secureConnect` already rejected on `main`, through the `error` event.)
- After `reconnect()`, data, `timeout` and `close` from the old socket don't touch the new connection.
- A connection that closes after a partial `VALUE` leaves the next connection parsing cleanly.

**Result.** 50 concurrent first requests open 1 socket (`pnpm benchmark:cold-start`: 50 → 1). In the cycle test above: 1 socket per burst, at most 1 open, 4 `connect` events, 0 open after `disconnect()`. All 6 new tests fail on `main`; with the fix, the full suite passed 6 consecutive runs.

**Compatibility.** No API change. `connect` events fire once per real connection instead of once per duplicate socket. Commands pending at `disconnect()` are rejected during the call instead of when the socket's `close` event arrives, with the same `Connection closed` error.

### H3 — Timeouts: connect timeout plus command deadline, no idle teardown

**Severity: high.** Behavior change (documented).

**Problem.** `socket.setTimeout(this._timeout)` (`src/node.ts:262`) is a socket inactivity timer, and its handler destroys the socket (`:314-318`). The README documents `timeout` as an operation timeout (`README.md:210`, `:238-239`). As implemented:
- Healthy idle connections are destroyed after `timeout` ms (default 5 s), so the next request pays for a reconnect: a TCP round trip plus the TLS and SASL handshakes where configured. While H2 is unfixed, that reconnect also stampedes.
- Writes reset the timer, so a stalled server is never detected while the client keeps sending. There is no real per-operation deadline.
- The Auto Discovery config connection is torn down between polls and rebuilt on every poll (`src/auto-discovery.ts:216-237`; code reading).
- Changing `client.timeout` has no effect on existing nodes. The setter only updates the client's own field (`src/index.ts:243-245`), each node keeps the value it was constructed with (`src/node.ts:110`), and Auto Discovery keeps its own copy.

**Evidence.**
- `timeout: 1000` and 1.3 s idle: the node was disconnected, and the next request took 1.42 ms vs 1.03 ms when the connection is kept (local; production adds network round trips and handshakes).
- A server that accepts but never replies, `timeout: 500`: with no further writes the request settled after ~500 ms; with a write every 100 ms it was still pending after 3 s.
- Re-measured on `main` before the fix, with the same results, plus: `client.timeout = 200` left the node at 5000, and Auto Discovery with `timeout: 200` and a 400 ms poll opened 6 config connections in 2.1 s (one per poll).

**Fix (done).**
- The socket timeout only covers opening the TCP/TLS connection, then `socket.setTimeout(0)`. The existing `Connection timeout` behavior is unchanged.
- One command-deadline timer per node (not one per request), `unref()`'d. The wait starts when a command is queued on an idle node and restarts whenever response bytes arrive, never on writes. The timer isn't moved on every response: when it fires early it is scheduled again for the time left, and when nothing is pending it stops. So the hot path costs one `performance.now()` per queued command on an idle node and one per `data` event. On expiry the node emits `timeout`, rejects everything pending with `Command timeout` and destroys the socket, so a late response can't be taken for a later command's.
- SASL authentication is a pending binary request, so the same deadline covers it; a timeout there rejects `connect()` with `Connection timeout`, as before.
- Idle connections stay open. TCP keep-alive (already enabled) detects dead peers. As a result the Auto Discovery config connection is reused across polls.
- The binary queue from H1 uses the same deadline.
- `MemcacheNode` and `AutoDiscovery` have a `timeout` getter and setter, and `client.timeout` updates existing nodes and the Auto Discovery config connection. The deadline reads the current value, including for commands already waiting. (It isn't part of `updateNodes()`, which the `keepAlive` setters also call, so changing `keepAlive` doesn't overwrite a node's own timeout.)

**Tests.**
- Idle longer than `timeout`: still connected, no `timeout` event.
- Stalled server with a pending command: rejects with `Command timeout` after `timeout` (300 ms, measured ≥ 300 and < 1500 ms) and emits `timeout` once, while another command is written every 50 ms. The client resolves `get` as a miss and emits `timeout` with the node ID.
- A response arriving in 100-byte chunks every 50 ms (about 1 s in total) with `timeout: 300` is not timed out.
- Lowering the timeout applies to a command already waiting; `client.timeout` updates existing nodes and Auto Discovery.
- A binary request, SASL authentication and a TLS handshake that get no response time out (the last two as `Connection timeout`).
- Auto Discovery with a poll interval longer than `timeout` keeps one config connection.

**Result.** All four problems are fixed (1 config connection instead of one per poll). 12 of the 15 new tests fail on `main`; the other 3 (slow but progressing response, stalled TLS handshake, client `get` against a stalled server) passed on `main` too and guard the new code paths. Throughput is unchanged: in three alternating runs per build, gets and sets at 1 and 100 in flight overlapped between `main` and the fix (for example gets at 100 in flight: 61–65k ops/s on `main`, 65–71k with the fix).

**Compatibility.** Observable change: idle connections are no longer closed, the `timeout` event now means "connect or command deadline exceeded", and commands that time out reject with `Command timeout` (before, the socket was destroyed and they rejected with `Connection closed`). README updated. Ship in a minor release.

---

## Phase 2 — Performance

Measured against real memcached 1.6.45 with the real dependencies (environment in the [appendix](#appendix--how-the-numbers-were-measured)). Compare before and after within a row; absolute numbers depend on the machine.

### P1 — Coalesce socket writes per tick

**Problem.** Every `command()` makes its own `socket.write()` call (`src/node.ts:807`) with Nagle's algorithm disabled (`:263`), so each command is its own syscall and TCP segment. In a CPU profile at 100 requests in flight, `writeUtf8String` was 45.6% of client CPU.

**Evidence** (get throughput, ops/s):

| In flight | Current | Cork per tick | Change |
|---|---|---|---|
| 1 | 12.1k | 11.8k | within noise |
| 10 | 64.2k | 75.3k | +17% |
| 100 | 91.9k | 170.2k | +85% |
| 500 | 89.1k | 191.4k | +115% |

Over TLS the gain is smaller (+5–24%) because Node's TLS layer already batches encrypted writes. An "adaptive" variant (send the first write of a tick immediately, cork the rest) was slower than the simple version at 10–100 in flight, so it is not recommended.

**Fix (done).**
- `writeToSocket()` is used by `command()` and by `queueBinaryRequest()` (the binary and SASL path from H1). On an idle node it writes at once. A write made while other requests are pending corks the socket, unless it is already corked, and schedules `process.nextTick(() => socket.uncork())`. So the writes of that tick go out in one `writev` instead of one `write` each.
- The idle case is a change from the fix first planned here, which corked every write. That version delayed a lone command to the end of its tick and added a `process.nextTick` per command. It cost 2–4% at 1 in flight (medians over 6 alternating rounds per build: gets 11.97k → 11.44k, sets 12.08k → 11.81k). Writing at once on an idle node recovers that, and was as fast or faster at every level. Medians over 6 rounds each against corking every write, at 1 / 10 / 100 / 500 in flight: gets +1% / +8% / +4% / +0%, sets +3% / +9% / +13% / +5%. This is not the "adaptive" variant above, which skipped the cork for the first write of every tick even while requests were pending.
- The corked state is `socket.writableCorked`, so there is no flag to track or clear on close. The uncork callback holds the socket it corked. A socket replaced in the same tick is still uncorked, and uncorking a destroyed socket does nothing.

**Tests.**
- A lone command on an idle node is written at once, without a `cork`.
- Three `get`s issued in one tick: the first is written at once, the other two share one `cork`, all three writes are in order, `uncork` runs on the next tick, and each caller gets its own result.
- A batch issued on a later tick, while the first batch is still pending, corks and uncorks again.
- A socket destroyed before the tick ends: both commands reject with `Connection closed` and the pending `uncork` doesn't throw.
- Two `binaryGet`s issued in one tick share one `cork` and each get their own value.

**Result.** 4 of the 5 new tests fail on `main`. The fifth (a lone command is written at once) passes there too and guards the idle path.

The B1 `concurrency` benchmark against the container IP (batches of 500 commands per second, `main` → P1):

| In flight | gets | sets |
|---|---|---|
| 1 | 24 → 23 | 22 → 23 |
| 10 | 132 → 151 | 116 → 162 |
| 100 | 176 → 375 | 190 → 421 |
| 500 | 157 → 393 | 174 → 401 |

Longer closed-loop runs over TCP (ops/s, median of 6 runs per build in `main`, P1, P1, `main` order; 60,000 operations at 1 in flight, 200,000 otherwise):

| In flight | gets | sets |
|---|---|---|
| 1 | 11.5k → 11.9k | 11.7k → 12.1k |
| 10 | 61.8k → 80.5k (+30%) | 65.7k → 89.9k (+37%) |
| 100 | 86.7k → 207.4k (+139%) | 91.8k → 254.9k (+178%) |
| 500 | 87.0k → 252.5k (+190%) | 89.9k → 276.5k (+208%) |

At 1 in flight the ranges overlap (gets 11.1–12.1k vs 11.5–12.3k), as expected: a lone command is written exactly as on `main`. From 10 in flight up they don't overlap.

Over TLS (4 runs per build) the gain shrinks as concurrency grows. At 10 in flight, the medians went from 45.0k to 66.5k (gets) and from 49.4k to 72.7k (sets), +47–48%. At 100 they rose 12% for gets and 2% for sets, with overlapping ranges. At 1 and 500 in flight there is no change.

**Compatibility.** No API change. A command issued while others are pending goes out at the end of the current tick instead of immediately. One visible difference: if such a command is issued with `MemcacheNode.command()` in the same tick as `disconnect()` or `reconnect()`, it still rejects, but is no longer sent. Before, the server ran it even though the caller was told it failed. In 20 runs of two `set`s followed by `disconnect()` in the same tick, `main` stored all 40 and P1 stores the first 20, which are written at once as on `main`. Commands issued through `Memcache` reach the node after an `await`, so they behave as before, including an unawaited `set()` followed by `disconnect()` or `quit()`.

### P2 — Linear multi-get miss detection

**Problem.** At the end of every multi-line get, misses are computed with `requestedKeys.filter((key) => !foundKeys.includes(key))` (`src/node.ts:974-976`). That is O(requested × found) and runs synchronously inside the socket `data` handler, so it blocks the whole event loop. `get` and `gets` always pass `requestedKeys` (`src/index.ts:709-712`, `:801-804`).

**Evidence** (`gets()`, all hits):

| Keys | Current | With a `Set` |
|---|---|---|
| 100 | 1.03 ms | 0.89 ms |
| 1,000 | 20.7 ms | 4.5 ms |
| 5,000 | 372 ms | 16.1 ms |
| 10,000 | 1,722 ms | 38.3 ms |

**Fix.** Build `new Set(foundKeys)` once and emit `miss` for each requested key that isn't in it.

**Tests.** The existing hit/miss event tests, plus a 10,000-key `gets()` correctness test (no timing assertion).

**Compatibility.** None.

### P3 — Buffer large values without re-copying

**Problem.** While a value body is arriving, `handleData` concatenates the whole accumulated buffer with each new chunk (`src/node.ts:813-814`, waiting in `:818-830`), which copies O(n² / chunk size) bytes. TLS delivers records of at most 16 KB, so it is hit harder than TCP (64 KB reads).

**Evidence** (single `get`):

| Value | TCP: current → fixed | TLS: current → fixed |
|---|---|---|
| 256 KB | 1.40 → 1.03 ms | 2.56 → 1.37 ms |
| 1 MB | 4.37 → 2.33 ms (1.9×) | 10.15 → 3.42 ms (3.0×) |
| 4 MB | 30.4 → 8.8 ms (3.5×) | 110.2 → 9.5 ms (11.6×) |
| 16 MB | 517 → 48 ms (10.8×) | not measured |

**Fix.** While waiting for a value body, keep incoming chunks in a list with a running byte count and return until the value plus its CRLF is buffered, then concatenate once. The binary path has worked this way since H1 (`handleBinaryData`).

**Tests.** Values delivered in 1-byte, 16 KB and 64 KB chunks; CRLF split across chunks; values containing `\r\n`; several values in one chunk; a value and `END` in the same chunk; the existing partial-delivery tests (`test/node.test.ts:699`, `:914`).

**Compatibility.** None.

### P4 — O(1) command queue

**Problem.** Each response calls `this._commandQueue.shift()` (`src/node.ts:844`), and so does `rejectPendingCommands` (`:1029`). Past roughly 10–30k queued commands, V8 can no longer trim the array in place, so every `shift()` copies the whole backing store.

**Evidence.** `shift()` costs ~90 ns with ≤10k queued, 67 µs at 50k and 140 µs at 100k. Bursts of concurrent gets:

| Burst | Current | Head-index queue |
|---|---|---|
| 10k | 160 ms | 147 ms |
| 30k | 1,898 ms | 254 ms |
| 60k | 6,823 ms | 521 ms |
| 100k | 18,121 ms | 836 ms |

**Fix.** Keep a head index and compact the array once the consumed part is at least half of it (amortized O(1)), or use a small ring-buffer class. `rejectPendingCommands` iterates once and then resets. Keep `hasPendingCommands()` (the H3 deadline's check) O(1). The binary queue from H1 (`_binaryQueue`) also uses `shift()`; give it the same structure.

**Tests.** More pipelined commands than two compaction thresholds resolve in order; the `commandQueue` getter returns the pending items (`test/node.test.ts:649` and `test/index.test.ts:1473` rely on it being an array with a `length`); close rejects everything pending.

**Compatibility.** `commandQueue` returns a snapshot array instead of the live internal array. Call this out in the release notes.

---

## Phase 3 — Next tier

Smaller wins; each needs B1 before/after numbers in its PR. The numbers here come from the second audit's microbenchmarks.

- **N1 — Encode large `set` values once.** Today the value is scanned by `Buffer.byteLength` (`src/index.ts:1493`), concatenated into the command string (`:949`), concatenated again with `\r\n` (`src/node.ts:795`) and then encoded on write. Encoding once with `Buffer.from` and writing header, body and CRLF under P1's cork took a 1 MB value from 1,054 to 332 µs of CPU. Needs an internal command path that accepts Buffers.
- **N2 — Ketama key cache.** The key→node cache (`src/ketama.ts:336`, `:448-472`) empties itself every 5,000 new keys. With many distinct keys it costs more than it saves (0.57–1.35 µs per lookup vs 0.26–0.42 µs without it). Replace it with a single-node fast path. `ModulaHash` can return a cached, frozen `[node]` per node instead of allocating one per call (`src/modula.ts:207`); `KetamaHash` results are already frozen. Keep `BroadcastHash`'s per-call copy (`src/broadcast.ts:78-80`) unless benchmarks show it matters: `getNodesByKey()` returns a mutable array, so handing out the internal cache would let a caller's `.pop()` remove a node from every later broadcast. If it becomes a frozen array instead, call out that mutating the result now throws. `test/ketama.test.ts:503-504` reads `_cache` directly.
- **N3 — Fewer async layers on the hot path.** `get` → `getNodesByKey` (async) → `execute` → `executeWithRetry` → `command` costs about 0.2–0.5 µs per operation. Use a synchronous node lookup when already connected. When no retries are configured, replace the extra async frame with a single `.catch(() => undefined)` on `node.command()`. Failures must still resolve to `undefined` (so `set()` resolves `false`) as they do today (`src/index.ts:1569-1575`); a bare `return node.command()` would let them reject instead.
- **N4 — Cheaper line parsing.** Search for CR with `indexOf(13)` instead of the `"\r\n"` string (`src/node.ts:832`), and accept it only when the next byte is LF. If the CR is the last byte buffered, keep it and wait for more data rather than assuming a complete delimiter. Also avoid decoding fixed tokens such as `END` and `STORED` into strings, and use an offset cursor instead of a new `subarray` per line. Line parsing is about 10% of client CPU in profiles.
- **N5 — Binary packet building.** Allocate each packet once instead of 3–4 buffers plus `concat` (`src/binary-protocol.ts:142-446`), and parse headers without allocating an object and a `subarray` (`:84-96`). If the packet comes from `Buffer.allocUnsafe`, every byte must be written explicitly: `serializeHeader` (`:63-77`) relies on `Buffer.alloc` to zero the CAS field (bytes 16–23) when no CAS is given, and leftover heap bytes there would send a random CAS token. Add a test that the CAS bytes are zero when no CAS is given.
- **N6 — Backpressure (optional).** The queue is unbounded and the return value of `socket.write()` is ignored. Consider an optional `maxPendingCommands` that fails fast under overload.

## Checked and not worth changing

- Hit/miss event emission: 0.2% of client CPU with the real `hookified`.
- Key hashing: FNV-1a over strings plus a binary search on the ring is already cheap.
- Multi-get already sends one `get k1 … kN` per server and queries servers in parallel.
- Hooks are skipped entirely when none are registered (`_hasHooks`), and disabled retries add no timers.

## R1 — Docs and release

- README: `timeout` option and event semantics (H3), SASL concurrency (H1), benchmarks section (B1).
- Regenerate the README benchmark tables with `pnpm benchmark:readme` once P1–P4 have landed. They still show the `main` baseline from B1.
- Release notes for each PR. H1, H2 and P1–P4 are fixes. H3 changes observable behavior, so ship it in a minor release. Mention the `commandQueue` snapshot change (P4).
- Keep the tracking table below up to date.

## Tracking

| ID | Title | PR | Status |
|---|---|---|---|
| T1 | Isolate flush tests | [#148](https://github.com/jaredwray/memcache/pull/148) | Done |
| B1 | Benchmark suite | [#149](https://github.com/jaredwray/memcache/pull/149) | Done |
| H1 | Binary/SASL request queue | [#150](https://github.com/jaredwray/memcache/pull/150) | Done |
| H2 | Single-flight connect, socket-scoped handlers | [#151](https://github.com/jaredwray/memcache/pull/151) | Done |
| H3 | Connect timeout plus command deadline | [#152](https://github.com/jaredwray/memcache/pull/152) | Done |
| P1 | Coalesce writes per tick | [#153](https://github.com/jaredwray/memcache/pull/153) | Done |
| P2 | Linear multi-get miss detection | | Not started |
| P3 | Large-value buffering | | Not started |
| P4 | O(1) command queue | | Not started |
| N1–N6 | Next tier | | Not started |
| R1 | Docs and release | | Not started |

## Appendix — How the numbers were measured

- Server: memcached 1.6.45 in Docker (`memcached:1.6.45@sha256:75c93cc9…`, the image pinned in `docker-compose.yml`), started without `-vv` and with `-I 32m`. The SASL checks used the compose `memcached-sasl` service.
- Client: the repository's TypeScript sources run directly on Node 22.22.2 with the real `hookified` 3.0.3 and `hashery` 3.0.1. "Fixed" numbers come from minimal prototypes of the changes described above.
- Connections went straight to the container IP to avoid `docker-proxy`. Linux, 4 vCPUs.
- Throughput is closed-loop: N concurrent workers, each issuing sequential `get`s of a 100-byte value. Throughput tables report the median of 5 runs; latencies are averages over 5–30 requests; bursts are single runs.
- Connection counts come from memcached's `total_connections` and `curr_connections` stats.
