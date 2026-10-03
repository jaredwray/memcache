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
  - `large-values`: 4 MB read and written as 256 KB / 1 MB / 4 MB values, over TCP and TLS (the writes were added with N1)
  - `bursts`: 60,000 gets as bursts of 10,000 / 30,000 / 60,000
  - `cold-start`: sockets opened by 50 concurrent first requests (a count, not a timing)
  - `set-get` still compares this client with memjs and memcached. Since the P3 PR, each library runs 1,000 single-key and 1,000 10-key set → get → delete tasks; the tasks of all three go into one queue in random order and run one at a time, so no client always goes first. The queue runs 5 times, shuffled each time, and the table shows each library's median total.
  - `compare` (added with P2) runs this client, memjs and memcached through the same gets and sets at 1 / 10 / 100 / 500 in flight and the same 10,000-key multi-gets, and prints one table per workload with a column per client.
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

**Fix (done).** The miss check builds `new Set(foundKeys)` once and emits `miss` for each requested key that isn't in it, in request order. A key requested twice still gets one event per request, as before: memcached returns a found key once for each time it is requested.

**Tests.**
- A 10,000-key `gets()` where every third key exists returns exactly the stored keys and values. It emits `hit` for each stored key and `miss` for every other key, both in request order.
- A `get` that requests a missing key and a found key twice each emits two `hit`s and two `miss`es.
- The existing hit/miss event tests pass unchanged.
- Both new tests also pass on `main`. The change is a pure speed-up, so they guard its behavior; the speed-up itself is shown by the numbers below, because a timing assertion would be flaky.

**Result.** B1 `multi-get` benchmark against the container IP (operations per second; each operation fetches the same 10,000 keys; 2 runs per build):

| Operation | `main` | P2 |
|---|---|---|
| 100 × `gets()` of 100 keys | 13–15 | 19–22 |
| 10 × `gets()` of 1,000 keys | 4 | 30–32 |
| 1 × `gets()` of 10,000 keys | 0.56–0.58 | 29 |

Time for one `gets()`, all hits (median of 4 runs per build, each the median of 7 calls):

| Keys | `main` | P2 |
|---|---|---|
| 100 | 0.71 ms | 0.61 ms |
| 1,000 | 18.2 ms | 3.7 ms |
| 5,000 | 352 ms | 13.7 ms |
| 10,000 | 1,805 ms | 35.6 ms |

At 10,000 keys, all but about 36 ms of `main`'s 1.8 s was the miss check, which ran synchronously and blocked the event loop. The prototype's numbers above (1,722 → 38.3 ms) hold.

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

**Fix (done).** While a value body is arriving, `handleData` keeps the incoming chunks in a list with a running byte count and returns until the value and its CRLF are all here, then joins them with one `Buffer.concat`. Lines (`VALUE` headers, `END`, errors) are parsed as before. Rejecting pending commands (close, `disconnect()`, `reconnect()`, a timeout) also drops any collected chunks. The binary path has worked this way since H1 (`handleBinaryData`).

**Tests.**
- A 1 MB value delivered in 16 KB and in 64 KB chunks is joined with exactly one `Buffer.concat`. On `main` it's one per chunk.
- A value containing CRLF, delivered one byte at a time.
- A value's CRLF split across chunks: the value, then `\r`, then `\nEND`.
- Several values, one of them empty, and `END` in one chunk.
- A connection that closes with value chunks already collected leaves nothing behind: on the next connection, a value that also arrives in pieces is read correctly.
- The existing partial-delivery tests pass unchanged.

**Result.** The two copy-once tests fail on `main`; the other four guard behavior that was already correct.

B1 `large-values` benchmark against the container IPs (operations per second; each operation reads the same 4 MB; 2 runs per build):

| Values | TCP: `main` → P3 | TLS: `main` → P3 |
|---|---|---|
| 16 × 256 KB | 80–89 → 108–113 | 46–54 → 66–80 |
| 4 × 1 MB | 75–76 → 106–113 | 28 → 97–105 |
| 1 × 4 MB | 30–33 → 126–129 | 9 → 80–110 |

Time for one `get` (median of 4 runs per build, each the median of 7 gets, or 3 for 16 MB):

| Value | TCP: `main` → P3 | TLS: `main` → P3 |
|---|---|---|
| 256 KB | 1.3 → 0.9 ms | 1.2 → 0.8 ms |
| 1 MB | 7.4 → 2.7 ms | 11.0 → 2.5 ms |
| 4 MB | 35.6 → 9.9 ms | 112.9 → 10.0 ms |
| 16 MB | 460 → 38.7 ms | 2,017 → 46.3 ms |

A 16 MB value is now 12× faster over TCP and 44× faster over TLS, and TLS is no longer slower than TCP for large values. The prototype's numbers above hold (TCP 16 MB: 517 → 48 ms; TLS 4 MB: 110 → 9.5 ms).

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

**Fix (done).**
- `src/queue.ts` adds a small `Queue` class. `push` appends to an array, and `shift` reads from a head index instead of calling `Array.prototype.shift()`. When the queue empties, the array is reset in place. Once the consumed part is at least 1,024 items and at least half of the array, `shift` drops it with one `slice`, so a burst of n commands copies about n items in total instead of about n²/2. A taken item's slot is cleared at once, so a settled command can be garbage collected before the next compaction.
- `MemcacheNode` uses it for the text command queue and for the binary request queue from H1. `hasPendingCommands()` (the H3 deadline's check) stays O(1). Rejecting pending commands (close, `disconnect()`, `reconnect()`, a timeout, a binary response out of order) drains each queue once.
- `commandQueue` returns a copy of the pending commands, oldest first. Before, it returned the internal array, so changing the array changed the queue.

**Tests.**
- `test/queue.test.ts`: the order holds across many compactions while the queue grows; a queue that never empties keeps its array to at most 1,024 slots plus its length; a taken item's slot is cleared at once; `drain` returns the rest in order and leaves the queue usable; `toArray` returns a copy; `peek` and `shift` on an empty queue return `undefined`.
- 5,000 pipelined text commands are answered in order, with the responses in three chunks; 5,000 pipelined binary requests likewise, in two chunks.
- With 5,000 text commands pending, 3,000 are answered and then the connection closes: the other 2,000 reject with `Connection closed`.
- `commandQueue` lists the pending commands, and clearing the returned array doesn't clear the queue.

**Result.** Only the copy test fails on `main`, where clearing the returned array empties the queue. The other node tests guard order and rejection, which `main` already got right at this size; the speed-up is shown by the numbers below rather than a timing assertion.

A burst of concurrent gets against the container IP, fired at once and awaited together (median of 5 bursts, or 3 from 60,000 up; 2 runs per build in `main`, P4, P4, `main` order):

| Concurrent gets | `main` | P4 |
|---|---|---|
| 10,000 | 59–60 ms | 64–66 ms |
| 30,000 | 1,699–1,854 ms | 173–187 ms (about 10×) |
| 60,000 | 6,624–6,757 ms | 371–396 ms (about 17×) |
| 100,000 | 18,341–18,567 ms | 573–777 ms (about 27×) |

At 10,000 there is no reliable difference. Ten more alternating pairs of 15 bursts each gave `main` 52–61 ms (median 59) and P4 52–59 ms (median 55). At that size V8 can usually trim the array in place, so `shift()` is cheap on `main` too; at times it falls back to copying the array, for example while the garbage collector is marking. A CPU profile of 40 bursts of 10,000 put 720 ms of self time in `processLine` (where `shift()` runs) on `main` and 283 ms on P4.

The B1 `bursts` benchmark (time to send 60,000 gets in bursts of each size; 2 runs per build):

| Bursts | `main` | P4 |
|---|---|---|
| 6 × 10,000 | 458–475 ms | 362–417 ms |
| 2 × 30,000 | 3.4–3.7 s | 388–399 ms |
| 1 × 60,000 | 6.7 s | 391–412 ms |

The burst size no longer matters: 60,000 gets take about 0.4 s however they are split.

The B1 `concurrency` benchmark, where at most 500 commands are queued, is unchanged within noise (batches of 500 commands per second, 2 runs per build):

| In flight | gets: `main` → P4 | sets: `main` → P4 |
|---|---|---|
| 1 | 22–23 → 22–23 | 22–23 → 23–25 |
| 10 | 135–148 → 149–153 | 153–159 → 180–181 |
| 100 | 307–346 → 361–369 | 436–444 → 430–468 |
| 500 | 322–349 → 390–392 | 427–473 → 410–418 |

**Compatibility.** `commandQueue` returns a copy instead of the live internal array. Code that only reads it, as the tests do (`length`, `Array.isArray`), works as before; code that changed the queue through it no longer can. Call this out in the release notes.

---

## Phase 3 — Next tier

Smaller wins; each needs B1 before/after numbers in its PR. The numbers here come from the second audit's microbenchmarks.

- **N1 — Encode large `set` values once.** Today the value is scanned by `Buffer.byteLength` (`src/index.ts:1493`), concatenated into the command string (`:949`), concatenated again with `\r\n` (`src/node.ts:795`) and then encoded on write. Encoding once with `Buffer.from` and writing header, body and CRLF under P1's cork took a 1 MB value from 1,054 to 332 µs of CPU. Needs an internal command path that accepts Buffers.
  - **Done.** A storage command (`set`, `add`, `replace`, `append`, `prepend`, `cas`) with a value of 64 KB or more passes the value to `node.command()` in a new `data` option instead of joining it to the command string. The node writes the command line, the value and the closing CRLF as three parts of one write (corked, under P1's rules), so the socket encodes the value straight from the caller's string, with no copy. Smaller values are still joined: three writes cost more than the copy below about 16 KB, and 64 KB leaves a margin. A first version that encoded each large value into one request buffer with `Buffer.allocUnsafe` was faster at 4 MB but used as much CPU as `main` at 1 MB and 2.4× as much at 64 KB: every set allocated a new `ArrayBuffer` outside the JS heap, and V8 collected more often to free them.
  - **Tests.** A value of 64 KB is passed as `data` and one of 64 KB − 1 is joined to the command; values of 70,000 to 90,000 bytes, with 2-, 3- and 4-byte characters, round-trip through `add`, `replace`, `append`, `prepend`, `set` and `cas` (with a CAS token read from the server); the node writes `data` after the command line in one corked write when idle, and with the other writes of the tick otherwise. The multi-byte round trip also passes on `main`; the other three fail there.
  - **Result.** One `set` against the container IPs (median latency per call, in µs; 2 runs per build in `main`, N1, N1, `main` order):

    | Value | TCP: `main` → N1 | TLS: `main` → N1 |
    |---|---|---|
    | 64 KB | 133–169 → 98–105 | 146–198 → 130–206 |
    | 256 KB | 571–817 → 219–312 | 690–715 → 469–638 |
    | 1 MB | 2,402–2,529 → 1,318–1,342 | 2,212–2,645 → 1,765–1,835 |
    | 4 MB | 6,967–8,329 → 5,307–5,688 | 8,819–8,850 → 6,437–7,062 |

    CPU per call fell by about as much (TCP 1 MB: 2,351–2,421 → 1,288–1,377 µs). Below 64 KB the path is unchanged.

    The B1 `large-values` benchmark, which now also writes the 4 MB (operations per second; 2 runs per build):

    | Sets | TCP: `main` → N1 | TLS: `main` → N1 |
    |---|---|---|
    | 16 × 256 KB | 94 → 178–197 | 78–79 → 117–125 |
    | 4 × 1 MB | 107–108 → 200–222 | 98–109 → 147–151 |
    | 1 × 4 MB | 110–119 → 213–220 | 114–124 → 146–168 |

    Large sets are about twice as fast over TCP and 1.2–1.6× as fast over TLS. The get rows are unchanged within noise.
- **N2 — Ketama key cache.** The key→node cache (`src/ketama.ts:336`, `:448-472`) empties itself every 5,000 new keys. With many distinct keys it costs more than it saves (0.57–1.35 µs per lookup vs 0.26–0.42 µs without it). Replace it with a single-node fast path. `ModulaHash` can return a cached, frozen `[node]` per node instead of allocating one per call (`src/modula.ts:207`); `KetamaHash` results are already frozen. Keep `BroadcastHash`'s per-call copy (`src/broadcast.ts:78-80`) unless benchmarks show it matters: `getNodesByKey()` returns a mutable array, so handing out the internal cache would let a caller's `.pop()` remove a node from every later broadcast. If it becomes a frozen array instead, call out that mutating the result now throws. `test/ketama.test.ts:503-504` reads `_cache` directly.
  - **Done, differently from the plan.** Removing the memo made lookups about 10× slower wherever keys repeat across several servers: with three servers and 100 or 4,000 repeated keys, a lookup went from 12–31 ns to 239–282 ns. Hashing a key (FNV-1a, with a float multiply per character) and searching the ring costs about 250 ns, and the memo had been hiding it. So the memo stays, and the rest of the plan applies:
    - Each node gets one frozen `[node]` array when it is added. The memo and every lookup share it, so a miss no longer allocates and freezes a new array, which makes it about 20% cheaper.
    - While every point on the ring belongs to one node, `getNodesByKey()` returns that node's array without hashing or touching the memo. A node with weight 0, or too light to get a point on the ring, gets no keys, so it doesn't count.
    - `ModulaHash` also keeps one frozen array per node, where it allocated one per lookup, and has the same fast path while every entry in its weighted list is the same node. A node with a negative weight gets no entry and doesn't count.
    - `BroadcastHash` and the hash functions are unchanged. Making FNV-1a a true 32-bit multiply (`Math.imul`) would move most keys to other servers.
  - **Tests.** For each provider, keys on the same node get the same frozen array (and changing a Modula result throws), and a single node is returned without hashing until a second node is added and again after it is removed. The four fail on `main`. A node with no points on the ring, or no place in Modula's list, still gets no keys, and doesn't stop the fast path for the one node that does; removing a node forgets the keys remembered for it; the memo-eviction test now uses two nodes, since one node never reaches the memo.
  - **Result.** Nanoseconds per `getNodesByKey()` (1M lookups, median of 5; 2 runs per build in `main`, N2, N2, `main` order):

    | Provider | 100 hot keys | 4,000 keys | 200,000 keys |
    |---|---|---|---|
    | Ketama, 1 server | 27–28 → 12 | 31 → 7–8 | 496–546 → 5–6 |
    | Ketama, 3 servers | 12–15 → 15–32 | 28–31 → 30–33 | 411–565 → 381–390 |
    | Modula, 1 server | 137–151 → 31–40 | 212–224 → 5 | 241–250 → 5–9 |
    | Modula, 3 servers | 227–231 → 138–150 | 238–257 → 156–160 | 258–286 → 173–182 |

    Ketama with three servers and repeated keys is unchanged: five more alternating runs gave 13–30 ns on `main` and 15–25 ns here for 100 keys, 28.6–41.9 and 28.6–30.6 ns for 4,000.

    B1 `multi-get`, whose 10,000 distinct keys churned the 5,000-key memo on `main` (operations per second; 2 runs per build): 10 × 1,000 keys 33–35 → 43–45, 1 × 10,000 keys 30 → 38–40, 100 × 100 keys 23–24 → 32–34. B1 `concurrency`, whose 1,000 keys already hit the memo, is unchanged within noise.
- **N3 — Fewer async layers on the hot path.** `get` → `getNodesByKey` (async) → `execute` → `executeWithRetry` → `command` costs about 0.2–0.5 µs per operation. Use a synchronous node lookup when already connected. When no retries are configured, replace the extra async frame with a single `.catch(() => undefined)` on `node.command()`. Failures must still resolve to `undefined` (so `set()` resolves `false`) as they do today (`src/index.ts:1569-1575`); a bare `return node.command()` would let them reject instead.
  - **Done.**
    - The single-key commands (`get`, `set`, `add`, `replace`, `append`, `prepend`, `cas`, `delete`, `incr`, `decr`, `touch`) look up their nodes synchronously when all of them are connected, so with no hooks registered the command is written before the call returns. Only when a node isn't connected do they await `getNodesByKey()`, which connects it. Its own single-node fast path is gone, since the commands no longer reach it; its loop returns the same nodes.
    - `execute()` is no longer an async function. Without retries (the default) each command's promise gets one handler: `[result]` for a single node, and a failure gives `undefined`, so `set()` still resolves `false`. Retries keep the async loop in `executeWithRetry()`.
    - `node.command()` is no longer async either. Everything runs in the promise executor, so a node that isn't connected still rejects; it never throws.
    - A `set` or `delete` on one server now creates 3 promises instead of 6, and a `get` 2 instead of 4. After the reply, each settles in half as many microtask turns.
  - **Tests.** Each of the 11 commands is written before its call returns on a connected node (fails on `main`); each connects first on a client that isn't connected; only the unconnected node of a two-node key is connected; with retries off a failing command makes `set`, `add`, `touch` and `delete` resolve `false`, `get` and `incr` `undefined`, and `execute()` `[undefined]`; a failing replica gives `undefined` in its slot and `set` resolves `false`; a key with no node still rejects; and `command()` on a node that isn't connected rejects without throwing.
  - **Result.** Client time per request, with a fake socket that answers from memory so the network doesn't count: the median change over 8 pairs of alternating rounds in one process, 100,000 requests per round.

    | Request | 1 in flight | 10 | 100 | 500 |
    |---|---|---|---|---|
    | `get` | −8% | −15% | −7% | −8% |
    | `set` | −15% | −19% | −28% | −26% |
    | `delete` | −7% | −19% | −21% | −22% |

    A `set` with 100 in flight went from 2.45 to 1.78 µs. B1 `concurrency` against the container IP (operations per second; 4 runs per build, in `main`, N3, N3, `main` order twice): 500 sets with 500 in flight 370–434 → 497–540 (+25% by median), with 100 in flight 333–462 → 470–518 (+10%), with 10 in flight 159–166 → 160–179 (+4%). Sets with 1 in flight and every gets row are unchanged within noise. With 1 in flight the round trip dominates, and a get saves about 0.3 µs of the roughly 5 µs it costs even with 500 in flight, less than the spread between runs.
- **N4 — Cheaper line parsing.** Search for CR with `indexOf(13)` instead of the `"\r\n"` string (`src/node.ts:832`), and accept it only when the next byte is LF. If the CR is the last byte buffered, keep it and wait for more data rather than assuming a complete delimiter. Also avoid decoding fixed tokens such as `END` and `STORED` into strings, and use an offset cursor instead of a new `subarray` per line. Line parsing is about 10% of client CPU in profiles.
  - **Done.** A profile of gets, sets and 100-key multi-gets put parsing at about a third of the client's time, not 10%: the `"\r\n"` search took 10%, a `subarray` per line or value 7%, decoding 9%, and `processLine()` 12%.
    - Line ends are found with `indexOf(13)`, a byte search, and a CR only counts when an LF follows it. A CR that is the last byte buffered waits for the next chunk.
    - The fixed one-word replies (`OK`, `END`, `ERROR`, `EXISTS`, `STORED`, `DELETED`, `TOUCHED`, `NOT_FOUND`, `NOT_STORED`) come back as constant strings instead of being decoded. A JS byte loop matches them: on a mix of replies it took 61 ns per line, against 140 ns to decode every line and 218 ns with `Buffer.compare()` and offsets.
    - Lines and values are read from an offset into the buffer, and what is left is kept once per chunk, instead of a new `subarray` per line and value.
    - Found while changing this code: a hit or miss listener that closed the connection, such as `client.on("hit", () => client.disconnect())`, crashed the process with a `TypeError`. `processLine()` read the command after the close had cleared it. A multi-get is now settled before its hit and miss events, so a listener can't fail it. Parsing stops when a listener closes the connection, so nothing is kept for the next one. When a listener throws, the lines before it aren't read again.
  - **Tests.** A CR at the end of a chunk waits for its LF, in a reply and in a `VALUE` line. A CR without an LF stays in the line, also across chunks. The fixed replies resolve as before without decoding, and a line of the same length but another word is decoded. A chunk of replies is parsed without a `subarray`, and an unfinished reply takes one. A get whose hit listener disconnects resolves with its value, the next get on that connection is rejected, and the next connection starts clean. A hit listener that throws doesn't make a reply be read twice. The last four fail on N3; the first two pass there.
  - **Result.** Client time per request with the fake socket from N3: the median change over 8 pairs of alternating rounds in one process, N3 → N4 (negative is faster).

    | Request | 1 in flight | 10 | 100 | 500 |
    |---|---|---|---|---|
    | `get` | −21% | −26% | −30% | −33% |
    | `set` | −11% | −22% | −30% | −9% |
    | `delete` | −10% | −18% | −18% | −22% |
    | `incr` | −11% | −11% | −18% | −12% |
    | 100-key multi-get | −29% | −26% | −28% | −27% |

    B1 against the container IP (4 runs per build, in N3, N4, N4, N3 order twice). Time per benchmark operation, from the median operations per second, so lower is better. A `multi-get` operation is the whole batch, and a `concurrency` operation is 500 requests.

    | Benchmark row | N3 | N4 | Time | Throughput |
    |---|--:|--:|--:|--:|
    | `multi-get` 10 × 1,000 keys | 25 ms | 19 ms | −23% | +30% |
    | `multi-get` 1 × 10,000 keys | 30 ms | 22 ms | −28% | +39% |
    | `multi-get` 100 × 100 keys | 36 ms | 29 ms | −20% | +25% |
    | `concurrency` 500 gets, 500 in flight | 2.36 ms | 1.86 ms | −21% | +27% |
    | `concurrency` 500 gets, 100 in flight | 2.51 ms | 1.99 ms | −21% | +26% |
    | `concurrency` 500 sets, 100 in flight | 1.80 ms | 1.63 ms | −10% | +11% |

    Gets gain the most: every value comes with a `VALUE` line, and the reply ends with `END`. The other `concurrency` rows (sets with 500, 10 or 1 in flight, and gets with 10 or 1) took 1% to 9% less time, within the spread between runs.
- **N5 — Binary packet building.** Allocate each packet once instead of 3–4 buffers plus `concat` (`src/binary-protocol.ts:142-446`), and parse headers without allocating an object and a `subarray` (`:84-96`). If the packet comes from `Buffer.allocUnsafe`, every byte must be written explicitly: `serializeHeader` (`:63-77`) relies on `Buffer.alloc` to zero the CAS field (bytes 16–23) when no CAS is given, and leftover heap bytes there would send a random CAS token. Add a test that the CAS bytes are zero when no CAS is given.
  - **Done.** These are the `binary*` methods on a node, which SASL-enabled servers require, and the SASL handshake itself; the client's own commands use the text protocol.
    - Each builder computes the packet's length and writes the header, extras, key and value into one `Buffer.allocUnsafe()`. Before, a request allocated a header object and a zeroed 24-byte header, Buffers for the key, value and extras, and joined them with `Buffer.concat`: 5 to 7 allocations, with the value copied twice. Every byte is written, since that memory can hold old bytes: the data type, vbucket, opaque and CAS are set to 0.
    - `handleBinaryPacket()` reads the opaque straight from the packet, and the methods read the status with `readStatus()`. Before, each response was parsed into a header object with a CAS `subarray` twice, once for its opaque and once for its status. The binary queue's callback no longer gets a parsed header; `binaryStats()` and `binaryVersion()`, which need more of it, still call `deserializeHeader()`.
    - `parseGetResponse()` and `parseIncrDecrResponse()` return the status and the value, without a header object or subarrays. An empty value still reads as no value in `binaryGet()`, as before.
  - **Tests.** Every builder's packet matches, byte for byte, the packet the old code joined, with multi-byte keys and values, string and `Buffer` values and 64-bit counters, while `Buffer.allocUnsafe()` returns memory filled with `0xff`; each build takes one allocation and no `concat`, which fails on `main`. The CAS field of every packet is zero; this passes on `main` too, where `Buffer.alloc` zeroed it. The parse helpers return the status and the value, skip a key in the response, and read an empty value as none.
  - **Result.** Building one packet (median of 9 alternating rounds in one process; lower is better): `get` 329 → 162 ns, `set` of 100 bytes 645 → 409 ns, `incr` 489 → 216 ns, `set` of 64 KB 24 → 20 µs, `set` of 1 MB 319 → 252 µs.

    Client time per binary request, with a fake socket that answers from memory as in N3: the median change over 8 pairs of alternating rounds in one process (negative is faster).

    | Request | 1 in flight | 10 | 100 | 500 |
    |---|---|---|---|---|
    | `binaryGet` | −25% | −24% | −33% | −31% |
    | `binarySet` | −21% | −34% | −31% | −40% |
    | `binaryDelete` | −23% | −33% | −32% | −37% |
    | `binaryIncr` | −19% | −25% | −35% | −34% |

    Against the bench container's IP, with B1 `concurrency`'s shape but a node's binary methods (a scratch script, as B1 has no binary benchmark): time per batch of 500 requests, from the median operations per second over 4 runs per build in `main`, N5, N5, `main` order twice, so lower is better.

    | Row | `main` | N5 | Time | Throughput |
    |---|--:|--:|--:|--:|
    | 500 gets, 10 in flight | 4.00 ms | 3.40 ms | −15% | +18% |
    | 500 gets, 100 in flight | 1.45 ms | 0.98 ms | −32% | +47% |
    | 500 gets, 500 in flight | 1.23 ms | 0.97 ms | −21% | +27% |
    | 500 sets, 10 in flight | 4.00 ms | 3.49 ms | −13% | +15% |
    | 500 sets, 100 in flight | 1.44 ms | 1.05 ms | −27% | +38% |
    | 500 sets, 500 in flight | 1.63 ms | 1.32 ms | −19% | +24% |

    With 1 in flight the round trip dominates: gets took 2% less time and sets 7% less, within the spread between runs.
- **N6 — Backpressure (optional).** The queue is unbounded and the return value of `socket.write()` is ignored. Consider an optional `maxPendingCommands` that fails fast under overload.
  - **Done.** A new `maxPendingCommands` option, on the client and on a node, caps the requests each node keeps waiting for a reply or for its connection to open. It is off by default (`0`).
    - A request made while a node has that many, text or binary, fails at once: it is neither queued nor written. Through the client it fails like any other command, so `set()` resolves `false` and `get()` `undefined`; `node.command()` and the `binary*` methods reject with a "Too many pending commands" error. A command whose reply has started to arrive still counts.
    - Requests waiting for a node's connection count too. A request that finds its node not connected waits in `connectForRequest()`, a new node method that the client's commands use instead of `connect()`, and past the limit it fails at once. It fails the way a failed connection does: single-key commands reject, and `gets()` leaves out that node's keys. Before this, found in review, a burst against a server whose connection never opened waited without limit until the connect timeout.
    - Refused requests are not retried, even with `retries` set: the retry loop stops at the node's `PendingLimitError`. A retry would wait out its delay and add to the load.
    - The client's setter updates every node, and nodes added later, including those found by Auto Discovery, get the limit. The client and the node round it down and treat anything below 1, or not a finite number, as no limit.
    - Every request a node refuses gets the same Error, built again when the limit changes. A new Error for each, with its stack trace, made the 100,000 calls in the measurement below take about 920 ms at a limit of 1,000, three times as long as queueing them all; without stack traces they took 500–620 ms.
    - A quit still goes out at the limit. Refused, it made `quit()` close the connection at once, and the requests ahead of it failed instead of getting their replies.
    - The return value of `socket.write()` is still not used. Every pending request has already been written, so the limit also bounds what the socket buffers. A server that reads requests without answering them, as in the measurement below, never fills that buffer, so a limit on buffered bytes wouldn't catch it.
  - **Tests.** A node has no limit unless given one, and rounds the limit down or ignores it like the client; at the limit a command fails without being queued or written, and a reply makes room again; a command whose reply is still arriving counts; binary requests count toward the same limit; refused requests share one error, and a new limit gives a new one; a text and a binary quit still go out at the limit, and the request ahead of the quit gets its reply; requests waiting for a connection that never opens count, the one past the limit fails at once, and they stop counting when the connection fails or opens. Through the client: the default and the option reach every node, the value is rounded down or ignored, the setter reaches existing nodes and nodes added later, discovered nodes get the limit with and without TLS, at the limit `set()`, `get()` and `delete()` fail at once without writing and then succeed once there is room, a refused `set()` isn't retried, and while a connection is being opened a request past the limit rejects and `gets()` leaves out the node's keys before the waiting requests settle.
  - **Result.** With no limit, the default, the request path is unchanged. With the fake socket from N3, the median N6/`main` time ratio over 8 pairs of alternating rounds, with 1, 100 and 500 in flight, was 0.98–1.01 for gets, 0.96–1.02 for sets and 0.91–1.00 for 100-key multi-gets (1.00 is no change; lower is faster).

    A server that reads requests and never answers, and 100,000 gets made at once with `timeout: 2000` (2 runs per limit):

    | Limit | Making the calls | Failed at once | Waited for the timeout | Heap held while waiting |
    |---|--:|--:|--:|--:|
    | none | 297–313 ms | 0 | 100,000 (until 2.08 s) | 123.6 MB |
    | 1,000 | 343–420 ms | 99,000 (by 388–469 ms) | 1,000 | 6.7 MB |
    | 100 | 356–399 ms | 99,900 (by 401–448 ms) | 100 | 5.7 MB |

    Times are from the first call. Without a limit every caller waits for the timeout, and each pending request holds about 1.2 KB of heap. With one, the callers beyond it get `undefined` right after the calls are made, and most of the heap left is the measurement's own 100,000 promises. Refusing a request costs a little more than queueing one, so making the calls took 15–35% longer, but nothing is written for the refused requests.

    The same burst on a client that isn't connected yet, against a server that accepts the connection but never answers its TLS handshake (`timeout: 2000`, 2 runs per limit):

    | Limit | Failed at once | Waited for the connection | Heap held while waiting |
    |---|--:|--:|--:|
    | none | 0 | 100,000 (until 2.50–2.57 s) | 123.5 MB |
    | 1,000 | 99,000 (by 450–484 ms) | 1,000 | 7.2 MB |
    | 100 | 99,900 (by 430–431 ms) | 100 | 5.8 MB |

    Before requests waiting for the connection counted, a limit of 1,000 gave the first row: all 100,000 waited, holding 123.5 MB.

## Checked and not worth changing

- Hit/miss event emission: 0.2% of client CPU with the real `hookified`.
- Key hashing: FNV-1a over strings plus a binary search on the ring is already cheap.
- Multi-get already sends one `get k1 … kN` per server and queries servers in parallel.
- Hooks are skipped entirely when none are registered (`_hasHooks`), and disabled retries add no timers.

## R1 — Docs and release

- README: `timeout` option and event semantics (H3), SASL concurrency (H1), benchmarks section (B1).
- The README benchmark tables were regenerated in the P3 PR, and the `bursts` table again in the P4 PR, so they include P1–P4.
- The `concurrency` table was not regenerated for N3. In that session the bench containers' published ports took about twice as long per round trip as when the table was made, on `main` as on N3 (1 in flight: 24 → 11–12 operations per second), so a new table would have shown drops N3 didn't cause. Regenerate all tables in one session here.
- Release notes for each PR. H1, H2 and P1–P4 are fixes. H3 changes observable behavior, so ship it in a minor release. Mention the `commandQueue` snapshot change (P4), that N4 fixes a crash when a hit or miss listener closes the connection, and the new `maxPendingCommands` option (N6).
- Keep the tracking table below up to date.
- **Done.** The README already covered H3's `timeout` option and event, concurrent `binary*` requests (H1) and the benchmark section (B1).
  - All the README benchmark tables were regenerated in one session, against the bench containers' IPs; a line above them says how they were made.
  - The version stays at 1.11.0 until the release. The bump to 1.12.0, a minor release for H3's behavior change and N6's new option, comes with it. Two tables label memcache with the version in `package.json`, so they read v1.11.0 until then; `pnpm benchmark:readme set-get compare` updates them after the bump.
  - Large-value sets came out lower than in N1's PR: 157 against 200–222 operations per second for 4 × 1 MB over TCP. N1's own code measured the same as `main` in this session (156–170), so the difference is the machine, not a regression.
  - Release notes for 1.12.0 are drafted in the R1 PR, for the release: the version bump, then the GitHub release, which publishes to npm. They include this comparison of 1.11.0 and `main`: today's benchmark files against both in one session, taking turns, 2–4 runs each (time per operation, so lower is better).

    | Benchmark | 1.11.0 | `main` | Speedup |
    |---|--:|--:|--:|
    | 500 gets, 100 in flight | 5.8 ms | 1.9 ms | 3.1× |
    | 500 sets, 100 in flight | 5.0 ms | 1.6 ms | 3.2× |
    | 500 gets, 10 in flight | 7.6 ms | 5.4 ms | 1.4× |
    | 500 gets, 1 in flight | 41 ms | 40 ms | unchanged |
    | `gets()` of 10,000 keys, one batch | 1.69 s | 22 ms | 77× |
    | `gets()` of 10,000 keys, 10 batches | 228 ms | 19 ms | 12× |
    | 60,000 gets in one burst | 6.7 s | 296 ms | 23× |
    | get of a 4 MB value, TLS | 105 ms | 10 ms | 10.6× |
    | get of a 4 MB value, TCP | 31 ms | 8.5 ms | 3.6× |
    | 4 sets of 1 MB values, TCP | 8.5 ms | 6.2 ms | 1.4× |
    | 1,000 tasks setting, getting and deleting 10 keys, one at a time | 519 ms | 412 ms | 1.26× |

    50 concurrent first requests opened 50 sockets on 1.11.0 and 1 on `main`. Against memjs and memcached, `main` is the fastest client in every row; 1.11.0 trailed memjs on gets with 100 or more in flight, on sets with 500, and on multi-gets of 1,000 or more keys per batch.

## Tracking

| ID | Title | PR | Status |
|---|---|---|---|
| T1 | Isolate flush tests | [#148](https://github.com/jaredwray/memcache/pull/148) | Done |
| B1 | Benchmark suite | [#149](https://github.com/jaredwray/memcache/pull/149) | Done |
| H1 | Binary/SASL request queue | [#150](https://github.com/jaredwray/memcache/pull/150) | Done |
| H2 | Single-flight connect, socket-scoped handlers | [#151](https://github.com/jaredwray/memcache/pull/151) | Done |
| H3 | Connect timeout plus command deadline | [#152](https://github.com/jaredwray/memcache/pull/152) | Done |
| P1 | Coalesce writes per tick | [#153](https://github.com/jaredwray/memcache/pull/153) | Done |
| P2 | Linear multi-get miss detection | [#154](https://github.com/jaredwray/memcache/pull/154) | Done |
| P3 | Large-value buffering | [#155](https://github.com/jaredwray/memcache/pull/155) | Done |
| P4 | O(1) command queue | [#156](https://github.com/jaredwray/memcache/pull/156) | Done |
| N1 | Encode large values once | [#157](https://github.com/jaredwray/memcache/pull/157) | Done |
| N2 | Cheaper key lookups (Ketama memo kept) | [#158](https://github.com/jaredwray/memcache/pull/158) | Done |
| N3 | Fewer async layers per request | [#159](https://github.com/jaredwray/memcache/pull/159) | Done |
| N4 | Cheaper line parsing, and a listener crash fix | [#160](https://github.com/jaredwray/memcache/pull/160) | Done |
| N5 | Binary packets in one allocation | [#161](https://github.com/jaredwray/memcache/pull/161) | Done |
| N6 | Optional pending request limit (`maxPendingCommands`) | [#162](https://github.com/jaredwray/memcache/pull/162) | Done |
| R1 | Docs and release | [#163](https://github.com/jaredwray/memcache/pull/163) | Done |

## Appendix — How the numbers were measured

- Server: memcached 1.6.45 in Docker (`memcached:1.6.45@sha256:75c93cc9…`, the image pinned in `docker-compose.yml`), started without `-vv` and with `-I 32m`. The SASL checks used the compose `memcached-sasl` service.
- Client: the repository's TypeScript sources run directly on Node 22.22.2 with the real `hookified` 3.0.3 and `hashery` 3.0.1. "Fixed" numbers come from minimal prototypes of the changes described above.
- Connections went straight to the container IP to avoid `docker-proxy`. Linux, 4 vCPUs.
- Throughput is closed-loop: N concurrent workers, each issuing sequential `get`s of a 100-byte value. Throughput tables report the median of 5 runs; latencies are averages over 5–30 requests; bursts are single runs.
- Connection counts come from memcached's `total_connections` and `curr_connections` stats.
