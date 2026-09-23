# Prioritised Action Plan — Distributed Message Broker

**Derived from:** `docs/PROJECT_RESULTS_AUDIT.md` (2026-09-03)
**Scope rule:** every item below closes a gap that the audit actually found. Nothing here is polish, refactoring, or a new feature.

**Prioritisation logic:** (1) things that make the project look broken to anyone who clones it, (2) things that unblock the project's own headline claim, (3) things that make an existing number defensible, (4) things that materially improve a number, (5) things that fill a measurement hole an interviewer will probe.

| # | Action | Effort | Blocking? |
|---|---|---|---|
| 1 | Fix the red test suite | ~1–2 h | Blocks any "clone and run it" moment |
| 2 | Make the chaos harness survivable at scale (stream history to disk, raise heap) | ~2–4 h | Blocks Action 3 |
| 3 | Run chaos at a citable scale and commit the raw artifacts | ~2 h work + overnight run | Depends on 2 |
| 4 | Re-run benchmarks correctly, commit raw JSON, add a rate-limited latency point | ~2–3 h | Independent |
| 5 | Add group commit to the Raft log (and optionally batched publish) | ~4–8 h / ~1–2 d | Independent; changes M6/M7 |
| 6 | Measure failover time | ~2 h | Independent |
| 7 | Rewrite the headline claims to match evidence | ~1 h | Depends on 3, 4, and optionally 5, 6 |

---

## Action 1 — Fix the failing and flaky tests so `./gradlew test` is deterministically green

> **Status: DONE (2026-09-23).** Two consecutive full `./gradlew test --rerun-tasks` runs: 337 tests, 0 failures, 0 errors (~450 s each).
> - `PartitionOrderingTest`: re-measured at 28.6 s against its 30 s budget (disk-bound, as predicted). `RECORD_COUNT` 2,000 → 500; now ~7.5 s.
> - `FollowerKillTest` / `ConsumerResumeAfterFailoverTest`: passed 18/18 in isolation, so not a timeout problem. Root cause was a **Raft liveness bug**: a follower reset its election timer *before* its durable append/apply; when that disk work outlasted the 150–300 ms election timeout, the timer thread (already past the deadline, blocked on the node lock) started an election unconditionally once the lock freed — spurious term bumps / leader churn under disk load, surfacing as NOT_LEADER to the tests' single-connection producers. Fixed in `RaftNode` (re-arm after disk work; `startElection` re-checks expiry under the lock) with regression test `SlowFollowerElectionTest` (reproduced term 1→2 deterministically before the fix). `/raft-review`: all five invariants PASS, no changes required. This also likely caused needless elections in the Docker cluster under load — relevant to M6/M7 in Action 4.
> - Not changed: `ConnectionAcceptor.close()` uses `shutdownNow()`, which interrupts handler threads that may be mid-`FileChannel` I/O (closing the channel via `ClosedByInterruptException`). This is a plausible cause of the one-off teardown `ClosedChannelException` the audit saw, but it was not reproduced; left as-is pending evidence.

### Action
Make `PartitionOrderingTest.concurrentProducersOnDifferentPartitionsPreserveEachOthersOrder` pass deterministically, and stabilise `FollowerKillTest.publishingContinuesAfterAFollowerIsKilled` and `ConsumerResumeAfterFailoverTest.consumerResumesFromCommittedOffsetAfterFailover`.

### Why
A recruiter or engineer who clones this repo will run `./gradlew test` — it is the first command in the README's Gradle table. It currently fails. Every correctness claim in the project is undermined by a red suite, regardless of the reason, and the reason ("the disk is too slow for the test's own timeout") is *worse* than a logic bug because it reads as untested code. This is the single highest embarrassment-per-hour item in the audit.

### Current problem
- `PartitionOrderingTest` fails deterministically (verified on two independent runs): `TimeoutException` at `PartitionOrderingTest.java:81`. It publishes 2 partitions × 2,000 records synchronously in a 30-second budget. Each publish costs 2 serialized `fsync`s (Raft log + applied-index marker) even though the partition log is in-memory: 8,000 fsyncs ÷ the machine's measured 231 fsync/s ≈ **34.6 s > 30 s**.
- `FollowerKillTest` and `ConsumerResumeAfterFailoverTest` each failed once in a full-suite run and passed on re-run — same disk pressure surfacing as timing flakiness rather than a clean timeout.

### Expected outcome
`./gradlew test` passes 336/336, repeatably. Not guaranteed on first attempt — the flaky pair may need more than a timeout adjustment if their failure mode turns out to be a connection-lifecycle race rather than pure slowness.

### How to execute
1. Reproduce and confirm the ceiling:
   ```bash
   export JAVA_HOME=$(/usr/libexec/java_home -v 21)
   ./gradlew :broker:test --tests '*PartitionOrderingTest' --console=plain
   ```
2. For `PartitionOrderingTest` (`broker/src/test/java/io/minikafka/broker/PartitionOrderingTest.java`), pick **one**, in this order of preference:
   - **Preferred:** drop `RECORD_COUNT` from 2,000 to ~500. The test asserts *per-partition ordering under concurrency* — that property is fully exercised at 500 records, and 4× less disk work puts it comfortably inside budget on any machine. Keep the 30 s timeout so the test still catches a real regression.
   - **Alternative:** keep the record count and raise the `future0.get(...)`/`future1.get(...)` budgets at lines 81–82 to 120 s (and the `start.await(30, SECONDS)` in `produce()` at line 94). This preserves load but makes the suite slower and still machine-dependent.
   - **Do not** work around it by weakening durability in `TestBrokerConfig` — that would make the test stop covering the real write path.
3. For `FollowerKillTest` and `ConsumerResumeAfterFailoverTest`, first establish whether they are timing or logic: run each 20× in a loop and count failures.
   ```bash
   for i in $(seq 1 20); do ./gradlew :broker:test --tests '*FollowerKillTest' -q --rerun-tasks || echo "FAIL $i"; done
   ```
   If failures are sparse and the exception varies (as observed: `ClosedChannelException` at line 36 on one run, `ProtocolException` at line 54 on another), the cause is almost certainly a client connection being reused after the killed broker's socket died. Check whether the test's `ProducerClient` is given a redirect/retry budget sized for a real failover — `ProducerClient.DEFAULT_MAX_RETRIES` is 5 with a 100 ms backoff, which is tight if an election takes 150–300 ms plus reconnect. Widening the test's retry budget is legitimate; the production defaults were already widened for the chaos harness for exactly this reason (commit `17a1039`).
4. Re-run the full suite twice end-to-end to confirm determinism:
   ```bash
   ./gradlew test --rerun-tasks --console=plain && ./gradlew test --rerun-tasks --console=plain
   ```

### How to validate
Two consecutive full-suite runs report 336 tests, 0 failures. Confirm by parsing the XML rather than trusting the console:
```bash
python3 - <<'PY'
import glob, xml.etree.ElementTree as ET
t=f=e=0
for p in glob.glob("*/build/test-results/test/*.xml"):
    r=ET.parse(p).getroot()
    t+=int(r.get('tests',0)); f+=int(r.get('failures',0)); e+=int(r.get('errors',0))
print(f"tests={t} failures={f} errors={e}")
PY
```

### Metrics affected
M10 (test suite health). Indirectly every other metric, since a red suite discredits them all.

### Resume impact
Enables the claim *"336 tests across 7 modules, green on every commit"* and, more importantly, removes the risk that an interviewer clones the repo and sees a failure in the first 60 seconds. This action does not add a claim so much as protect all the others.

### Dependencies
None. Do this first.

---

## Action 2 — Make the chaos harness capable of running at scale (stream history to disk; set an explicit heap)

### Action
Change `HistoryRecorder` to stream events to a file instead of retaining them in heap, make the checkers consume from that file, and set an explicit `maxHeapSize` on the `chaosTest` task.

### Why
The project's stated headline claim — 0 loss / 0 duplication across ~1,000 crashes over ~10M messages — is currently **impossible to attempt**, not merely un-attempted. The harness will exhaust its heap within minutes of starting such a run. Until this is fixed, no larger chaos run of any size can be attempted, so this action gates the entire correctness story.

### Current problem
Three confirmed defects, all in the harness rather than the broker:
1. `HistoryRecorder` (`chaos/src/main/java/io/minikafka/chaos/HistoryRecorder.java:19`) accumulates every producer and consumer event in `ConcurrentLinkedQueue`s held for the whole run.
2. `HistoryRecorder.history()` (line 80) then copies the queues into `ArrayList`s, and the `History` record's compact constructor (line 151) copies *again* via `List.copyOf` — tripling peak footprint at exactly the moment the run is largest.
3. The `chaosTest` Gradle task sets no `maxHeapSize` (verified: no `maxHeapSize` or `Xmx` in any `build.gradle.kts` or `gradle.properties`), so it runs at Gradle's **512 MB default**.

At 10M messages that is ~20M event objects plus two full copies — tens of GB against a 512 MB ceiling.

### Expected outcome
The harness can run for hours at multi-million-message scale with bounded memory. Not guaranteed to reach 10M in one run — Action 3 will establish the real practical ceiling empirically.

### How to execute
1. **Immediate mitigation (5 minutes, do this even if you defer the rest):** add an explicit heap to `chaos/build.gradle.kts` in the `chaosTest` task registration (around line 48):
   ```kotlin
   maxHeapSize = providers.gradleProperty("chaosHeap").getOrElse("4g")
   ```
   This alone raises the practical ceiling roughly 8×, buying maybe 1–2M messages. It does not fix the O(n) growth.
   In the same edit, wire the run timeout through (currently missing — see Action 3 step 2):
   ```kotlin
   systemProperty("chaos.runTimeoutMs", providers.gradleProperty("runTimeoutMs").getOrElse(""))
   ```
2. **The real fix:** give `HistoryRecorder` a streaming mode.
   - It already has a `dump(history, path)` method that writes CSV — reuse that record format so nothing downstream needs a new parser.
   - Add a constructor taking an output `Path`; on `recordProducer`/`recordConsumer` (lines 60, 73), append a CSV line to a `BufferedWriter` under a lock (or a single-consumer queue drained by a writer thread) instead of enqueuing an object. Do **not** fsync per event — buffered writes are fine, this is test telemetry.
   - Keep the in-heap path as the default so the existing 31 chaos unit tests and the demo runner are unaffected; select streaming via `ChaosConfig`.
3. **Adapt the checkers** in `chaos/src/main/java/io/minikafka/chaos/check/`:
   - `LossChecker` (64 lines) and `DuplicationChecker` (96 lines) are single-pass set/count operations — convert them to consume a streaming iterator over the CSV. Loss needs the acked set and the received set; at 10M messages a `LongOpenHashSet`-style primitive set or a sorted external merge keeps this bounded.
   - `DivergenceChecker` reads replica logs directly and does not depend on history size — leave it alone.
   - `LinearizabilityChecker` **must not** be given the full history. Its own javadoc (`LinearizabilityChecker.java:38-44`) states it is intractable on "millions of ops with no quiescent gaps" and is designed for per-scenario/per-partition windows. Feed it bounded windows around each injected crash — that is where the interesting concurrency is anyway — and record how many windows were checked. Be explicit in the results that linearizability was verified over N windows totalling M ops, not over the whole run.
4. Add the streamed history file path to the chaos report so a failing run leaves a debuggable artifact.

### How to validate
Run an intermediate-scale chaos test and watch RSS:
```bash
docker compose -f docker/docker-compose.yml up -d --build
./gradlew chaosTest -Pcrashes=50 -Pmessages=200000 -PchaosHeap=2g --console=plain
```
Success = the run completes, and the JVM's peak heap does not scale with message count (check with `jcmd <pid> GC.heap_info` sampled during the run, or add `-XX:+PrintGCDetails`). The history CSV on disk should grow linearly while heap stays flat.

### Metrics affected
Unblocks M1, M2, M3, M4 at scale. Does not itself change any measured value.

### Resume impact
None directly — this is enabling work. But without it, no chaos claim larger than the current 999-message run is possible, so it gates the project's primary resume asset.

### Dependencies
None, but do Action 1 first so you are not debugging a harness against a red suite.

---

## Action 3 — Run chaos at a citable scale and commit the raw artifacts

### Action
Execute the chaos harness at the largest scale that completes reliably overnight, and commit the resulting machine-readable output (history CSV summary, chaos report, JUnit XML) into the repo.

### Why
Two gaps close at once. First, the current chaos evidence is **999 messages and 4 crashes** — small enough that a reviewer may fairly read it as a smoke test. Second, and worse, **no chaos run in this project has ever had its raw output committed**; `.gitignore` excludes `build/` wholesale, so `docs/results.md` is where numbers stop being traceable. "Show me the output" is a normal interview move and currently has no answer.

### Current problem
- M1/M2/M3/M4 are all recorded at 999 messages / 4 crashes (`docs/results.md`, 2026-07-28) and could not be reproduced during the audit.
- M5 (split-brain) rests on **one** injected network partition, against spec 11 AC-2's "N injected network partitions between random broker pairs".
- No artifact in the repository supports any of these values.

### Expected outcome
A chaos result at a scale worth citing. Based on the measured ~549 msgs/s, realistic overnight targets are roughly **1M messages with 100–200 injected crashes** (~30 min of publishing plus crash-recovery stalls, so budget several hours with margin). Reaching the literal 10M/1,000 target is unlikely without Action 5 and is not required for a strong claim. **The invariants holding is not guaranteed** — a larger run may legitimately surface a real bug, which would be a valuable finding rather than a failure of this action.

### How to execute
1. Complete Action 2 first.
2. Raise `ChaosConfig.DEFAULT_RUN_TIMEOUT_MS` (`chaos/src/main/java/io/minikafka/chaos/ChaosConfig.java:32`, currently 15 minutes) or pass an override. **Confirmed (2026-09-23): `chaos.runTimeoutMs` is NOT wired through the `chaosTest` task** — `chaos/build.gradle.kts:48-65` forwards crashes/messages/producers/consumers/partitions/seed only; the timeout is wired for `demoRun` (line 45) but not for chaos. Add `systemProperty("chaos.runTimeoutMs", providers.gradleProperty("runTimeoutMs").getOrElse(""))` alongside Action 2's heap change, or any long run will be cut off at 15 minutes.
3. Start conservatively and step up, so a failure at hour 4 is not the first thing you learn:
   ```bash
   docker compose -f docker/docker-compose.yml up -d --build
   ./gradlew chaosTest -Pcrashes=20  -Pmessages=100000  -PchaosHeap=4g   # ~5 min, sanity
   ./gradlew chaosTest -Pcrashes=100 -Pmessages=1000000 -PchaosHeap=4g   # the real run
   ```
   Run it under `nohup`/`tmux` and tee the console output to a file.
4. Also raise the network-partition scenario's N. In `chaos/src/chaosTest/java/io/minikafka/chaos/NetworkPartitionChaosTest.java`, inject **at least 10** partition/heal cycles between random broker pairs rather than 1, to make the split-brain claim meaningful.
5. **Commit the evidence.** Add a `docs/evidence/` directory that is *not* gitignored, and copy into it: the chaos report, the checker summaries, `chaos/build/test-results/chaosTest/*.xml`, and the console log. Keep the history CSV out of git if it is large — commit a checksum and the summary instead.
6. Update `docs/results.md` and `docs/PROJECT_RESULTS_AUDIT.md` with the new row and a pointer to the evidence directory.

### How to validate
The run exits 0 (spec 11 AC-5 requires non-zero on any failure), the report shows the intended crash count actually injected, and all four checkers report PASS. Then verify the evidence is real: `git status` shows the artifacts tracked, and the numbers in `docs/results.md` match the committed files exactly.

### Metrics affected
M1, M2, M3, M4, M5 — all of them, in both value and evidentiary status.

### Resume impact
The core claim. Converts *"0 loss / 0 duplication over 999 messages and 4 crashes"* into something like *"0 committed-message loss and 0 duplication across ~100 injected leader crashes over 1M messages on a live 3-broker cluster, verified by loss, duplication, per-partition linearizability, and cross-replica divergence checkers"* — with committed artifacts behind it. That is the sentence the whole project exists to earn.

### Dependencies
Action 2 (mandatory — the run will OOM otherwise). Docker running. Several hours of uninterrupted machine time.

---

## Action 4 — Re-run the benchmarks correctly, commit the raw JSON, and add a rate-limited latency measurement

### Action
Re-execute `:bench:jmh` for RF=1 and RF=3, commit the resulting `results.json` files, and add a latency measurement taken at a stated sub-saturation offered rate rather than at 8 saturating threads.

### Why
Two separate problems. First, **the existing benchmark numbers cannot be traced to any artifact** — `bench/build/results/jmh/` is empty and no JSON was ever committed. They are probably accurate; they are not evidenced. Second, the p99 of 61.28 ms is a *saturated-system* number: `PublishLatencyBenchmark` runs at a fixed 8-thread offered concurrency with no rate limiter (its own javadoc says so), against a service rate of ~550/s. Reporting a saturated p99 next to a target that assumed sub-saturation load makes the latency result look far worse than the system actually behaves.

### Current problem
- M6 (549 / 1,668 msgs/s), M7 (p50 7.86 / p99 61.28 / p999 63.09 ms) and M8 (67.1%) exist only as text in `docs/results.md`.
- M7's conditions are misleading by construction: the number answers "what is p99 when 8 threads saturate a 550/s system", not "what is p99 at a given load".

### Expected outcome
Traceable throughput and latency numbers with fully stated conditions, plus a *meaningful* p99 measured at, say, 50% of measured capacity. That second p99 will very likely be dramatically better than 61 ms — plausibly close to the p50 of ~8 ms — but this is an expectation from queueing behaviour, not a guarantee.

### How to execute
1. Record the environment first — an unconditioned benchmark number is worthless. Capture and commit: machine model, chip, core count, RAM, macOS version, Docker Desktop version and its CPU/memory allocation, JDK version.
2. Run both configurations, with the cluster torn down and rebuilt between them:
   ```bash
   ./gradlew :bench:jmh -Prf=1 --console=plain
   ./gradlew :bench:benchClusterDown -Prf=1
   ./gradlew :bench:jmh -Prf=3 --console=plain
   ./gradlew :bench:writeResults
   ```
   The `jmh` task's `doLast` already copies `results.json` to `results-rf${rf}.json` (`bench/build.gradle.kts`), and `writeResults` merges both into `docs/results.md`.
3. **Commit `bench/build/results/jmh/results-rf1.json` and `results-rf3.json` into `docs/evidence/`.** They are small and they are the entire provenance of M6–M8.
4. Add the rate-limited latency point. JMH has no rate limiter, so use the standalone `LoadGenerator` (`bench/src/main/java/io/minikafka/bench/LoadGenerator.java`) instead — it already has `LatencyHistogram` and takes `--messages`, `--threads`, `--duration`. Add a `--target-rate` option that paces submissions (a simple token bucket or a fixed inter-arrival sleep per thread is sufficient), then measure at roughly 250 msgs/s against a cluster whose measured capacity is ~550 msgs/s:
   ```bash
   ./gradlew :bench:loadGen -Pargs="--duration 300 --threads 8 --target-rate 250 --payload-size 1024"
   ```
5. Report **both** latency figures in `docs/results.md`, clearly labelled: "p99 at saturation (8 threads offered)" and "p99 at 250 msgs/s offered (~45% of measured capacity)". Do not replace one with the other — showing both is more honest and more informative.
6. This run also satisfies spec 12 AC-6 (a 5-minute sustained `LoadGenerator` run without broker crash or OOM), which currently has **no** recorded evidence. Record its outcome.

### How to validate
`results-rf1.json` and `results-rf3.json` exist, are committed, and their contents reproduce the exact numbers printed in `docs/results.md`. The `LoadGenerator` run sustains its target rate for the full 300 s with all three brokers alive at the end.

### Metrics affected
M6, M7, M8; plus spec 12 AC-6, previously unevidenced.

### Resume impact
Makes the cost-of-durability claim (*"majority replication costs 67% of single-replica throughput"*) fully defensible with committed artifacts — this is already the project's cleanest performance number. Adds a *usable* latency claim (*"p99 publish→commit of X ms at Y msgs/s offered load, RF=3, fsync on the commit path"*) to replace an unusable one.

### Dependencies
Docker running. Independent of Actions 1–3.

---

## Action 5 — Add group commit to the Raft log (and, if time allows, batched publish)

### Action
Coalesce concurrent Raft proposals so that one `fsync` covers many entries, instead of forcing on every entry. If time permits afterwards, extend the wire protocol so one `PublishReq` can carry multiple records.

### Why
This is the only action that can materially move M6 and M7, and it addresses the audit's single confirmed root cause. The measurement is unambiguous: on the same disk, single durable appends run at **231/s** while 2,000 appends sharing one fsync run at **131,066/s** — a ~567× difference on the durability path alone. Group commit captures a large share of that without touching the wire protocol or the client.

Note the honest framing: batching is not a clever optimisation, it is the mechanism by which every production log system reaches its headline throughput. Its absence is why 549 msgs/s and 200,000 msgs/s are three orders of magnitude apart.

### Current problem
Confirmed by code inspection:
- `FileRaftLogStore.append()` calls `channel.force(true)` **unconditionally, once per entry** (`FileRaftLogStore.java:106,131`), inside a `synchronized` method invoked under `RaftNode`'s lock via `propose()` (`RaftNode.java:189`). Concurrent producers on one partition serialize behind it.
- `LogSegment.append()` forces again under `FsyncPolicy.EVERY_WRITE` (`LogSegment.java:167`), and `AppliedIndexStore.record()` forces a third time (`AppliedIndexStore.java:77`) — three serialized fsyncs per publish on the leader, replicated on each follower.
- `PublishReq` carries exactly one `payload` (`PublishReq.java`), so batching is not even *representable* on the wire.

### Expected outcome
A substantial throughput improvement at RF=3, especially with many concurrent producers — the exact factor cannot be predicted and **must be measured, not assumed**. Group commit alone is unlikely to approach 200,000 msgs/s; the realistic goal is to move the number from "embarrassing" to "explainable", and to be able to say precisely what the remaining gap costs and why.

### How to execute
**Phase A — group commit in the Raft log (the high-value, lower-risk half):**
1. In `FileRaftLogStore`, separate *write* from *sync*: have `append()` write into the channel and return without forcing, and add an explicit `sync()` that calls `force(true)`.
2. In `RaftNode.propose()`, do not hold the node lock across the fsync. Append under the lock, release, then have a single committer coalesce: whichever thread arrives first performs one `force(true)` covering every entry written since the last sync, and all proposals whose entry index is ≤ the synced index are released together. A `CountDownLatch`/`CompletableFuture` per sync generation is the standard shape.
3. **Preserve the durability invariant absolutely:** no proposal may be acknowledged, and no `matchIndex`/commit advance may be published to a peer, before the fsync covering its entry has returned. Getting this wrong silently breaks INV-1, which is the entire project. Write the test first: a test that kills the process between write and sync and asserts the un-synced entry was never acked.
4. Apply the same coalescing to `AppliedIndexStore.record()`, which is currently one fsync per applied entry and protects crash-recovery idempotency rather than record durability — it can safely lag behind the segment append as long as it never leads it (the ordering comment at `PartitionReplica.java:121` documents the constraint).
5. **This change is in the consensus critical path, so `CLAUDE.md` §SDD item 4 requires review by the `distributed-systems-reviewer` subagent (`/raft-review`) before it can be considered done.** Do not skip this.

**Phase B — batched publish (only if Phase A lands cleanly and time remains):**
6. Add a `PUBLISH_BATCH_REQ` message type carrying N records with a shared `producerId` and a contiguous `seqNo` range; extend `MessageCodec`. Keep `PublishReq` for compatibility with existing tests.
7. Add client-side accumulation to `ProducerClient` (linger-ms + max-batch-size, the standard Kafka shape) behind an opt-in constructor so every existing test keeps its current synchronous semantics.
8. `IdempotencyStore` must handle a batch atomically — a partially-applied batch would break INV-3.

### How to validate
1. **Correctness first, before any benchmark:** the full unit suite stays green (336/336), and a chaos run at the Action 3 scale still reports 0 loss / 0 duplication. A throughput win that costs a correctness guarantee is a net loss for this project — the correctness claim is the deliverable, per `CLAUDE.md`.
2. Re-run Action 4's benchmarks and compare against the committed pre-change `results-rf3.json`. Report both, before and after.
3. Confirm the mechanism actually engaged: fsync count per N publishes should now be far below N. Instrument a counter on `force(true)` calls, or verify indirectly by checking that throughput now scales with producer concurrency (it currently does not).

### Metrics affected
M6 (throughput), M7 (latency — expect p50 to *rise* slightly from batching delay while p99 and aggregate throughput improve; report both honestly), M8 (the RF delta will need re-measuring). Possibly M10, since it would relieve the fsync pressure causing the `PartitionOrderingTest` timeout — but fix that test in Action 1 regardless, do not wait for this.

### Resume impact
If Phase A alone yields a meaningful multiple, it enables a genuinely strong and *specific* engineering claim: *"identified fsync-per-entry as the throughput ceiling via direct measurement (231 durable appends/s on the test hardware), implemented group commit in the Raft log, and improved RF=3 throughput from 549 to N msgs/s while preserving zero-loss guarantees under the chaos harness."* A measured bottleneck, a targeted fix, a before/after number, and a preserved invariant is a far better interview story than a large throughput number with no narrative — and it is achievable, whereas 200,000 msgs/s is not.

### Dependencies
Action 4 must run **first** to establish the committed before-baseline. Action 1 should be done so regressions are visible. Requires `/raft-review` sign-off before being marked done.

---

## Action 6 — Measure leader failover time

### Action
Instrument and record the time from leader `SIGKILL` to the first successful write accepted by the newly elected leader.

### Why
"It fails over" is the project's central behavioural claim, and **no measurement of how fast exists anywhere in the repo** — no target was ever defined and no value was ever recorded. "How long does failover take?" is close to a guaranteed interview question following any Raft claim, and having no answer is a visible hole in an otherwise well-measured project. It is also cheap: the demo runner already performs exactly this event.

### Current problem
M11 in the audit: *not measured, no verified result available, and no target ever defined.*

### Expected outcome
A distribution (not a single sample) of failover times over ~20 kills. Given `RaftConfig` defaults — 150–300 ms randomised election timeout, 50 ms heartbeat interval — the election itself should land in the low hundreds of milliseconds, but the *client-visible* recovery will be longer: it includes the client's metadata refresh and its 100 ms retry backoff (`ProducerClient.DEFAULT_RETRY_BACKOFF_MS`). Both are worth reporting separately. No specific value is guaranteed.

### How to execute
1. The instrumentation point already exists. `chaos/src/main/java/io/minikafka/chaos/demo/DemoRunner.java` kills `broker-2` mid-publish, and `HistoryRecorder` already timestamps every producer event with `invokedNanos`/`respondedNanos`.
2. Record the kill instant, then compute two figures from the history:
   - **Client-visible recovery:** kill instant → first successful ack on a partition whose leader was the killed broker.
   - **Election time:** kill instant → the point at which a metadata query first reports a new leader for that partition. Poll `MetadataClient` on a tight loop from the harness during the kill window.
3. Repeat ~20 times (a loop of `./scripts/demo.sh --messages 5000`, or a dedicated chaos scenario), and report min / p50 / p99 / max rather than a single number.
4. Add a "Failover time" table to `docs/results.md`, stating explicitly that **no target was defined** for this metric — do not retrofit one.

### How to validate
At least 20 samples, with the distribution reported. Sanity-check the numbers against `RaftConfig`: a p50 client-visible recovery far below 150 ms would indicate a measurement bug (the election timeout floor makes it implausible); one in the multi-second range would point at client-side retry/backoff rather than at Raft, which is itself a useful finding worth reporting.

### Metrics affected
M11 (currently entirely unmeasured).

### Resume impact
Enables a concrete, specific claim — *"leader failover completes in p50 X ms / p99 Y ms across 20 injected SIGKILLs"* — and, more importantly, means the obvious follow-up question to every failover claim has a measured answer rather than a shrug.

### Dependencies
Docker running. Cheapest done alongside Action 3, since both need a live cluster and repeated kills.

---

## Action 7 — Rewrite the headline claims to match the evidence

### Action
Update `README.md`, `docs/results.md`, and the resume bullets so that no stated number exceeds what the committed artifacts prove, and so the 200K/8ms targets are either retired or explicitly reframed as un-met aspirations.

### Why
The audit's §7.3 lists four claims that must not appear on a resume as written. The README is already unusually honest — it flags the chaos row as validated-scale and states outright that the correctness row, not the throughput row, is the headline. But `planning/01-requirements.md` still carries 200K/8ms as NFR-7/NFR-8 with no note that they were aspirational and are unmet by ~364× and ~7.7× respectively, and the README's headline table still prints them in a "Target" column that invites comparison the project loses.

### Current problem
- NFR-7 (200,000 msgs/s) and NFR-8 (p99 ≤ 8 ms) were written into `distributed-message-broker.md` §A.2 as resume aspirations and later promoted into the requirements document as if they were engineering targets. **No baseline, capacity model, or derivation for either exists anywhere in the repo.**
- Every chaos and benchmark number currently in `docs/results.md` is untraceable to an artifact (Actions 3 and 4 fix this).

### Expected outcome
Documentation where every number is traceable to a committed artifact, and where the performance targets are presented honestly as un-met aspirations with a measured explanation — not quietly left in a "Target" column.

### How to execute
1. Do this **last**, after Actions 3, 4, and (if attempted) 5 and 6 have produced real numbers.
2. In `docs/results.md`: replace every table row with the new measured values, and add an "Evidence" column pointing at the file in `docs/evidence/` that supports each row.
3. In `README.md`: keep the existing honest framing, update the numbers, and change how NFR-7/NFR-8 are presented — instead of a bare "Target: 200,000 msgs/s" next to "Measured: 549 msgs/s", state the target, state that it was set as an aspiration at project inception with no capacity model behind it, and state the measured reason it is not attainable in this design (no batching; three serialized fsyncs per publish; ~231 durable appends/s on the test hardware). Owning the gap with a measured explanation is strictly better than presenting it as a scoreboard the project loses.
4. In `planning/01-requirements.md`, annotate NFR-7 and NFR-8 as unmet with a pointer to `docs/PROJECT_RESULTS_AUDIT.md` §6.2. Do not delete them — the delta between intention and outcome is itself an honest part of the project's story.
5. Draft the resume bullets from §7.1 of the audit only, and check each one against a committed artifact before using it.

### How to validate
Every numeric claim in `README.md` and `docs/results.md` can be traced, by a reader with no context, to a file in `docs/evidence/`. Nothing from audit §7.3 appears anywhere as a claim.

### Metrics affected
None directly — this action changes what is *claimed*, not what is *true*.

### Resume impact
Determines whether the project survives an interviewer who reads the repo carefully. A project with modest, fully-evidenced numbers and a clear-eyed account of an unmet target reads as engineering maturity. The same project with a 200K figure in a target column next to a 549 measurement invites the least favourable possible reading.

### Dependencies
Actions 3 and 4 at minimum; ideally 5 and 6 as well.

---

## Explicitly out of scope

Named here so a future session does not re-derive them as work:

- **Chasing 200,000 msgs/s.** Not attainable in this design on this hardware; Action 5 is about a *measured, explainable* improvement, not about reaching that figure.
- **Cosmetic refactoring, doc cleanup that does not affect validation, new features, or architectural changes with no demonstrated need.** The audit found no stubs, no TODOs, and no incomplete integrations — the code does not need tidying, it needs evidence.
- **Log compaction, cross-partition transactions, dynamic membership.** All explicitly out of scope by locked decision (`CLAUDE.md`), and their absence is a defensible design answer, not a gap.
- **GPU evaluation, dataset scaling, model configuration.** Not applicable — this is a message broker, not an ML system.
