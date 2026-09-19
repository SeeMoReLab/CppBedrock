# CppBedrock for Adaptive Timers

CppBedrock runs PBFT, LinearPBFT, SBFT, HotStuff variants, and Zyzzyva as YAML-configured state machines. In the adaptive-timer repo each replica is one process, the committee comes from a JSON file, and clients talk to replicas over gRPC streams. The CloudLab harness (`scripts/cloudlab/run_pbft.sh`, `run_sbft.sh`, both over `lib_bedrock.sh`) drives everything below; this file documents the pieces for local use. PBFT and SBFT are the protocols maintained for the adaptive-timer experiments; the other configs still load but have not been ported to the request timer, view change, and failure-spec machinery described here.

## Build

Dependencies: cmake, a C++17 compiler, gRPC and protobuf (with `grpc_cpp_plugin`), yaml-cpp, nlohmann-json, OpenSSL, libxml2 (parses `failure_spec.xml`; part of the macOS SDK, `libxml2-dev` on Ubuntu). On Ubuntu `scripts/cloudlab/setup.sh` installs them; on macOS use Homebrew.

```bash
cmake -S CppBedrock -B CppBedrock/build -DCMAKE_BUILD_TYPE=Release
cmake --build CppBedrock/build -j
ctest --test-dir CppBedrock/build      # tests: request timer, task scheduler, failure spec, consensus
```

Sanitizer validation: the consensus suite passes with ASan and UBSan. Instrumented live streaming against the local prebuilt, non-instrumented Homebrew Protobuf 34.1 library triggers a container-annotation mismatch while parsing packed repeated IDs; this was reproduced using only Protobuf serialization/deserialization, without the consensus or networking code. Build Protobuf with matching sanitizer settings for full instrumentation. The live smoke test also passes with `ASAN_OPTIONS=detect_container_overflow=0`, retaining other ASan checks and UBSan. See the upstream [mixed-instrumentation explanation](https://github.com/google/sanitizers/wiki/AddressSanitizerContainerOverflow).

The learning-agent contract is the repo-wide `proto/agent.proto` (CMake reads `../proto/agent.proto`; override with `-DBEDROCK_AGENT_PROTO=<path>` when building outside the repo). CppBedrock keeps no copy of it.

## Keys and committee

Replica ids are 0-based and must be exactly `0..n-1`; the leader of view `v` is replica `v mod n`. PBFT/SBFT require exactly `3f+1` replicas. Every replica needs its own private key and all committee public keys. Replica authentication is mandatory, including votes and recovery messages.

```bash
# keys: server_<id>_private.pem / _public.pem for ids 0..3 plus client_private.pem
bash CppBedrock/keys/gen_server_keys.sh 4 netcfg/keys
# committee: {"replicas": [{"id": 0, "host": "127.0.0.1", "port": 15000}, ...]}
python3 scripts/cloudlab/gen_committee.py --workspace bedrock --ips 127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1 --port-stride 1 --front-port 15000 --out-dir netcfg
```

`gen_committee.py --workspace bedrock` writes both the keys and `committee.json`.

## Run replicas

```bash
CppBedrock/build/CppBedrock --node-id 0 --committee netcfg/committee.json \
  --protocol-config CppBedrock/config/config.pbft.yaml --keys-dir netcfg/keys
```

Options: `--initial-timeout <ms>` (election timeout, overrides `timers.view_change_ms`), `--initial-slow-path-timeout <ms>` (SBFT slow-path timeout, overrides `timers.fast_path_wait_ms`), `--failure-spec <xml> --failure-start-unix-ms <ms>` (proposal-delay injection, see below), `--stats-interval <ms>`, `--verbose`.

Replicas also report engine CPU per second as `propose_us`, `validate_us`, and `execute_us`. All three run under the single engine lock, so they measure the serialized cost of a batch rather than wall-clock latency; they are the first place to look when throughput is below the proposal cadence.

Every replica logs one `Stats` line per second with the same `[server_<id> <timestamp>]` prefix the other protocols in the repo use: `view`, `leader`, `view_changing`, `executed_seq` (contiguous execution including state transfer), `stable_checkpoint`, `committed_transactions` (unique executed requests, including state transfer), `rejected`, `committed_total` (locally executed batches), `commits`, `requests`, `inflight`, `pending` (buffered client requests), `timer_pending` (request-timer entries), `view_changes`, `new_views`, `fast_path`, `slow_path`, `delayed_proposals`, `clients`, `replies`, `send_failures`, `election_timeout_ms`, `slow_path_timeout_ms`.

## Benchmark client

```bash
CppBedrock/build/bedrock_client --committee netcfg/committee.json --rate 1000 --duration 60 \
  --target-mode broadcast --connections 4 --request-timeout 10000 [--sign true --client-key netcfg/keys/client_private.pem]
```

The client opens `--connections` streams to every replica, sends at the total `--rate` (broadcast mode: every request to every replica, which is what the request timer below expects; leader mode: to the current leader, following the leader hints replicas put in every reply), counts a request as successful once `f+1` distinct replicas replied with success, and prints one Monitor line per second in the SmartBFT format:

```
[client <ts>] Monitor duration=1.000s trxs=N succ=N err=N tps=X avg_ms=X p50=N p95=N p99=N max=N
```

`tps` reports successful requests per second; `trxs` includes both successes and errors. Explicit admission rejections and request timeouts are counted separately in the final client log. In broadcast mode, every target must reject before the client declares admission rejection: `n-f` local rejections do not exclude acceptance and later execution by another replica. A success reply supersedes that replica's earlier rejection. If a target is unavailable, unresolved requests use the client deadline. In leader mode, the target leader's rejection suffices. Admission rejection is not a consensus-backed transaction-abort certificate. Replies may coalesce up to 128 request IDs with identical results into one gRPC write, with at most 1 ms of transport buffering.

`--start-unix-ms` delays submission to a shared start time (the harness uses it) and prints the `Waiting until benchmark start unix ms ...` line the plot scripts look for.

## Periodic batching

PBFT and SBFT propose at a fixed, configurable cadence. Arrivals never trigger an early proposal, even when a batch reaches its size limit. At each tick, the leader takes the oldest pending requests up to both the count and byte limits, and starts one consensus instance. Multiple batches may be in flight. Empty ticks are skipped; a full pipeline skips the tick; missed ticks are not replayed in a catch-up burst. A leadership change cancels the old schedule. Request order within a batch is authenticated, preserved during view changes and recovery, and executed before advancing to the next sequence.

| Replica / CloudLab runner option | Default | Meaning |
| --- | ---: | --- |
| `--proposal-interval` | 100 ms | Fixed cadence, separate from election and SBFT timers |
| `--batch-max-requests` | 8192 | Maximum requests in a proposal; hard maximum 8192 |
| `--batch-max-bytes` | 524288 | Conservative encoded request-byte budget; hard maximum 512 KiB |
| `--max-inflight-batches` | 16 | Maximum proposed but unexecuted batches; hard maximum 16, the consensus window. Keeping the cadence at a 150 ms one-way delay needs about six, since a decision takes roughly four one-way trips |
| `--max-pending-requests` | 32768 | Maximum requests in each of the ingress and pending queues |
| `--max-pending-bytes` | 16777216 | Maximum request bytes in each of those queues |

The byte limit is part of the authenticated protocol configuration and must match on every replica. Larger committees may require a smaller byte limit: startup rejects combinations whose worst-case view-change evidence exceeds the control-message budget. The default supports four- and seven-replica committees. Followers also enforce the protocol batch count and byte bounds. Oversized individual requests are rejected explicitly.

At 40,000 requests/s and 100 ms, each tick needs to accommodate about 4,000 requests. The byte limit can bind before the count limit, depending on payload size. These defaults are a starting point for measurement, not a throughput guarantee. For example:

```bash
bash scripts/cloudlab/run_pbft.sh /path/to/cloudlab_key normal --rate 40000 --no-failure-spec --proposal-interval 100 --batch-max-requests 8192 --batch-max-bytes 524288
# Substitute run_sbft.sh for SBFT. All options above are also replica CLI options.
```

Broadcast admission is local to each replica, so backups can accept requests rejected by a full leader. When a watched request remains unproposed, a backup relays its oldest pending requests in an authenticated, view-scoped batch. The leader reserves one count- and byte-bounded relay batch per peer, separately from its client queue, and serves ordinary ingress and peer batches in rotation, filling spare batch space from the next source. Relay memory is bounded by `(n-1) * batchMaxBytes` plus container overhead. Duplicate/in-flight requests are skipped; repeated relays cannot replace an occupied reservation; a view change clears reservations while backups retain their pending requests for retransmission. Forwarding retries do not reset the watched-request deadline. Relaying is a liveness measure against a leader that looks stuck, not a data path: with a broadcasting client the leader almost always holds the request already. A backup relays only after its watched entry has waited a quarter of the election timeout, relays a given watched entry once, and retries only after half the election timeout. Relaying on every proposal tick instead sent a batch several times a second to a leader that was already behind, which turned a slow leader into a stopped one.

Ingress is bounded before consensus processing. Excess requests receive `overloaded` instead of silently joining an unbounded queue. A separate bounded reply queue and writer per client stream prevent slow client reads from blocking consensus; a full reply queue explicitly closes the affected stream. Public keys are cached for signature verification. The 40k target must be measured using successful client TPS and replica progress, with rejections, timeouts, and latency reported alongside it.

Client identities retain exact replay protection using disjoint request-ID intervals. Sequential clients produce compact checkpoints; gaps and out-of-order IDs are preserved. Storage can still grow with the number of clients and fragmented ID ranges. No IDs are silently expired.

A reproducible local four-replica test verifies client outcomes and replica convergence, retaining logs and `results.json` in a temporary directory:

```bash
python3 CppBedrock/tests/batching_load_test.py --target-mode leader --rate 40000 --duration 60
python3 CppBedrock/tests/batching_load_test.py --target-mode leader --rate 10000 --duration 8 --batch-max-requests 128 --max-pending-requests 512 --initial-timeout 250 --expect-rejections
# Exercise divergent admission across replicas, as in the CloudLab default:
python3 CppBedrock/tests/batching_load_test.py --target-mode broadcast --rate 10000 --duration 15 --batch-max-requests 128 --max-pending-requests 512 --initial-timeout 1000 --expect-rejections
python3 CppBedrock/tests/batching_load_test.py --target-mode broadcast --rate 40000 --duration 60 --allow-rejections
python3 CppBedrock/tests/batching_load_test.py --rate 1000 --duration 6 --pause-replica 0
```

Local validation on 2026-09-16, Release build, four replicas and one client on the same host, four client lanes, leader targeting, default batching, and 60 seconds at 40,000 offered requests/s:

| Protocol | Successful requests | Errors / rejections / timeouts | Successful TPS including drain | Mean / p99 latency |
| --- | ---: | ---: | ---: | --- |
| PBFT | 2,400,000 | 0 / 0 / 0 | 39,928 | 88 / 117 ms |
| SBFT | 2,400,000 | 0 / 0 / 0 | 39,928 | 103 / 132 ms |

Both maintained approximately 40,000 successful TPS during the submission interval, with no view changes or stream failures. This is a local benchmark, not a guarantee for distributed deployments or larger payloads. Deliberately limiting batches to 128 requests and pending queues to 512 sustained approximately 1,280 successful TPS under 10,000 offered requests/s, explicitly rejecting excess work with zero timeouts. Separate tests paused either the leader or a backup for three seconds; both protocols completed all 6,000 requests and restored the paused replica's state.

The wire format is v3. Rebuild the client and all replicas and restart the complete committee together; v2 replicas and older clients are incompatible.

## PBFT/SBFT consensus, recovery, and view changes

The maintained engine lives in `src/core/EntityConsensus.cpp`, `EntityRequests.cpp`, `EntityViewChange.cpp`, and `EntityRecovery.cpp`. YAML configures timers and the client entry point; quorum thresholds and phase prerequisites are enforced by the engine.

- Consensus messages name a batch by digest and never carry its requests. A replica signature covers the proposal header (protocol, committee key set, view, sequence, leader, batch digest), so a batch can be detached from a proposal and reattached without disturbing the signature. A batch digest includes each request's operation, transaction, client id, request id, timestamp, and signature, with unambiguous field boundaries and request order, so the digest binds the batch exactly. Replicas reject conflicting proposals, proposals from nonleaders, invalid signers, invalid certificates, and any proposal carrying a partial batch. PBFT/SBFT reject `--proposal-signing false`; unsigned messages from older binaries are incompatible with this wire format. Rebuild and restart the entire committee together.
- PBFT prepares require the primary's proposal and `2f` matching backup votes, including a backup's own vote. Commit requires local prepared state and `2f+1` matching commits, including the local commit. Messages that arrive early are buffered and reevaluated when their prerequisites arrive.
- Certified decisions execute in contiguous sequence order. A later decision cannot execute across a missing slot. Request deduplication survives log pruning and state transfer, and duplicate client requests receive their cached success response.
- Every two executed batch slots, replicas sign a digest of the balances and request replay table. `2f+1` matching signatures certify a checkpoint. Only this certified prefix may be pruned. A four-batch window bounds consensus evidence above the checkpoint; new proposals wait when the window is full.
- Recovery exchanges a certified checkpoint snapshot when the requester is missing that prefix, and signed decision proofs for the missing suffix. Replicas already past the checkpoint repair their suffix without retransferring application state. A replica verifies the checkpoint quorum, snapshot digest, and decision evidence before installing state or executing the missing suffix. Maintenance runs every 250 ms; recovery requests are limited to once per second and normal recovery waits for a progress stall, including while waiting for NewView. Maintenance retransmits votes every tick but a proposal only once per second and only to replicas whose vote has not arrived, since a batch is orders of magnitude larger than a vote. A replica that has already requested a higher view can learn certified decisions from older views without voting in those older views. Full snapshots use the 64 MiB gRPC message limit; this implementation does not implement incremental or chunked snapshots. The replay table is retained for the lifetime of the run, compressed into exact per-client ID intervals.
- PBFT and SBFT share one watched-entry request watchdog. It selects the oldest pending client request or accepted batch sequence and measures the election timeout from when that entry becomes watched. Executing the watched entry grants the next oldest entry a fresh timeout; arrivals, duplicate requests, and execution of other entries do not reset it. Batch execution removes all its request and sequence entries atomically. Original acceptance timestamps remain intact for ordering and latency accounting, including across view changes. Followers track accepted batch sequences even when requests were sent only to the leader.
- Request expiration starts a view change; a replica also joins when `f+1` authenticated replicas request higher views. Each timeout callback checks both its generation and deadline after acquiring the engine lock, so completion or an adaptive timeout update invalidates stale callbacks. Adaptive election-timeout updates recompute the request deadline from the start of the current watch without restarting elapsed time.
- NewView waiting uses a separate scheduler deadline, armed only after `2f+1` replicas have joined the view change or while fetching the certified checkpoint of a valid NewView. Request watching is suspended during view change. Repeated NewView messages and recovery updates do not restart this wait. Consecutive NewView-wait expirations double the wait duration (saturating at `INT_MAX` milliseconds); execution of a previously unexecuted client request in normal operation resets that backoff. The configured/agent-provided election timeout remains the base value. SBFT's fast-to-slow-path timer is independent and unchanged.
- View-change evidence carries proposal headers, not batch bodies, so its size depends on the committee and the four-slot window rather than on the batch size. Measured on a four-replica committee, a ViewChange is about 3 to 17 KB and a NewView about 11 to 60 KB, whatever `batch-max-bytes` is set to. Carrying bodies instead made a NewView grow with the batch size, and periodic view-change retransmission then consumed the bandwidth the view change needed to finish, so a view change under load never completed.
- A replica that is missing the batch a decided or re-proposed digest names asks its peers for it (`BatchRequest`/`BatchResponse`) and accepts a body only when it hashes to the digest its own certified evidence names. It votes and executes only once it holds that batch, so no replica ever votes for bytes it has not seen, and execution waits for the batch rather than treating a missing one as an error. Executed batches stay in the log until a stable checkpoint prunes them, so a replica can still serve them to one that is catching up.
- ViewChange includes a certified checkpoint and signed prepared/committed evidence. SBFT additionally carries each replica's latest fast-path vote. NewView includes `2f+1` ViewChanges and signed re-proposals. Every recipient independently verifies the evidence and recomputes the safe selection. Slots between the checkpoint and the last selected request are filled with empty batches when no value must be preserved. A replica missing the selected checkpoint first obtains and verifies its state, then votes in the new view.
- Installing a view clears all transient per-view phase and timer flags, preserves certified evidence, starts a fresh watch on the oldest remaining entry, and schedules buffered requests. Delayed proposal injection does not block message handling.

This remains an in-memory benchmark implementation: checkpoints and voting history are not persisted to disk, so crash/restart with the same replica identity is not a supported fault model. State transfer supports lagging or temporarily disconnected running replicas. Client broadcasting is still required to preserve requests that only reached an unavailable leader before it proposed them. The client stream remains a benchmark interface, not an authenticated production client protocol.

Watchdog regression: with 10,000 offered requests/s, 128 requests per 100 ms batch, a 512-request pending limit, and a 250 ms election timeout, both protocols completed 10,752 of 80,000 requests in the eight-second submission test. They rejected the other 69,248, sustained approximately 1,283 successful TPS including drain, and had zero client timeouts or view changes. Successful request latency was roughly 400 ms, longer than the watchdog timeout, while each watched entry made timely progress. A separate three-second leader pause at 1,000 offered requests/s recovered all 6,000 requests with zero errors for both protocols. A 15-second run at 40,000 offered requests/s completed all 600,000 requests in each protocol with zero errors or view changes, approximately 39,700 successful TPS including drain. These are local four-replica checks.

Broadcast overload validation is separate from the earlier leader-targeted benchmarks. With 10,000 offered requests/s, 128 requests per batch, a 512-request pending limit, and a 1,000 ms watchdog, the final 15-second broadcast regression completed 19,912 requests in PBFT and 19,886 in SBFT, about 1,273 and 1,255 successful TPS including drain. Excess requests were explicitly rejected, with zero timeouts or view changes and matching replica/client transaction totals. The CloudLab run `20260916_203931` exposed divergent local admission: backups watched requests rejected by the leader, while the client classified some later-committed requests as rejected after only `n-f` rejection replies. The regression now defaults to broadcast routing, checks that replica transaction totals match client successes, and rejects runs with unexpected view changes or client timeouts. A local 60-second broadcast attempt configured for 40,000 requests/s submitted and completed 1,486,500 requests with zero errors, about 24,616 successful TPS including drain; it failed the offered-rate check and does not establish 40k broadcast capacity.

### Sustaining 40,000 requests/s

Four replicas in network namespaces on one 32-core CloudLab host (`amd244.utah.cloudlab.us`), broadcast client on the same host, 100-second runs at 40,000 offered requests/s. `full cadence` counts the seconds in which the leader reported `ticks=10 batches=10`, which is what separates a healthy run from a saturated one.

| One-way delay | Election timeout | Successful TPS | p50 | p99 | max | Peak pending | Full cadence | Elections |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5 ms | 500 ms | 39,806 to 39,918 | 151 ms | 205 ms | 244 ms | 4,300 | 101 of 101 | 0 |
| 5 + 50 ms | 1,000 ms | 39,837 to 39,869 | 313 ms | 367 ms | 396 ms | 12,132 | 99 of 100 | 0 |
| 5 + 150 ms | 2,000 ms | 36,300 to 39,600 | 599 to 637 ms | 751 to 8,297 ms | - | 26,287 to the cap | 79 to 100 of 100 | 0 to 3 |

A seventh fix, **not re-deriving what the leader just computed**, changed the 150 ms regime substantially. `proposeBatch` assembles a batch, digests it and signs it, then handed the envelope to the same path a proposal from the network takes, which verified the signature this replica had just produced and recomputed the digest over half a megabyte it had just hashed - about 87 ms of every second on the thread whose wall clock decides whether the cadence holds. An interleaved A/B on one host, alternating the commit in and out so drift could not bias it:

| | TPS | p50 | p99 | full cadence | elections |
| --- | ---: | ---: | ---: | ---: | ---: |
| leader skips its own re-derivation | 38,533 and 38,596 | 637 and 634 ms | 4,848 and 3,698 ms | 79 and 88 of 100 | 2 |
| leader re-derives | 30,835 and 31,956 | 1,314 and 1,235 ms | 8,832 and 8,757 ms | 10 and 10 of 100 | 3 |

Twenty-five percent more throughput and the cadence restored, from removing work that cost a tenth of the engine budget - because getting the tick back under its budget breaks the feedback loop rather than merely shaving it. A later pair of runs reached 39,438 and 39,602 TPS with no elections and 99 to 100 full-cadence seconds, so the target is now reachable in this regime; roughly half of runs still tip into the slow state during the transient when the injected delay steps up.

Two further ideas were tried and removed because measurement did not support them. **Moving rather than copying** a request into the pending pool removes the middle of three protobuf copies, but an interleaved A/B could not resolve it: 36,317 and 39,438 TPS with the change against 39,602 and 38,382 without, a within-pair spread larger than the difference. **Enlarging the pending pool** fails in both directions: the runs that elect are the ones that reach the cap during the transient, but raising the cap to 49,152 gave 32,349, 34,811 and 32,418 TPS with three to five elections and all three runs reaching the new cap, and 131,072 gave 21,573 with ten. Reaching the cap is a symptom of a run that has already fallen into the slow state, not its cause; the extra depth only adds queueing delay on top.

The first two regimes are reproducible across three runs each. **The 150 ms regime is not.** One run reached 39,737 TPS with no elections and has never reproduced: eight later runs across election timeouts of 500, 2,000, 3,000, 4,000 and 8,000 ms, pending caps of 32,768, 40,960 and 131,072, and batch caps of 4,096 and 8,192 all landed between 29,668 and 37,450 with one to three elections. The election timeout does not order the results - 2,000 ms beat 8,000 ms in one pair and lost to it in another - so the spread is run-to-run bistability rather than a timeout effect, and any single run in this regime is one sample of a wide distribution.

The mechanism is a feedback loop at the cadence boundary. Requests in flight are the offered rate times the time to order one, so they scale with delay: about 4,300 at 5 ms, 12,100 at 55 ms, 24,800 at 155 ms. At 155 ms that is 76 percent of the 32,768 pending cap, and batches become byte-limited at about 5,500 requests where the cadence only needs 4,000. The leader's proposal tick then costs 73 to 81 ms of its 100 ms budget. Lose one tick and the backlog grows, which makes batches larger, which makes the tick slower, which loses more ticks; the pool reaches its cap, admission diverges between replicas because each rejects a different subset, and a backup ends up watching a request the leader rejected and never ordered, which eventually elects a healthy leader out of office.

Raising the pending cap does not break the loop, it only changes which end fails: 131,072 let a replica queue 3.3 seconds of traffic instead of shedding it, giving 21,573 TPS with ten elections, and 40,960 filled to its own cap. Capping the batch at 4,096 requests so the cadence rather than the byte budget governs gave 33,918 TPS and 34 of 100 full-cadence seconds, inside the same band.

Leader engine time per second at 155 ms shows how little headroom is left: 205 ms admitting, 187 ms executing, 90 ms assembling, 87 ms validating, 57 ms signing, about 831 ms of every second inside the single engine lock, against roughly 660 ms at 5 ms one-way. The load generator on the same host takes another seven of the 32 cores. Four replicas plus the client on one host is at its limit here. The untried levers are a second host for the client, dropping the leader's redundant validation of its own proposal (87 ms/s spent re-digesting a batch it just digested and re-verifying its own signature), reducing per-request admission cost further, or a lower target rate at 150 ms.

Getting the first two regimes there took six fixes. The first run at 40,000 offered managed 25,256 TPS at 5 ms and 6,949 TPS at 155 ms.

**Per-request bookkeeping in ordered maps of strings** (25,256 to 39,856 TPS at 5 ms). The leader held the engine lock 657 ms of every second: 224 ms admitting, 237 ms executing, 77 ms assembling, 74 ms validating, 45 ms signing. The pending pool sits at its cap whenever offered load exceeds capacity, so each request walked an ordered map of strings roughly 75 times across the pool, the watchdog's two indexes and the replay filter, which also split a key the caller had just joined. The pool and the watchdog's key index are hashed, and `ExecutedRequests` takes the client ID and request ID directly. Cadence went from 5.5 to 11 batches/s.

**A digest recomputed on the hot path.** `carriesBody()` recomputed the batch digest, so it was linear in the batch, and `advanceConsensus()` calls it once per arriving vote. `bodyMatchesDigest()` is now the authoritative check, used where an incoming proposal is validated and where a fetched batch is installed; `carriesBody()` answers in constant time from the invariant validation establishes, that a stored proposal with a non-empty request list has already been checked against its digest.

**Missing TCP tuning in the harness** (6,949 to 14,853 TPS at 155 ms). `scripts/cloudlab/lib_bedrock.sh` did not apply the namespace socket-buffer tuning that `lib_rustbft.sh` already applied for this regime: at 100 to 200 ms one-way the default send buffer caps a stream well below what batch dissemination needs, and slow start after idle re-throttles it between proposal ticks.

**netem jitter** (14,853 to 30,865 TPS at 155 ms). The applier defaults to 20 percent jitter, so a 150 ms phase ran as `delay 155ms 31ms`. Jitter reorders packets and TCP treats reordering as loss; a 512 KiB batch is hundreds of packets. `scripts/apply_network_failures.py` says so in its own comment. High-throughput specs must set `<globalNetworkDelay><jitterRatio>0</jitterRatio>`.

**Per-batch work done per request** (30,582 to 39,869 TPS at 55 ms). Execution replied to each request separately, and each reply cost a registry lookup, two mutex acquisitions and a condition-variable wake; `ClientStreams` now takes a batch of replies for one client. The balance lock was taken per transaction although execution already runs under the engine lock, and is now taken per batch. `validateProposal` built a set of heap-allocated request keys to reject a batch naming the same request twice, and now sorts string views into the batch's own bytes.

**Retransmission that fired when nothing was lost.** Maintenance resent every vote for every unexecuted sequence every 250 ms, and the leader resent the full proposal to every peer whose prepare had not arrived. At 150 ms a prepare for a batch proposed 100 ms ago has not arrived because nothing was lost, so the leader pushed megabytes of batches a second into a link already carrying the real traffic. Retransmission is now gated on execution having stalled: execution is in sequence order, so a message a replica needs and never received stops `lastExecuted_` advancing. Votes go only to peers whose own vote is missing, since a live peer that had the message would have answered it. Fetching a missing batch body stays unconditional, because that is what unblocks execution.

**A scan of every in-flight request, per tick.** `acceptProposal` did not mark the requests a batch carries as in flight, although `installBatchBody` and `installReProposal` both did, so a replica could not tell a request waiting its turn in the leader's queue from one the leader never received except by walking every unexecuted batch and building a key per request. `forwardPendingRequests` paid that per relay decision and `proposeBatch` paid it on every tick for as long as any relayed batch was outstanding. `acceptProposal` now marks its batch like the other two paths.

Two couplings were removed along the way, and the rule is worth stating: **nothing in the retransmission or relay path should derive its timing from the election timeout.** An earlier retransmission gate used an eighth of it, harmless at the 8 s default, but once the timeout was calibrated down to 2 s it cost 7,000 TPS and thirteen elections. The relay grace still uses a quarter of it, which is why relaying becomes routine at 150 ms once the timeout is tuned below roughly 2.5 s. A learning agent that lowers the election timeout must not thereby make the system slower.

### Calibrated timeouts and proposal delays

The election timeout is a watchdog on *execution*, not on request latency: `PendingRequestTimer::selectWatchLocked` sets `watchedSince_` when an entry becomes watched, and the deadline is `watchedSince_ + timeout`, so a fresh deadline is armed every time the watched entry executes. It therefore measures how long ordering stalls. While the pipeline keeps up a 5 ms link and a 50 ms link look identical to it - both hold full cadence and execute a batch every 100 ms - so **network delay alone does not separate a small timeout from a medium one**. Good pipelining and a delay-sensitive election timeout are in tension. The separation comes from proposal delay, which stalls execution by a controllable amount, and this is why the failure spec needs its `<proposalDelay>` phases.

The ladder, measured at 40,000 requests/s:

| Election timeout | 5 ms | 5 + 50 ms | 5 + 150 ms |
| ---: | --- | --- | --- |
| 350 ms | 39,396 tps, no elections | 39,731 tps, no elections | collapses |
| 500 ms | 39,918 tps, no elections | 39,697 tps, no elections | 5,822 tps, 95 elections |
| 1,000 ms | - | 39,846 tps, no elections | 24,391 tps, 52 elections |
| 2,000 ms | - | 39,735 tps, no elections | 36,300 to 39,600 tps, 0 to 3 elections |
| 4,000 ms | - | - | 39,491 tps, no elections, 100 of 100 full-cadence seconds |

The 150 ms column also explains the bistability recorded above: the bad state was *triggered* by elections. At 4,000 ms there are none and the regime is stable at full cadence, so the instability was the timeout provoking view changes on a healthy leader, not a capacity wall.

Proposal delays, injected on the leader and re-resolved every 20 seconds so each run contains several episodes. `delayed` is the count of proposals actually held back, and it is the diagnostic that makes the result readable: a high count with no elections means the delay was absorbed, while a low count with elections means the first episode elected and the delay then stopped applying, because the new leader is not the targeted one.

| Proposal delay | 350 ms timeout | 1,000 ms timeout | 4,000 ms timeout |
| ---: | --- | --- | --- |
| 300 ms | 4 elections, 4 delayed | **absorbed**: no elections, 623 delayed, p50 148 to 461 ms | - |
| 450 ms | - | 11 elections, 149 delayed | (2,000 ms: 3 elections) |
| 600 ms | - | 3 elections, 223 delayed | (2,000 ms: 3 elections) |
| 1,000 ms | - | 4 elections, 9 delayed | **absorbed**: no elections, 537 delayed |

The stall a proposal delay causes is larger than the delay itself, because the pipeline drains before the held proposal lands, and `proposeBatch` allocates the sequence number before applying the delay, so a held proposal occupies a consensus-window slot while it waits. With a 2,000 ms large timeout no delay separated medium from large - 450, 600 and 1,000 ms all produced about three elections, one per re-targeting episode. Widening the large timeout to 4,000 ms resolves it, and costs nothing at 150 ms where 4,000 ms is the best-performing value anyway.

So the calibration is:

| | Value | Verified by |
| --- | ---: | --- |
| Small timeout | 350 ms | Clean at 5 ms; elects under a 300 ms proposal delay that the medium absorbs |
| Medium timeout | 1,000 ms | Clean at 5+50 ms; absorbs a 300 ms proposal delay; elects at 5+150 ms and under a 1,000 ms proposal delay |
| Large timeout | 4,000 ms | Clean at 5+150 ms at full cadence; absorbs a 1,000 ms proposal delay at both 5 ms and 5+50 ms |
| P1, absorbed by medium | 300 ms | No elections and 623 delayed proposals at 1,000 ms; 4 elections at 350 ms |
| P2, elects at medium only | 1,000 ms | 4 elections at 1,000 ms; no elections and 537 delayed at 4,000 ms |
| P3, absorbed by large at 50 ms | 1,000 ms | No elections and 488 delayed proposals at 4,000 ms with a 50 ms link |

One requirement is **not** met: a small timeout that keeps electing under a 50 ms link with no proposal delay. No value does this, because the replica cannot distinguish 5 ms from 50 ms while the pipeline absorbs both. The small-versus-medium distinction is available only through proposal delay.

### CloudLab validation under injected network delay

Four replicas in network namespaces on one 32-core host, broadcast client, `config/pbft/failure_spec.xml` (5 ms base one-way delay, rising to 55 ms at t=60 s), 150-second runs.

| Protocol | Offered | Fast-path wait | Successful TPS | p50 latency before / after the delay | Zero-throughput seconds | View changes | Errors |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| PBFT | 20,000 | n/a | 20,000 | 116 / 312 ms | 0 | 0 | 0 |
| PBFT | 40,000 | n/a | approximately 27,000 | 192 ms / 1.1 to 2.3 s | 0 | 0 | 0 |
| SBFT | 12,000 | 20 ms | 12,000 | 112 / 274 ms | 0 | 0 | 0 |
| SBFT | 20,000 | 20 ms | collapses | 115 ms / grows past 5 s | 30 | 3 | 0 |
| SBFT | 20,000 | 200 ms | 20,000 | 115 / 479 ms | 0 | 0 | 0 |

SBFT's fast path needs prepares from all `n` replicas. `config.sbft.yaml` sets `fast_path_wait_ms: 20`, which is sized for a sub-millisecond LAN: over a 50 ms link the spread between the `2f+1`-th and `n`-th prepare frequently exceeds it, so the collector falls back to the slow path and pays two extra message delays. Slow-path decisions went from none before the delay to 22 to 33 percent after, capacity fell from 20,000 to roughly 14,000, and the run then saturated and collapsed. Raising `--initial-slow-path-timeout` to 200 ms on the same 20,000 load restored a full 20,000 TPS with 11 slow-path decisions out of 1,220 and no view changes. This is the knob the learning agent tunes; the default suits a LAN, not a delayed link.

At PBFT 40,000 the offered load is roughly twice capacity, so the queues stay full, excess requests are rejected, and latency reflects queueing; throughput and view stability are unaffected. Three defects produced an unrecoverable stall in earlier runs (`20260917_173251`, `20260917_203733`), all now fixed:

- View-change evidence embedded whole batches, so a NewView grew to tens of megabytes and the 250 ms retransmission of that evidence consumed the bandwidth the view change needed to finish. A view change under load never completed and throughput went to zero permanently. Evidence now names batches by digest.
- A re-proposal installed without its batch was recorded as an empty batch and could then execute as empty, diverging from the committee. One replica detected this as `local state disagrees with a certified checkpoint`. Only a body verified against the signed digest now enters the log, and execution re-checks that digest before running a batch.
- Backups relayed a full batch to the leader every 250 ms whenever the watched entry was older than that. Under the injected delay every batch exceeded 250 ms, so relaying became continuous, stalled the leader, and fed back into itself. Relaying now waits a quarter of the election timeout and sends a given watched entry once.

## SBFT timer calibration

SBFT has two timers and they are not independent. Measured at 40,000 requests/s, four replicas in namespaces on one 48-core CloudLab host.

**The slow-path timeout** is how long the collector waits for the `n`-th prepare once it holds `2f+1`. Waiting costs the spread between those prepares; not waiting costs two extra message delays. So the winner is decided by the spread against the round trip, and a straggler - one replica whose outgoing packets are slow - is the knob that sets the spread:

| Regime | 500 ms wait | 20 ms wait | Winner |
| --- | --- | --- | ---: |
| 5 ms, no straggler | 39,908 tps, 100 percent fast | 39,940 tps, 96 percent fast | neither, identical |
| 5+50 ms, no straggler | 39,847 tps, 100 percent | 39,838 tps, 97 percent | neither |
| 5+150 ms symmetric | 37,429 tps, p50 659 ms, 95 percent | 33,796 tps, p50 1,088 ms, 27 percent | high |
| 5 ms, straggler +300 | 32,968 tps | 34,398 tps, 10 percent fast | low |

At 5 ms the round trip is 10 ms, below the 20 ms low setting itself, so no straggler can make the high value win at base delay. The crossover at 150 ms sits between a +350 straggler (high still won, 30,644 against 27,282) and +500 (low won, 29,040 against 24,199), so it is above `2d` rather than at it.

**The election timeout** is the same execution-stall watchdog as PBFT, but its floor is set by how long a view change takes to recover rather than by request latency. 350 ms and 1,000 ms cascade, re-firing during their own recovery: at 5+150 ms with no fault, 350 ms gave 4,338 tps with 122 elections and 1,000 ms gave 18,431 with 52, against 37,429 and no elections at 8,000 ms. Elections cost SBFT twice, because the fast path needs all `n` prepares and does not survive a view change. The usable ladder is 2,000 / 4,000 / 8,000.

Five of the six pairs have a regime where they win outright:

| Election | Slow path | Regime | Winner | Runner-up |
| ---: | ---: | --- | ---: | --- |
| 2,000 | 20 | 5 ms, straggler +300, leader stalled 5 s | 34,398 | 31,870 at 4,000; 9,429 at 8,000; 32,968 at 500 |
| 2,000 | 500 | 5+50 ms, straggler +45, leader stalled 5 s | 32,926 | 26,125 at 4,000; 9,184 at 8,000; 31,205 at 20 |
| 4,000 | 20 | 5+150 ms, straggler +500, leader stalled 5 s | 19,950 | 15,398 at 2,000; 14,660 at 8,000; 18,806 at 500 |
| 4,000 | 500 | 5+250 ms, straggler +100, leader stalled 5 s | 23,481 | 12,660 at 2,000; 13,235 at 8,000; 16,691 at 20 |
| 8,000 | 500 | 5+150 ms symmetric, no fault | 37,429 | 24,125 at 4,000; 18,431 at 1,000; 33,796 at 20 |

The shape is the familiar one. A short election timeout wins where a real fault must be caught and stall noise is low; a long one wins where noise would cause spurious elections and there is no fault to catch; the middle value wins where both are present. The straggler magnitude tunes the two dimensions separately, because above the crossover it flips the slow-path winner while below it leaves the high value winning, and it adds election noise either way.

**The sixth pair, 8,000 with 20, has no regime where it wins.** Seven scenario shapes were tried: base with a severe +2,000 straggler (every election timeout tied at about 39,400 to 39,900 with one election each), 5+150 ms with +350 and with +500, 5+250 ms with +600 and with +800, and a burst of +5,000 ms for 2 s every 20 s. In all of them 4,000 matched or beat 8,000. The reason is structural rather than a gap in the search: a high election timeout earns its keep by avoiding elections, and elections hurt SBFT mainly because they break the fast path, so once the slow-path timeout is low enough to abandon the fast path there is little left for the high timeout to protect. A learning agent should be expected to discover that this corner is dominated.

Run-to-run spread in the stressed regimes is wide, so single runs mislead. Repeats of 5+150 ms with a +500 straggler gave 30,821 and 28,658 at 4,000 against 29,040 and 30,902 at 8,000 - fully overlapping, and an earlier reading of that pair as a 4,000 win was noise.

## SBFT fast and slow paths

SBFT uses `n=3f+1` (`c=0`) with the primary as collector and individually verifiable RSA votes. Certificates carry the votes explicitly; they are not constant-size threshold signatures.

- Fast path: the collector verifies votes from all `n` replicas and broadcasts the full certificate. Recipients verify its distinct signers and matching view, sequence, and digest before marking the request committed.
- Slow path: with `2f+1` votes, the collector waits `fast_path_wait_ms` for the remaining votes. On expiry it broadcasts the prepared certificate, collects `2f+1` signed CommitAck votes, and broadcasts the final certificate. A decision executes once all preceding slots have executed.
- View change preserves a full decision proof if present. Otherwise it compares the highest slow-path prepared view with possible fast decisions supported by `f+1` individual votes. A unique fast candidate overrides the slow candidate only at a higher view; the slow candidate wins ties. This preserves fast decisions even if their full certificate reached only a replica omitted from the next view-change quorum.

Executions are attributed to their path in `SbftReport` and the `Stats` line. With timely votes from all replicas, requests take the fast path. With up to `f` unavailable backups and a functioning collector, requests can finish on the slow path. Signed messages and explicit certificates add real cryptographic and bandwidth costs to measurements.

The protocol rules follow [PBFT sections 4.2-4.4](https://www.usenix.org/legacy/publications/library/proceedings/osdi99/full_papers/castro/castro_html/node4.html) and the [SBFT dual-mode view-change algorithm](https://arxiv.org/abs/1804.01626). `tests/consensus_test.cpp` uses deterministic message delivery with real replica keys to exercise quorum accounting, reordering, equivocation, invalid evidence, view changes, fast/slow recovery, checkpoints, and replay protection.

## Failure spec (proposal delay)

`--failure-spec <failure_spec.xml> --failure-start-unix-ms <ms>` makes a replica read the `<pbft>` or `<sbft>` section (lowercase protocol name) of the shared spec, the same file `scripts/apply_network_failures.py` applies the `<network>` section of and the learning agents read the `<agent>` section of. The controller in `src/core/FailureSpec.cpp` is a port of the SmartBFT and Rust proposal-delay controllers: `warmUpTime`, `atTime`/`startAt`, `interval` repetition, explicit replica ids, and the `leader` token that pins the leader window (leader plus the next f-1 replicas) once per phase or interval tick. Every replica observes the leader a few times per second so all replicas pin the same window. A delayed leader schedules each batch transmission without blocking message handling. Cadence ticks continue within the bounded pipeline, so sufficiently long injected delays can fill the pipeline and reduce throughput. When accepted requests exceed the election timeout, the request timer triggers a view change.

## Learning agent (wall-clock mode)

Learning-agent communication is protocol-agnostic. The per-protocol report and timeout messages live in the repo-wide `proto/agent.proto`; the wall-clock cycle and the protocol adapters live in `src/core/agent/AgentClient.cpp`.

Each replica connects to a local LearningAgent gRPC service (`--agent-port`) and repeats this wall-clock episode cycle:

1. Feature collection for `--learning-feature-duration` ms: consensus latency, phase latencies, inter-commit gaps, throughput, view changes, new views, and (SBFT) fast/slow path counts are accumulated, then sent to the agent as a report (`window_consensus_count = 0` marks the wall-clock window). The previous episode's reward is attached to this report.
2. Reply wait for `--learning-reply-wait` ms: the replica polls `GetTimeout` for the recommendation and applies it at the deadline (the election timeout for every protocol, plus the slow-path timeout for SBFT).
3. Warm-up for `--learning-warmup-duration` ms: metrics are discarded while the system settles under the new timeouts.
4. Reward collection for `--learning-reward-duration` ms: metrics under the new timeouts become the reward, delivered with the next episode's report.

Phase latencies: PBFT reports PrePrepare to prepare quorum, prepare quorum to commit quorum, and commit quorum to execution; SBFT participants report PrePrepare to certificate arrival, certificate to execute certificate (zero on the fast path), and the rest.

Protocol mapping: PBFT and LinearPBFT report as `PROTOCOL_PBFT`, SBFT as `PROTOCOL_SBFT`, the HotStuff variants as `PROTOCOL_HOTSTUFF`. Zyzzyva has no report message in the proto and cannot run with an agent. Start the matching agent, for example `python3 learning_agent/main.py --node-id 0 --protocol pbft --host 127.0.0.1 --port 15501 --signing false --model-type scheduled --schedule-file learning_agent/config/schedule_pbft.csv`.

A live smoke test is available: with an agent running on port 50190, run `./build/agent_client_smoke PBFT 50190 12`.

## Not ported

`config.linearpbft.yaml`, the HotStuff configs, and Zyzzyva keep the original CppBedrock timer (`startTimer`/`stopTimer` actions over `TimeKeeper`) and view-change events; they do not read the failure spec and are not covered by the harness.
