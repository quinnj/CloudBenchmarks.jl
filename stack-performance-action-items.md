# Action Items: Stack Performance And Efficiency Work

## Context
- Repo: Multi-repo performance stack (`CloudBenchmarks`, `CloudStore`, `CloudBase`, `Reseau`)
- Worktree: `/Users/jacob.quinn/.julia/dev/CloudBenchmarks`
- Branch: `jq-reseau-http`
- Additional worktrees:
  - `CloudStore`: `/Users/jacob.quinn/.julia/dev/CloudStore` on `jq-reseau-http`
  - `CloudBase`: `/Users/jacob.quinn/.julia/dev/CloudBase` on `jq-reseau-http`
  - `Reseau`: `/Users/jacob.quinn/.julia/dev/Reseau` on `main`
- Source reports:
  - `/Users/jacob.quinn/.julia/dev/CloudBenchmarks/azure-profiling-results.md`
  - `/Users/jacob.quinn/.julia/dev/CloudBenchmarks/stack-performance-swarm-report.md`
  - `/Users/jacob.quinn/.julia/dev/Reseau/performance-swarm-report-2026-03-13.md`

## Items

### [x] ITEM-001 (P0) Finalize CloudBenchmarks VM Runner Simplification
- Description: The current `CloudBenchmarks` worktree already contains substantial uncommitted simplifications to VM setup, environment loading, Azure smoke runs, and profiling entrypoints. We need to verify that work end-to-end, capture assumptions, and commit it as a clean baseline before stacking further performance changes on top.
- Desired outcome: `CloudBenchmarks` has a single clear VM/project/environment flow, Azure and GCP runners use the `vm` project consistently, env parsing is Julia-side instead of shell `source`, and the profiling scripts are validated and committed.
- Worktree: `/Users/jacob.quinn/.julia/dev/CloudBenchmarks`
- Affected files: `README.md`, `scripts/run-azure-cloud-smoke.sh`, `scripts/run-gcp-cloudbench.sh`, `scripts/run_azure_cloud_smoke.jl`, `scripts/run_gcp_cloudbench.jl`, `scripts/setup-gcp-vm.sh`, `scripts/profile_cloudbench_case.jl`, `scripts/run-cloudbench-profile.sh`, `vm/Project.toml`, `vm/azure.env.example`, `vm/bench.env.example`, `vm/src/CloudBenchVM.jl`, `.gitignore`
- Implementation notes:
  - Re-read each touched script/module and confirm the design matches the documented Azure profile findings.
  - Preserve the user’s `azure-profiling-results.md` and other existing local files.
  - Verify shell syntax, dry-run flows, and documentation accuracy.
  - Update the action-item file with assumptions before any follow-up edits.
  - Commit only the `CloudBenchmarks` baseline cleanup/profiling work for this item.
- Verification:
  - `bash -n scripts/run-azure-cloud-smoke.sh`
  - `bash -n scripts/run-gcp-cloudbench.sh`
  - `bash -n scripts/setup-gcp-vm.sh`
  - `bash -n scripts/run-cloudbench-profile.sh`
  - `CLOUDBENCH_DRY_RUN=1 CLOUDBENCH_SKIP_INSTANTIATE=1 bash scripts/run-gcp-cloudbench.sh`
  - `CLOUDBENCH_DRY_RUN=1 CLOUDBENCH_SKIP_INSTANTIATE=1 bash scripts/run-azure-cloud-smoke.sh`
  - `CLOUDBENCH_DRY_RUN=1 CLOUDBENCH_SKIP_INSTANTIATE=1 bash scripts/run-cloudbench-profile.sh put 1mb`
- Assumptions:
  - The current uncommitted `CloudBenchmarks` edits are part of the intended baseline and should be finalized rather than discarded.
  - Dry-run validation is sufficient for this item; no live cloud resources are required to mark it complete.
  - The `.gitignore` and `vm/bench.env.example` edits are intentional parts of the VM-runner simplification because they support persistent local override envs and safer fresh-VM setup.
- Risks:
  - Shell and Julia runner behavior can diverge subtly if dry-run coverage misses a live-only path.
- Completion criteria:
  - The updated runner/profile flow is documented, dry-runs succeed, and the work is committed in the `CloudBenchmarks` repo.
- Verification evidence:
  - `bash -n` passed for `scripts/run-azure-cloud-smoke.sh`, `scripts/run-gcp-cloudbench.sh`, `scripts/setup-gcp-vm.sh`, and `scripts/run-cloudbench-profile.sh`.
  - GCP dry run succeeded with explicit env overrides and printed the resolved benchmark matrix.
  - Azure smoke dry run succeeded with explicit env overrides and printed the resolved smoke config.
  - Generic profile dry run succeeded for `CLOUDBENCH_PROVIDER=azure` and printed the resolved profiling case settings.

### [ ] ITEM-002 (P0) Finalize CloudBase Transport And Signing Baseline Fixes
- Description: `CloudBase` already has uncommitted changes for the `Reseau` header API transition, request-body copy removal for `Vector{UInt8}`, and cached Azure SAS query-pair parsing. We need to verify the current patch set, preserve the measured before/after evidence, and commit it cleanly.
- Desired outcome: `CloudBase` has a validated baseline of low-risk transport/signing improvements committed before we start more structural changes.
- Worktree: `/Users/jacob.quinn/.julia/dev/CloudBase`
- Affected files: `src/reseau_http.jl`, `src/azure.jl`, `test/runtests.jl`
- Implementation notes:
  - Re-read the current modifications and ensure tests cover the intended behavior.
  - Reproduce or re-check the targeted microbenchmarks for body prep and SAS signing as feasible from the current local stack.
  - Update assumptions in the tracker before any edits.
  - Commit only the `CloudBase` baseline transport/signing fixes for this item.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no -e 'using Pkg; Pkg.test(; coverage=false)'`
  - `julia --project=. --startup-file=no --history-file=no -e 'using CloudBase, HTTP; req = HTTP.Request("PUT", "https://example.blob.core.windows.net/c/b", ["Content-Length" => "4"], UInt8[0x61,0x62,0x63,0x64]); body, n = CloudBase._prepare_transport_body!(req); @assert body === req.body; @assert n == 4'`
  - `julia --project=. --startup-file=no --history-file=no -e 'using CloudBase, HTTP; creds = CloudBase.AzureCredentials("sp=racwdl&st=2024-01-01T00:00:00Z&se=2026-01-01T00:00:00Z&spr=https&sv=2022-11-02&sr=c&sig=abc"); req = HTTP.Request("GET", "https://acct.blob.core.windows.net/c"); CloudBase.azuresign!(req; credentials=creds)'`
- Assumptions:
  - The repo-local `CloudBase` environment still resolves against the local `Reseau` checkout after the current dev-path changes.
  - Re-running the earlier microbenchmarks is helpful but not strictly required if the functional verification succeeds and the code matches the previously recorded results.
- Risks:
  - The local `Reseau` checkout may introduce transient compatibility or precompile issues during verification.
- Completion criteria:
  - Tests pass, the baseline perf fixes are preserved, and the work is committed in the `CloudBase` repo.

### [ ] ITEM-003 (P0) Finalize Reseau Low-Level Write And HTTP/1 Streaming Fixes
- Description: `Reseau` already contains uncommitted low-level improvements to accept `AbstractVector{UInt8}` in TCP/TLS/IOPoll writes, avoid redundant request-template body cloning, and stream fixed-length `BytesBody` requests more directly. We need to verify and commit this baseline before taking on the next structural HTTP items.
- Desired outcome: The current write-path and HTTP/1 send-path improvements are validated and committed as a clean baseline.
- Worktree: `/Users/jacob.quinn/.julia/dev/Reseau`
- Affected files: `src/3_internal_poll.jl`, `src/4_tcp.jl`, `src/6_tls.jl`, `src/7_1_http1.jl`, `src/7_6_http_client.jl`, `test/http1_wire_tests.jl`, `test/tcp_tests.jl`, `test/tls_tests.jl`
- Implementation notes:
  - Re-read the current modifications carefully because they touch low-level semantics.
  - Run focused tests first, then the full `Reseau` suite if the environment allows.
  - Preserve unrelated untracked files (`repro/`, Windows notes) untouched.
  - Update assumptions in the tracker before any edits.
  - Commit only the verified low-level write/streaming baseline for this item.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no test/tcp_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/tls_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http1_wire_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no -e 'using Pkg; Pkg.test(; coverage=false)'`
- Assumptions:
  - The uncommitted `Reseau` changes are the intended continuation of the prior request-body/write-path work and should be finalized first.
  - If the full suite fails on unrelated existing issues, we will record that precisely and keep iterating until this item’s touched paths are demonstrably sound.
- Risks:
  - Low-level changes can affect timeout and close semantics in non-obvious ways.
  - The full suite may surface unrelated instability from the rewrite branch.
- Completion criteria:
  - Focused tests pass, the full suite is run or any residual blocker is fully understood and resolved, and the work is committed in the `Reseau` repo.

### [ ] ITEM-004 (P0) Remove Task And BufferStream Response Consumption Tax
- Description: High-level `Reseau.HTTP` response finalization currently pumps every response through a `BufferStream` plus a spawned task even when `decompress=false`. This is one of the clearest measured hot-path costs from the swarm report.
- Desired outcome: Non-decompressing response consumption reads directly from the underlying `AbstractBody`; gzip still works through a direct `IO` adapter without the task + `BufferStream` detour.
- Worktree: `/Users/jacob.quinn/.julia/dev/Reseau`
- Affected files: `src/7_6_http_client.jl`, `src/7_6_http_stream.jl`, `test/http_client_tests.jl`, `test/http_integration_tests.jl`, `test/http_client_transport_tests.jl`
- Implementation notes:
  - Introduce a small `BodyIO <: IO` adapter over `AbstractBody`.
  - Route plain response reads through `BodyIO` directly.
  - Route decompression through `CodecZlib.GzipDecompressorStream(BodyIO(...))`.
  - Preserve connection-release semantics and managed-body lifecycle.
  - Add focused tests for plain and gzip paths, plus response-stream sinks.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no test/http_client_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http_client_transport_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no -e 'using Reseau; const HT = Reseau.HTTP; include("test/http_client_tests.jl")'`
  - `julia --project=. --startup-file=no --history-file=no -e 'using Reseau; const HT = Reseau.HTTP; # run a small local microbench for _consume_incoming_response! before/after and assert functionality'`
- Assumptions:
  - This change can stay internal to `Reseau.HTTP` without any public API break.
- Risks:
  - Connection-release semantics can regress if body EOF/close handling changes subtly.
- Completion criteria:
  - The task/`BufferStream` detour is removed for the normal path, tests pass, microbench evidence is recorded, and the work is committed.

### [ ] ITEM-005 (P0) Rewrite Hot Header Serialization And Token Checks
- Description: Header serialization is currently O(n^2) on stored header entries and hot token checks (`Connection`, `Transfer-Encoding`, etc.) allocate aggressively.
- Desired outcome: Request/response header writing iterates stored entries directly, and hot token membership checks avoid transient lowercase/trim strings.
- Worktree: `/Users/jacob.quinn/.julia/dev/Reseau`
- Affected files: `src/7_0_http_core.jl`, `src/7_1_http1.jl`, `test/http1_wire_tests.jl`, `test/http_core_tests.jl`
- Implementation notes:
  - Replace `header_keys`/`headers` loops in serialization hot paths with direct entry iteration.
  - Add internal ASCII token scanners for `headercontains`-style hot uses.
  - Preserve public semantics and duplicate header ordering.
  - Re-run the header microbench from the swarm report.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no test/http1_wire_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http_core_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no -e 'using Reseau; const HT = Reseau.HTTP; # run header serialization/token microbench and print alloc/time summary'`
- Assumptions:
  - The public `Headers` API should remain source-compatible even if hot internals change.
- Risks:
  - Header list/token semantics are easy to get subtly wrong for edge cases.
- Completion criteria:
  - Serialization no longer rescans headers quadratically, hot token checks are allocation-light, tests pass, and the work is committed.

### [ ] ITEM-006 (P0) Add Internal No-Copy Request/Response Construction
- Description: Redirect/retry/request-finalization paths still route through public constructors that defensively copy owned header/trailer state.
- Desired outcome: Internal trusted constructors or builders let `Reseau.HTTP` reuse already-owned request/response metadata without redundant copies, while public constructors stay defensive.
- Worktree: `/Users/jacob.quinn/.julia/dev/Reseau`
- Affected files: `src/7_0_http_core.jl`, `src/7_6_http_client.jl`, `test/http_client_tests.jl`, `test/http_retry_tests.jl`
- Implementation notes:
  - Add explicit internal constructors for already-owned `Headers`, `trailers`, and request/response metadata.
  - Update retry/redirect/finalization paths to use them.
  - Preserve external API behavior.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no test/http_client_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http_retry_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http_integration_tests.jl`
- Assumptions:
  - Internal no-copy constructors are acceptable pre-1.0 as long as they stay non-public.
- Risks:
  - Accidentally exposing shared mutable header state outside intended internal ownership boundaries.
- Completion criteria:
  - Retry/redirect/finalization paths stop making redundant metadata copies, tests pass, and the work is committed.

### [ ] ITEM-007 (P0) Coordinate Connection Acquisition And Reuse
- Description: `Reseau.HTTP.Transport` currently either reuses an idle connection or immediately dials. It lacks per-host waiter queues and direct handoff behavior under bursty concurrency.
- Desired outcome: The transport can coordinate pending acquires per host, reduce unnecessary redials, and optionally enforce a max-connections-per-host policy.
- Worktree: `/Users/jacob.quinn/.julia/dev/Reseau`
- Affected files: `src/7_6_http_client.jl`, `test/http_client_transport_tests.jl`, `test/http_client_tests.jl`
- Implementation notes:
  - Add a per-host pending-acquire queue under the transport lock.
  - Add optional `max_conns_per_host` if the design warrants it.
  - Prefer direct handoff to waiting callers before returning to the idle pool.
  - Add trace/instrumentation hooks or test visibility for reuse vs fresh dial behavior.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no test/http_client_transport_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http_client_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no -e 'using Reseau; const HT = Reseau.HTTP; # run a local concurrency microbench or targeted reuse scenario and print reuse/dial counts'`
- Assumptions:
  - This can remain within the current HTTP/1 transport design without a full transport rewrite.
- Risks:
  - New coordination logic can deadlock or starve if queue semantics are wrong.
- Completion criteria:
  - Reuse behavior improves in concurrent tests, verification passes, and the work is committed.

### [ ] ITEM-008 (P0) Add Resolver Singleflight And Lookup Instrumentation
- Description: `Reseau.HostResolvers` currently performs duplicate in-flight lookups for the same host and does not expose enough cheap visibility into lookup/connect behavior.
- Desired outcome: Concurrent identical lookups are coalesced, and we have enough instrumentation to measure lookup/dial activity during benchmarks.
- Worktree: `/Users/jacob.quinn/.julia/dev/Reseau`
- Affected files: `src/5_host_resolvers.jl`, `test/host_resolvers_tests.jl`, `src/7_6_http_client.jl`
- Implementation notes:
  - Add singleflight-style coalescing keyed by normalized `(network, host)` for non-literal hosts.
  - Add lightweight counters or trace callbacks for lookup activity so benchmark harnesses can observe lookup frequency.
  - Preserve current timeout and error semantics.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no test/host_resolvers_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http_client_transport_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no -e 'using Reseau; const HR = Reseau.HostResolvers; # exercise duplicate concurrent lookup suppression and assert single resolver invocation'`
- Assumptions:
  - Duplicate suppression should land before a cache because it is the lower-risk semantic win.
- Risks:
  - Shared lookup state must not leak stale exceptions or wakeups across callers.
- Completion criteria:
  - Concurrent duplicate lookups are coalesced, tests pass, and the work is committed.

### [ ] ITEM-009 (P1) Add Explicit CachingResolver With Stale Refresh
- Description: The stack needs a correct, explicit DNS cache layer rather than hidden mutable behavior inside the default system resolver.
- Desired outcome: `Reseau` has an opt-in caching resolver wrapper with fresh-hit, stale-while-refresh, and bounded cache behavior suitable for transport reuse scenarios.
- Worktree: `/Users/jacob.quinn/.julia/dev/Reseau`
- Affected files: `src/5_host_resolvers.jl`, `test/host_resolvers_tests.jl`, possibly `src/7_6_http_client.jl`
- Implementation notes:
  - Add `CachingResolver(parent=SystemResolver(); ttl_ns, stale_ttl_ns, negative_ttl_ns, max_hosts)` or similar.
  - Keep `SystemResolver` unchanged as the pure baseline.
  - Exclude literal IPs from the normal cache path.
  - Start conservatively on negative caching.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no test/host_resolvers_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no -e 'using Reseau; const HR = Reseau.HostResolvers; # run warmed-cache hit microbench and functional cache tests'`
- Assumptions:
  - TTLs here are policy TTLs, not authoritative DNS record TTLs, because libc `getaddrinfo` does not expose record TTLs.
- Risks:
  - Search-domain and trailing-dot normalization can make cache keying subtly wrong.
- Completion criteria:
  - The caching resolver has passing semantics tests, warmed-hit behavior is benchmarked, and the work is committed.

### [ ] ITEM-010 (P1) Redesign URLParts To Be Lazy And Raw-Range Based
- Description: `_URLParts` is a high-allocation hot structure today and is a good pre-1.0 breaking-change candidate.
- Desired outcome: HTTP client URL parsing keeps raw source strings plus ranges/offsets and only materializes derived strings lazily when needed.
- Worktree: `/Users/jacob.quinn/.julia/dev/Reseau`
- Affected files: `src/7_6_http_client.jl`, `test/http_client_tests.jl`, `test/http_client_proxy_tests.jl`, `test/http_retry_tests.jl`, `test/http_websocket_client_tests.jl`
- Implementation notes:
  - Replace eager `_URLParts` string fields with raw-source-backed ranges plus lazy derivation helpers.
  - Preserve correct authority, IPv6, userinfo, redirect, and proxy behavior.
  - Re-run the URL parsing microbench from the report.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no test/http_client_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http_client_proxy_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http_retry_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no test/http_websocket_client_tests.jl`
  - `julia --project=. --startup-file=no --history-file=no -e 'using Reseau; const HT = Reseau.HTTP; # run current URL parse microbench shape and print before/after summary'`
- Assumptions:
  - Pre-1.0 breaking internal representation changes are acceptable if external request APIs remain sensible.
- Risks:
  - Redirect, proxy, and websocket edge cases may rely on today’s eagerly materialized fields.
- Completion criteria:
  - The lazy URL representation is in place, correctness tests pass, the microbench improves materially, and the work is committed.

### [ ] ITEM-011 (P1) Reduce CloudBase HTTP.jl Translation Tax
- Description: `CloudBase` still constructs full `HTTP.jl` requests/responses and then translates them into `Reseau` shapes on every request. After the lower-level work lands, we should remove as much of that hot-path translation as possible.
- Desired outcome: `CloudBase` request signing/auth/request shaping works against a leaner transport-facing representation and avoids repeated request/response conversion churn.
- Worktree: `/Users/jacob.quinn/.julia/dev/CloudBase`
- Affected files: `src/reseau_http.jl`, `src/azure.jl`, `src/aws.jl`, `test/runtests.jl`
- Implementation notes:
  - Reinvestigate the shim layer after the relevant `Reseau.HTTP` internals are in place.
  - Collapse duplicated query-rendering and request-shaping logic where practical.
  - Avoid unnecessary `HTTP.Request`/`HTTP.Response` round-tripping on the hot path.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no -e 'using Pkg; Pkg.test(; coverage=false)'`
  - `julia --project=. --startup-file=no --history-file=no -e 'using CloudBase; # run focused request-shaping microbench/probe and print summary'`
- Assumptions:
  - Some public compatibility glue may still remain at the outer API boundary even if the hot path becomes transport-native.
- Risks:
  - Request signing code is correctness-sensitive and easy to regress when the underlying request representation changes.
- Completion criteria:
  - The transport hot path does materially less translation work, tests pass, and the work is committed.

### [ ] ITEM-012 (P1) Reduce CloudStore Multipart Churn And Widen Write Surfaces
- Description: `CloudStore` still copies too much in multipart GET/PUT paths and still has write surfaces that are narrower than the lower stack now needs.
- Desired outcome: Multipart paths avoid obvious header/closure/body copy churn, file-backed uploads exploit views or mmap where safe, and upload stream writes accept `AbstractVector{UInt8}`.
- Worktree: `/Users/jacob.quinn/.julia/dev/CloudStore`
- Affected files: `src/get.jl`, `src/put.jl`, `src/object.jl`, `src/parse.jl`, `test/runtests.jl`
- Implementation notes:
  - Remove per-part header copying and ordered-write closure churn where possible.
  - Widen `MultipartUploadStream.write` to `AbstractVector{UInt8}` with correct lifetime handling.
  - Revisit `IOBuffer`/file multipart preparation to avoid unconditional copies.
  - If the local stack inconsistency is still present, fix the env first or verify from a controlled temp env.
- Verification:
  - `julia --project=. --startup-file=no --history-file=no -e 'using Pkg; Pkg.test(; coverage=false)'`
  - `julia --project=. --startup-file=no --history-file=no -e 'using CloudStore; # run focused multipart/body-prep probes and print summary'`
- Assumptions:
  - The current local dev-stack inconsistency can be resolved or worked around before this item is finalized.
- Risks:
  - Multipart correctness and buffer lifetime handling are easy to get wrong under concurrency.
- Completion criteria:
  - Multipart churn is materially reduced, write surfaces are widened appropriately, tests pass, and the work is committed.

## Continuity

```text
* Take investigation/review findings and make a detailed, prioritized action item .md file; ensure each action item has enough detail (description, affected files, etc.) that a fresh context/engineer "taking on" the item would understand what needs to be done and where to go to get started and ideally how to verify that it's done
* Start working on the action-item list, for each item:
  * Thoroughly investigate the action item and work involved, state assumptions, do the work, including verification step
  * Work until verification succeeds (i.e. tests pass)
  * Mark the item done in the action item list
  * Commit the work involved for this action item
  * Continue with the same steps on the next action item
* When compacting, the itemizer instructions should be preserved *exactly* to ensure continuity
* The action-item document should very clearly state the repo/worktree where the work should be done
* Post-compaction, if there are unstaged edits in files relating to the current action item, you should assume they were your own edits and should continue directly w/ work without pausing to confirm
* No shortcuts or cutting corners while doing the action item work; each item should be done thoughtfully, carefully, with production-quality effort/work put into it; we're not trying to rush the work here at all and prefer quality, robustness, and thoroughness over "quick wins".
* No backwards compat or unnecessary shims should be included unless specifically requested
```
