# Azure CloudBenchmarks Profiling Write-Up

Date: 2026-03-13

Environment:
- Host repo: `CloudBenchmarks`
- Transport: `Reseau`
- Backend: Azure Blob Storage
- Container: existing container from `vm/azure.env`
- Julia threads used for profiles: `24`

Profiled cases:
- `1mb get`, semaphore `192`
- `1mb put`, semaphore `192`
- `64mb put`, semaphore `128`

Artifacts captured for each case:
- CPU sample profile
- Wall-time sample profile
- Allocation sample profile
- Allocation top-summary TSV
- Metrics file with warmup / CPU-profiled / wall-profiled / alloc-profiled throughput

Raw artifacts:
- `prof-1mb-get-t24-s192-*`
- `prof-1mb-put-t24-s192-*`
- `prof-64mb-put-t24-s128-*`

## Changes required to make the Azure scripts run on this VM

This section captures the code and config changes that were necessary to get the Azure runner and profiling flow working in this environment.

Without these changes, the benchmark scripts did not run successfully end to end on this machine.

### 1. `vm/azure.env` was not safe to `source`

Problem:
- the Azure env file contained a raw SAS token with `&` characters
- `scripts/run-azure-cloud-smoke.sh` originally used shell `source` on the env file
- bash interpreted the SAS token line as shell control syntax instead of a single env assignment
- as a result, the Azure credential variables never loaded correctly

Observed failure:
- the script exited early claiming no Azure auth env vars were set

What changed:
- replaced shell `source` behavior with a literal `.env` parser in `scripts/run-azure-cloud-smoke.sh`
- the new loader:
  - ignores comments and blank lines
  - supports optional `export `
  - parses `KEY=VALUE` literally
  - strips surrounding quotes when present
  - does not execute the env file as shell

Impact:
- `AZURE_STORAGE_SAS_TOKEN=sp=...&st=...` works correctly
- env files are safer and less brittle

### 2. The Azure smoke runner was using the wrong Julia project

Problem:
- `scripts/run-azure-cloud-smoke.sh` originally launched Julia with `--project="${ROOT_DIR}"`
- the repo root `Manifest.toml` was pinned to local dev paths from another machine, including paths like `/Users/jacob.quinn/.julia/dev/...`
- on this VM those paths did not exist
- Julia failed to load `CloudBase`, `CloudStore`, and related dependencies

Observed failure:
- package load / precompile errors from missing local dev package paths

What changed:
- switched the Azure runner to use the VM project by default:
  - `CLOUDBENCH_JULIA_PROJECT` defaults to `vm`
- added an automatic `Pkg.instantiate()` step before running the benchmark

Why this fixed it:
- `vm/Project.toml` already had Git/path-based sources intended for a fresh machine
- the runner now boots the project that is actually appropriate for a disposable VM

### 3. The Azure smoke script incorrectly created explicitly configured containers

Problem:
- `scripts/run_azure_cloud_smoke.jl` always called `create_container!`
- your `vm/azure.env` pointed to an existing container: `jq-test`
- your SAS token was container-scoped
- Azure rejected container creation with `403`

Observed failure:
- `failed to create Azure container jq-test; status=403`

What changed:
- modified the smoke script so container creation only happens when the script generated a disposable container name itself
- if `CLOUDBENCH_AZURE_CONTAINER` is explicitly set, it now uses that existing container directly

Impact:
- existing containers + container-scoped SAS tokens now work as expected

### 4. `CloudBase` was incompatible with the current `Reseau` header API

Problem:
- `CloudBase` on the `jq-reseau-http` branch still used `HT.get_headers`
- current `Reseau` exposes `headers(headers, key)` instead
- the Azure runner hit this when making requests through `Reseau`

Observed failure:
- `UndefVarError: get_headers not defined in Reseau.HTTP`

What changed:
- checked out `CloudBase` locally on this VM
- patched `src/reseau_http.jl`:

Before:

```julia
for value in HT.get_headers(headers, key)
```

After:

```julia
for value in HT.headers(headers, key)
```

Impact:
- `CloudBase` now works against the current `Reseau` checkout on this machine

### 5. The VM project had to be pointed at the local patched `CloudBase`

Problem:
- `vm/Project.toml` originally sourced `CloudBase` from the GitHub `jq-reseau-http` branch
- that remote source did not include the compatibility patch above

What changed:
- rewired the VM project source for `CloudBase` from remote URL to local path:

Before:

```toml
CloudBase = {url = "https://github.com/JuliaServices/CloudBase.jl.git", rev = "jq-reseau-http"}
```

After:

```toml
CloudBase = {path = "../../CloudBase"}
```

Impact:
- the benchmark runner and profiling scripts use the patched local `CloudBase` checkout

### 6. Added a dedicated Azure profiling runner

Problem:
- `CloudBenchmarks` had built-in support to turn on Julia CPU profiling, but nothing that:
  - loaded Azure credentials/container setup
  - ran one targeted Azure case
  - wrote CPU, wall-time, and allocation reports to files
  - repeated runs enough to collect stable samples

What changed:
- added `scripts/profile_azure_case.jl`
- added `scripts/profile-azure-cloudbench.sh`

What this new flow does:
- loads the same Azure env config as the smoke runner
- launches Julia with explicit thread count
- profiles one exact case at a time
- writes:
  - `*-cpu.txt`
  - `*-wall.txt`
  - `*-alloc.txt`
  - `*-alloc-summary.tsv`
  - `*-metrics.txt`

Why this mattered:
- without this, there was no clean reproducible way on this VM to capture the profiling data used in this write-up

### 7. Added a safer Azure env example

Problem:
- the existing Azure env example did not make it obvious that connection strings and SAS tokens should be quoted in shell-sensitive contexts

What changed:
- restored / updated `vm/azure.env.example`
- added quoted examples for:
  - `CLOUDBENCH_AZURE_CONNECTION_STRING`
  - `AZURE_STORAGE_SAS_TOKEN`

Impact:
- reduces the chance of future env-file parsing mistakes

## Files changed during this session

### In `CloudBenchmarks`

- `scripts/run-azure-cloud-smoke.sh`
  - replaced `source`-based env loading with literal `.env` parsing
  - switched default Julia project to `vm`
  - added automatic `Pkg.instantiate()`

- `scripts/run_azure_cloud_smoke.jl`
  - only create/delete containers when the container name was auto-generated

- `vm/Project.toml`
  - pointed `CloudBase` source to local path `../../CloudBase`

- `vm/azure.env.example`
  - updated examples for quoted connection string / SAS token usage

- `scripts/profile_azure_case.jl`
  - new file for targeted Azure profiling

- `scripts/profile-azure-cloudbench.sh`
  - new shell wrapper for the profiling entrypoint

### In `CloudBase`

- `src/reseau_http.jl`
  - replaced `HT.get_headers` with `HT.headers`

## Minimal patch summary

### `CloudBase/src/reseau_http.jl`

```diff
-        for value in HT.get_headers(headers, key)
+        for value in HT.headers(headers, key)
```

### `CloudBenchmarks/scripts/run_azure_cloud_smoke.jl`

```diff
-    create_container!(container, credentials)
     results_file = ""
     try
+        generated && create_container!(container, credentials)
         results_file = CloudBenchmarks.runbenchmarks(
```

### `CloudBenchmarks/vm/Project.toml`

```diff
-CloudBase = {url = "https://github.com/JuliaServices/CloudBase.jl.git", rev = "jq-reseau-http"}
+CloudBase = {path = "../../CloudBase"}
```

## Operational note

If this VM is torn down and you want to recreate the same working state locally, the minimum important facts are:

1. the Azure runner must not `source` raw SAS-token env files
2. the Azure runner should use the `vm` Julia project, not the root project manifest from this repo checkout
3. `CloudBase` needs the `HT.get_headers` -> `HT.headers` compatibility fix for the current `Reseau`
4. the smoke script must not try to create an explicitly configured existing container
5. the profiling scripts added here were custom and are not part of the original repo baseline

## Short answer

The main bottlenecks do not point to a single “core event loop is broken” problem.

The current picture is:

1. Small-object throughput (`1mb`) is heavily penalized by per-request overhead:
   - connection lifecycle work
   - DNS / connect path activity
   - TLS handshake / readiness work
   - header / URL / query / signature string construction
   - high allocation churn in strings, buffers, header collections, and request/response scaffolding

2. Large-object put throughput (`64mb`) is less request-overhead-bound and much more likely limited by memory movement and body preparation.

3. There is at least one clear code-level hotspot worth fixing immediately:
   - `CloudBase/src/reseau_http.jl` currently copies `Vector{UInt8}` bodies before transport send
   - for large puts this is likely avoidable and expensive

4. The profiles suggest that for small objects the system spends substantial time waiting on transport and connection work rather than purely executing CPU-heavy code.

## Important caveat

The raw profile text files are snapshots, not a synthesized diagnosis.

They contain the sampled stacks and top allocation summaries, but they do not themselves explain:
- which samples are expected network wait vs suspicious wait
- which allocations are structurally important vs accidental churn
- which code paths are most actionable
- what to prioritize first

This write-up is the synthesized interpretation of those artifacts.

## Throughput observed during profiling

These numbers are from the metrics files and are useful mainly as context, since profiling perturbs runtime:

### `1mb get`, `24` threads, `sem=192`

From `prof-1mb-get-t24-s192-metrics.txt`:
- warmup: `16.54 Gbps`
- best during CPU-profiled repeats: `25.98 Gbps`
- best during wall-profiled repeats: `28.07 Gbps`
- best during allocation-profiled repeats: `9.49 Gbps`

Interpretation:
- allocation profiling hurts this path dramatically
- that is strong evidence that the path is allocation-heavy enough for allocation instrumentation to materially perturb it
- the case is sensitive to per-request overhead

### `1mb put`, `24` threads, `sem=192`

From `prof-1mb-put-t24-s192-metrics.txt`:
- warmup: `15.60 Gbps`
- CPU-profiled: `27.94 Gbps`
- wall-profiled: `26.94 Gbps`
- allocation-profiled: `14.70 Gbps`

Interpretation:
- same broad pattern as `1mb get`
- allocations matter a lot
- request construction / signing / buffer handling are likely expensive

### `64mb put`, `24` threads, `sem=128`

From `prof-64mb-put-t24-s128-metrics.txt`:
- warmup: `17.97 Gbps`
- CPU-profiled: `25.57 Gbps`
- wall-profiled: `26.02 Gbps`
- allocation-profiled: `24.98 Gbps`

Interpretation:
- allocation profiling barely hurts this case compared with `1mb`
- that strongly suggests large puts are not dominated by the same small-object request churn
- this case is much more likely bounded by bulk transport and body movement

## Findings by case

## Case 1: `1mb get`

### Wall-time profile: strongest signal

Top wall-time samples show substantial time in:
- `Reseau.HTTP._new_conn!`
- `Reseau.HTTP._acquire_conn!`
- `Reseau.TLS._wait_ssl_ready!`
- `Reseau.IOPoll.wait_read!`
- `Reseau.EventLoops.pollwait!`
- `getaddrinfo` / resolver path in `Reseau.HostResolvers`

This is the most important result from the `1mb get` profile.

Interpretation:
- the system is spending meaningful wall time either opening new connections or waiting on connection establishment / readiness
- DNS resolution is still visible in the profile
- this does not look like “steady-state reused connections are fully saturating the pipe”
- it looks more like connection acquisition and connection lifecycle are materially involved in the hot path

This is exactly the kind of problem that will hurt `1mb` more than `64mb`:
- per-request connection/setup cost is large relative to payload size
- bandwidth falls far below NIC ceiling because the workload becomes request-rate-limited

### CPU profile

The CPU profile includes noticeable samples in:
- `CloudBase.Azure.azuresign!`
- `CloudBase.reseau_http.cloudrequest`
- `_ensure_content_length!`
- header normalization / request write path inside `Reseau.HTTP`
- TLS write/read path in `Reseau`
- string construction and integer-to-string conversion

Interpretation:
- even when not blocked on wall time, the `1mb get` path still burns CPU on request preparation and protocol scaffolding
- Azure signing and request shaping are not free
- there is visible string formatting churn in request construction

### Allocation profile

Top sampled allocations:
- `String`
- `Profile.Allocs.BufferType`
- `Memory{Pair{String, String}}`
- `Memory{String}`
- `Memory{Char}`
- `Memory{SubString{String}}`
- `Memory{Reseau.HTTP.ClientConn}`
- `IOBuffer`
- `Vector{UInt8}`
- `Reseau.HTTP._IncomingResponseHead`
- `Task`

Interpretation:
- the get path allocates heavily in metadata and protocol scaffolding, not only in payload buffers
- strings and string-adjacent containers dominate sampled allocation totals
- connection objects show up in allocations, which supports the wall-time signal that connection lifecycle is active in the hot path
- response-head parsing / header materialization are probably doing more work than ideal

Most likely allocation contributors in this path:
- header copying / normalization
- URL and query mutation for SAS signing
- request/response object construction
- connection-reader and response-head scaffolding
- string conversions around `HTTP.URI`, headers, and Azure signing

### Bottom line for `1mb get`

Most plausible bottleneck stack:

1. connection reuse is not effective enough under this workload, or new-connection work is still too common
2. DNS/connect/TLS readiness is still materially present in the steady-state path
3. protocol and metadata allocations add enough churn to hurt small-object throughput

## Case 2: `1mb put`

### Wall-time profile

The wall-time profile is less neatly concentrated than `1mb get`, but it still shows:
- event loop wait (`epoll_wait`)
- `Reseau.HTTP._new_conn!`
- transport setup in the request path

Interpretation:
- this path also likely pays meaningful connection/setup cost
- the profile is not screaming “pure CPU bottleneck”
- it still looks like per-request transport overhead is meaningful

### CPU profile

Top CPU samples include:
- `CloudBase.reseau_http.cloudrequest`
- request kwarg filtering
- response/request object path through `HTTP.Messages`
- TLS / crypto reads
- `malloc`
- GC helpers and scheduler bookkeeping

Interpretation:
- `1mb put` has visible CPU overhead in request setup and transport orchestration
- allocator / GC noise is non-trivial
- this path is doing enough object churn that allocator pressure is part of the cost

### Allocation profile

Top sampled allocations:
- `Profile.Allocs.BufferType`
- `String`
- `Vector{UInt8}`
- `Memory{UInt8}`
- `Memory{Pair{String, String}}`
- `IOBuffer`
- `Memory{Reseau.HTTP.ClientConn}`
- `Task`
- `Reseau.HTTP._IncomingResponseHead`

Interpretation:
- buffer allocations are even more prominent here than in `1mb get`
- request/response scaffolding is still significant
- connection objects again show up

The put path likely pays for:
- request header and target construction
- Azure signing mutation
- request-body wrapping/copying
- response scaffolding
- allocator churn around many small request objects

### Bottom line for `1mb put`

This path appears to be a combination of:
- per-request transport overhead
- allocation / GC pressure
- request-body and header materialization cost

For `1mb`, these fixed costs are large enough to prevent approaching the NIC ceiling.

## Case 3: `64mb put`

### Wall-time profile

Important wall-time entries:
- `CloudBase.reseau_http._prepare_transport_body!`
- `Vector{UInt8}(x::SubArray...)`
- `copyto!` / array copying
- resolver path still appears (`getaddrinfo`), but less dominant relative to the total payload cost

Interpretation:
- unlike `1mb`, the large-put path makes payload preparation itself visible
- copying the body before transport send is now expensive enough to show clearly

### CPU profile

The CPU profile includes:
- `CloudStore.putObjectImpl`
- `CloudStore.putObject`
- `CloudBase.reseau_http.cloudrequest`
- `CloudBase.Azure.azuresign!`
- `URIs.queryparampairs`
- string and generic memory conversion
- host resolver connect path
- GC work

Interpretation:
- bulk puts still spend time in request setup and signing
- but the more actionable signal is that body preparation and memory motion are clearly present

### Allocation profile

Top allocations:
- `Profile.Allocs.BufferType`
- `String`
- `Vector{UInt8}`
- `Memory{UInt8}`
- much lower relative metadata churn than the `1mb` cases

Interpretation:
- large puts still allocate, but the path is much less dominated by tiny metadata objects
- the fact that allocation profiling barely reduces throughput supports this
- the main issue here is probably not “too many tiny allocations”
- it is more likely “too much copying / body prep / avoidable memory movement”

### Bottom line for `64mb put`

The largest concrete code smell found during profiling is here:

In `CloudBase/src/reseau_http.jl`:

```julia
if body isa AbstractVector{UInt8}
    bytes = body isa Vector{UInt8} ? copy(body) : Vector{UInt8}(body)
    return bytes, Int64(length(bytes))
end
```

That means:
- if the caller already has a `Vector{UInt8}`, the transport still duplicates it
- for `64mb` puts, that is an avoidable extra full-body copy
- for many concurrent requests, this is substantial memory bandwidth and GC pressure

This is the cleanest “offline fix” candidate from the current profiling session.

## Cross-cutting findings

## 1. Small requests are dominated by fixed costs

This is the broadest conclusion.

The `1mb` cases are paying for:
- signing
- header preparation
- URL/query mutation
- request/response object setup
- connection acquisition / handshake / DNS
- metadata allocation churn

All of these are mostly fixed per request.

As object size grows, those costs amortize better, which is why larger sizes get much closer to the machine’s line rate.

## 2. Connection lifecycle looks too active for the small-object sweet spot

The wall profile for `1mb get` makes connection work visible enough that it deserves direct investigation.

Possible explanations:
- idle connection reuse is insufficient under the benchmark pattern
- connections are being discarded too aggressively
- some requests are missing reuse opportunities
- connection limits are causing churn
- DNS resolution is happening more often than expected

This does not prove a bug, but it does prove this area is worth offline review.

## 3. Azure signing and request mutation are visible in the hot path

`azuresign!` appears in CPU samples.

With SAS credentials specifically, `azuresign!` mutates the request URL by:
- parsing current query params
- parsing token query params
- merging them
- rebuilding `request.url`
- rebuilding `request.target`

Relevant code:
- `CloudBase/src/azure.jl:178-186`

This likely contributes to:
- `String`
- `SubString`
- query pair
- header
allocation churn.

For small objects, this is a meaningful tax.

## 4. Allocation pressure is real in `1mb` paths

This is not speculative.

Evidence:
- allocation profiling severely perturbs `1mb get` and `1mb put`
- top allocation summaries are full of strings, buffers, header vectors, and protocol scaffolding

That means the code is not just waiting on the network. It is also doing a lot of heap work per operation.

## 5. The event loop itself is not the clearest primary culprit

There is visible time in:
- `poll`
- `epoll_wait`
- `wait_read!`
- `pollwait!`

But that alone is not evidence of a poller bottleneck.

In these profiles, those waits are more consistent with:
- waiting for network readiness
- handshake readiness
- connection establishment

I would not prioritize “rewrite the poller” based on this data.

I would prioritize:
- connection reuse
- DNS/connect behavior
- request allocation churn
- redundant body copies

## Specific code hotspots worth offline review

## A. Redundant body copy in transport prep

File:
- `/home/azureuser/CloudBase/src/reseau_http.jl`

Relevant lines:
- around `146-150`

Current behavior:
- copies `Vector{UInt8}` bodies before issuing the transport request

Why it matters:
- for `64mb` puts, this is very likely expensive and avoidable
- memory copies at this size can consume memory bandwidth and inflate GC pressure

Recommendation:
- if transport semantics allow it, do not copy when `body isa Vector{UInt8}`
- keep copy only for aliasing-unsafe cases
- benchmark `64mb put` before/after

Priority: very high

## B. Request header materialization / conversion

File:
- `/home/azureuser/CloudBase/src/reseau_http.jl`

Relevant lines:
- `_to_http_headers` around `121-128`
- request construction around `283-316`
- `_filter_request_kwargs` around `167-175`

Potential issues:
- repeated `String(key)` / `String(value)` conversion
- request context / header shaping per request
- extra intermediate vectors and pairs

Recommendation:
- audit all header conversion paths for avoidable copies
- avoid rebuilding vectors of pairs unless required by the downstream API
- cache or reuse stable request scaffolding if possible

Priority: medium-high

## C. Azure SAS signing path allocations

File:
- `/home/azureuser/CloudBase/src/azure.jl`

Relevant lines:
- `178-186`

Potential issues:
- query parsing of request URL
- query parsing of SAS token
- query merging
- URL and target reconstruction

Why it matters:
- shows up in the hot path
- likely creates many small strings and pair objects
- particularly painful for `1mb` workloads

Recommendation:
- avoid reparsing static SAS token parameters on every request
- pre-parse and cache SAS token pairs if credentials are reused
- consider a lower-allocation URL mutation path

Priority: high for small-object performance

## D. Connection reuse / DNS behavior

Files:
- `Reseau HTTP client` internals
- `CloudBase.CloudPool`

Relevant code in local repo:
- `CloudBase/src/reseau_http.jl:177-189`

Observed signal:
- `1mb get` wall profile shows `_new_conn!`, `_acquire_conn!`, `_wait_ssl_ready!`, resolver activity, and `getaddrinfo`

Questions to investigate offline:
- are idle connections being reused as often as expected?
- is `max_idle_per_host` sufficient?
- are connections getting closed before reuse?
- is DNS being consulted too often?
- is the transport keyed in a way that prevents efficient reuse?

Recommendation:
- instrument connection reuse counters in `Reseau.HTTP`
- count:
  - new connections
  - reused idle connections
  - DNS resolutions
  - TLS handshakes
- compare `1mb get` and `64mb put`

Priority: very high for small-object performance

## E. Per-request object churn in benchmark harness

File:
- `/home/azureuser/CloudBenchmarks/src/CloudBenchmarks.jl`

Relevant lines:
- `137-151` in `do_op_n`

Potential issues:
- creates many tasks
- creates a fresh `CloudPool` per `do_op_n`
- allocates per-operation structures eagerly

Important nuance:
- this may be acceptable for benchmarking transport throughput
- but it also means harness overhead is part of the measurement

Recommendation:
- test a variant that reuses a `CloudPool` across repeated benchmark runs within the same process
- test a worker-loop approach instead of spawning one Julia task per operation burst

Priority: medium

## F. String-heavy path in request shaping

Evidence:
- large sampled `String` totals in both `1mb` profiles
- visible CPU in integer/string formatting and substring/string conversions

Likely contributors:
- Azure signing
- URL and target construction
- header canonicalization
- response header parsing / materialization

Recommendation:
- use allocation tracking locally around:
  - `azuresign!`
  - `_request_url`
  - `_headers`
  - `_to_http_headers`
  - response header decode path
- rank them by bytes allocated per request

Priority: high

## Recommended next steps, in order

## 1. Remove the `Vector{UInt8}` copy in `_prepare_transport_body!`

Why first:
- clear code-level issue
- large-put path already points directly at it
- easy to test

Expected payoff:
- best chance of improving `64mb put`
- lower memory pressure

## 2. Instrument connection reuse and DNS / handshake counts

Why second:
- `1mb get` wall profile strongly suggests connection lifecycle is a major limiter
- current sample profiles hint at the issue but do not quantify it

What to measure:
- total requests
- new connections
- reused connections
- DNS lookups
- TLS handshakes
- idle-conn hit rate

Expected payoff:
- converts the current suspicion into a concrete diagnosis

## 3. Optimize SAS signing for reused credentials

Why:
- likely repeated query parsing / rebuilding on every request
- shows up on hot path

Likely fix shape:
- pre-parse SAS token once
- merge query parameters with fewer temporary objects
- avoid rebuilding strings more than necessary

Expected payoff:
- especially useful for `1mb get/put`

## 4. Audit header and URL conversion churn in `reseau_http.jl`

Why:
- strong allocation evidence
- affects all operations

Targets:
- `_to_http_headers`
- `_request_url`
- request header building
- content-length/header normalization

Expected payoff:
- broad small-object improvement

## 5. Investigate whether benchmark harness itself is over-amplifying overhead

Why:
- per-burst `CloudPool` creation
- many spawned tasks

Goal:
- separate “real transport cost” from “harness cost”

Expected payoff:
- cleaner measurement
- maybe some real throughput improvement

## 6. Only after the above, revisit poller / event-loop internals

Why last:
- current data does not clearly indict the poller as the main bottleneck
- easier wins appear above it

## Offline experiments worth running next

1. `64mb put` before/after removing `Vector{UInt8}` copy.

2. Add counters for:
   - connection reuse
   - DNS calls
   - TLS handshakes
   during `1mb get`.

3. Microbenchmark:
   - `azuresign!`
   - `_request_url`
   - `_to_http_headers`
   for allocations and runtime.

4. Repeat `1mb get` with:
   - same semaphore
   - same thread count
   - but forced persistent transport reuse metrics enabled
   to confirm whether reuse is the limiting factor.

5. Compare SAS-token auth path against shared-key auth path, if available.
   - If shared key materially reduces allocation churn, SAS URL mutation may be a bigger culprit than expected.

## What I would fix first if the goal is fastest progress

If the goal is the highest ROI sequence:

1. remove request-body copy for `Vector{UInt8}`
2. measure and improve connection reuse / DNS / handshake behavior
3. reduce SAS signing allocations
4. reduce header / URL allocation churn

That sequence fits the current evidence best.

## Conclusion

The profiles do not suggest a single catastrophic low-level bottleneck.

They suggest two distinct regimes:

- `1mb get/put`: request-rate-limited by connection lifecycle overhead and allocation-heavy request/response scaffolding
- `64mb put`: largely amortized request overhead, now more sensitive to payload handling and avoidable memory copies

The best immediate engineering target is the body-copy issue in `CloudBase`.

The best next diagnostic target is connection reuse behavior in `Reseau` for the `1mb get` case.
