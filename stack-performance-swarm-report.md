# Stack Performance Swarm Report

Date: 2026-03-13

Scope:
- `CloudStore`
- `CloudBase`
- `Reseau.HTTP`
- `Reseau.TLS/TCP/IOPoll`
- local references in Go stdlib, `AwsHTTP`, and `aws-c-io`

This report is a second-pass investigation starting from the Azure benchmark/profile write-up in `azure-profiling-results.md`. The goal here was not just to find quick wins, but to map out the deeper structural places where we can remove allocation churn, reduce fixed per-request costs, and simplify the stack before 1.0.

## Executive Summary

The profile evidence and code review both point to the same conclusion:

- the biggest remaining wins are in request-path coordination and allocation control, not in rewriting the low-level poller
- `Reseau.HTTP` still pays a lot of eager string/buffer/object costs that could be made lazy or no-copy
- `CloudBase` still constructs and then re-serializes request state in a very string-heavy way
- `CloudStore` still has several whole-body copy paths that are now more expensive than they need to be because lower layers can already handle `AbstractVector{UInt8}` on writes
- DNS resolution should first gain duplicate-suppression, then an explicit opt-in caching wrapper; this is the cleanest path that stays semantically correct
- connection reuse behavior likely needs transport-level acquisition coordination, not just more idle slots

If we want the highest payoff implementation order, the current recommendation is:

1. remove the task + `BufferStream` detour from high-level response consumption
2. add connection-acquisition coordination and optional max-conns-per-host
3. add DNS singleflight, then an explicit caching resolver wrapper
4. rewrite hot header serialization/token checks to avoid O(n^2) scans and transient strings
5. add internal no-copy request/response constructors
6. redesign `_URLParts` around lazy/raw ranges instead of eagerly allocating strings
7. widen upper layers like `CloudStore` to fully exploit `AbstractVector{UInt8}` write support

## Measured Prototypes

These were quick local experiments to validate whether the structural ideas are actually material.

### 1. High-level response consumption

Current path:
- `Reseau/src/7_6_http_client.jl:1744-1808`

What happens today:
- high-level request paths build a `BufferStream`
- spawn a task to pump `AbstractBody` into it
- then read the bytes back out
- this happens even when `decompress=false`

Measured on a synthetic 1 MiB response:
- current `_consume_incoming_response!(..., nothing; decompress=false)`: about `6.87 MB` allocated/call and about `2.10 ms`/call
- direct `body_read!` loop: about `1.06 MB` allocated/call and about `0.16 ms`/call

Implication:
- this path is roughly `13x` slower and about `6.5x` more allocation-heavy than it needs to be

Recommendation:
- add a `BodyIO <: IO` wrapper over `AbstractBody`
- for `decompress=false`, read directly from `BodyIO`
- for gzip, wrap `CodecZlib.GzipDecompressorStream(BodyIO(...))`
- avoid the task + `BufferStream` pump entirely

### 2. Header serialization

Current path:
- `Reseau/src/7_1_http1.jl:422-428`
- `Reseau/src/7_0_http_core.jl:334-366`

What happens today:
- `_write_headers!` calls `header_keys(hdrs)`
- then calls `headers(hdrs, key)` for each key
- that allocates and rescans the full header vector repeatedly

Measured on a representative HTTP/1 request-head serialization microbench:
- current: `50_000` iterations in about `98.0 ms`, `172 MB` allocated total
- direct iteration over stored header entries: `50_000` iterations in about `16.8 ms`, `0 B` allocated total after buffer warmup

Implication:
- about `5.8x` faster
- essentially all hot-path allocation from serialization disappears in the prototype

Recommendation:
- serialize the stored header entries directly
- keep the current canonicalization rules at mutation time
- avoid `header_keys` and `headers` in hot write paths

### 3. URL parsing

Current path:
- `Reseau/src/7_6_http_client.jl:1939-2001`

What happens today:
- `_parse_http_url` eagerly creates owned strings for scheme, rest, authority, target, host, and full URL
- it also folds userinfo into a Basic auth header eagerly

Measured on a representative Azure-style URL:
- current parser: `50_000` parses in about `16.16 ms`, `43.2 MB` allocated total
- simple index/range-only prototype: `50_000` parses in about `4.19 ms`, `1.6 MB` allocated total

Implication:
- about `3.9x` faster
- about `96%` less allocation

Recommendation:
- replace `_URLParts` with a lazy/raw representation
- store the original URL plus byte/index ranges
- derive `target`, `authority`, `server_name`, and `authorization` lazily
- only materialize `full_url` for public response metadata or error paths

### 4. DNS caching prototype

Current path:
- `Reseau/src/5_host_resolvers.jl:282-342`
- `Reseau/src/5_host_resolvers.jl:704-728`

Measured on repeated `localhost` system-resolution hits:
- current `_resolve_system_host("localhost")`: `10_000` iterations in about `1.133 s`, `12.32 MB` allocated total
- cached no-copy hit: about `0.000135 s`, `0 B` allocated total
- cached hit with defensive `copy`: about `0.000259 s`, `0.8 MB` allocated total

Implication:
- a real cache layer can eliminate almost all repeated Julia-side resolver work on hits
- even with defensive copying, the hit path is still dramatically cheaper

Important caveat:
- this is only a hit-path prototype, not a full correctness-preserving TTL resolver
- it does not by itself solve duplicate suppression, stale refresh, or negative-cache semantics

## Highest-Priority Implementation Areas

### A. Response consumption without task staging

Files:
- `Reseau/src/7_6_http_client.jl:1744-1808`
- `Reseau/src/7_6_http_client.jl:2462-2492`
- `Reseau/src/7_6_http_stream.jl:168-170`

Why it matters:
- this is a per-request fixed cost
- it shows up directly in small-object GET/PUT throughput
- it is a good fit for a semantics-preserving improvement

Expected payoff:
- high

Risk:
- low to medium

### B. Connection acquisition coordination

Files:
- `Reseau/src/7_6_http_client.jl:338-366`
- `Reseau/src/7_6_http_client.jl:368-397`

Reference designs:
- `Go/src/net/http/transport.go`
- `AwsHTTP/src/connection_manager.jl`

Problem:
- `Reseau` either pops an idle connection or immediately dials
- there is no pending-acquire queue
- there is no per-host dial coordination
- under bursty concurrency, small requests can redial even when a connection is about to return to idle

Recommendation:
- add a per-host waiter queue
- optionally add `max_conns_per_host`
- allow direct handoff from a completed request to a waiting acquire before going back through the idle pool

Expected payoff:
- high for the small-object benchmark cases

Risk:
- medium

### C. DNS duplicate suppression and explicit caching

Files:
- `Reseau/src/5_host_resolvers.jl:282-342`
- `Reseau/src/5_host_resolvers.jl:704-728`
- `Reseau/src/5_host_resolvers.jl:937-998`
- `Reseau/src/5_host_resolvers.jl:1249-1284`

References:
- Go `singleflight` behavior in `net/lookup.go`
- `aws-c-io` host resolver cache and failure demotion

Problem:
- every hostname lookup goes straight through blocking `getaddrinfo`
- concurrent callers do not share lookups
- deadline support currently adds a `Timer`, `Condition`, and spawned task around an uninterruptible lookup

Recommendation:
- first add in-flight lookup coalescing keyed by `(network, host)`
- then add an explicit resolver wrapper, not hidden mutable state inside `SystemResolver`
- suggested shape:
  - `CachingResolver(parent=SystemResolver(); ttl_ns, stale_ttl_ns, negative_ttl_ns, max_hosts)`
- keep `SystemResolver` as the pure baseline
- cache literals outside the normal path
- add temporary address failure demotion as a separate concern

Expected payoff:
- high on bursty cloud request workloads

Risk:
- medium

### D. Hot header token checks and membership tests

Files:
- `Reseau/src/7_0_http_core.jl:450-503`

Problem:
- `headercontains` and related helpers lowercase and trim by allocating new strings
- this is used in hot internal paths like `Connection`, `Transfer-Encoding`, `Content-Encoding`, and retry logic

Measured prototype:
- current `headercontains`: about `768 B` allocated/call on a representative token-list header
- byte-wise ASCII token scanner prototype: about `15 B` allocated/call

Recommendation:
- replace string-producing trim/lower helpers for internal call sites with byte-wise ASCII scanners

Expected payoff:
- medium-high

Risk:
- low to medium

### E. Internal no-copy request and response constructors

Files:
- `Reseau/src/7_6_http_client.jl:1027-1087`
- `Reseau/src/7_0_http_core.jl:780-811`
- `Reseau/src/7_6_http_client.jl:2471-2492`
- `Reseau/src/7_0_http_core.jl:874-909`

Problem:
- `_copy_request_shallow_body` and `_copy_request` still route through the public constructor
- the public constructor copies `headers` and `trailers`
- `_finalize_request_response` similarly copies parsed response headers again

Recommendation:
- add trusted internal constructors for already-owned header/trailer state
- keep public constructors defensive

Expected payoff:
- medium

Risk:
- low

### F. Lazy `URLParts`

Files:
- `Reseau/src/7_6_http_client.jl:1609-1619`
- `Reseau/src/7_6_http_client.jl:1939-2001`
- related redirect/path rewriting in `Reseau/src/7_6_http_client.jl:1094-1237`

Problem:
- current URL parsing is eager and string-heavy
- redirects and target merging then do more `split`, `SubString`, `String`, and `join`

Reference ideas:
- Go `net/url` keeps raw-vs-escaped path distinctions
- `AwsHTTP` extracts only the minimal URI bits needed in the hot path

Recommendation:
- make the client fast path operate on raw ranges and minimally decoded fields
- preserve raw path and authority separately
- only build the full absolute URL lazily

Expected payoff:
- high

Risk:
- medium to high
- but this is precisely the sort of breaking pre-1.0 redesign that is cheapest now

## CloudBase Findings

### 1. Request shaping is still very string-heavy

Files:
- `CloudBase/src/reseau_http.jl:12-71`
- `CloudBase/src/reseau_http.jl:132-166`
- `CloudBase/src/reseau_http.jl:257-316`

Findings:
- `_query_string` duplicates percent-encoding logic and allocates through `IOBuffer`, `String`, `String[]`, and `join`
- `_headers` stringifies all input eagerly
- `_request_url` reconstructs a full absolute URL string after building `HTTP.URI` and `HTTP.Request`
- `cloudrequest` builds a request in `HTTP.jl` form, signs/mutates it, then converts it back into `Reseau` request inputs

Recommendation:
- if `Reseau` gets a lower-allocation request builder, `CloudBase` should stop bouncing through `HTTP.URI`/`HTTP.Request` for the hot transport path
- unify query encoding with the lower-level implementation instead of reimplementing it in both layers

### 2. Request-body prep still fully materializes `IO`

Files:
- `CloudBase/src/reseau_http.jl:147-166`

Findings:
- `Vector{UInt8}` is already fixed
- but `IO` still gets fully read into memory before transport send
- forms still get fully encoded to a `String`

Recommendation:
- medium-term: align this with a real streaming request-body abstraction in `Reseau`
- short-term: exploit `AbstractVector{UInt8}` more aggressively for in-memory buffer cases

### 3. Azure and AWS signing paths are still expensive

Files:
- `CloudBase/src/azure.jl:164-227`
- `CloudBase/src/aws.jl:310-379`

Findings:
- Azure shared-key signing still uses `filter`, `map`, `sort!`, `join`, and string interpolation on each request
- AWS SigV4 is still even more allocation-heavy: canonical URI/query/header construction is entirely string-built
- SAS parsing got better with cached token pairs, but the shared-key and AWS paths still have plenty of fixed-cost churn

Recommendation:
- build canonical request strings with byte-oriented builders
- precompute or cache stable per-credential pieces where safe
- share a lower-allocation canonical-header representation if we standardize header storage lower in the stack

### 4. `_filter_request_kwargs` allocates every request

Files:
- `CloudBase/src/reseau_http.jl:168-176`

Finding:
- each request builds a `Pair{Symbol, Any}[]` and named tuple for a tiny fixed keyword set

Recommendation:
- inline the supported keyword extraction or use a small immutable helper object

## CloudStore Findings

### 1. `IOBuffer` request-body prep still forces full copies

Files:
- `CloudStore/src/put.jl:6-20`
- `CloudStore/src/put.jl:23-35`

Findings:
- `prepBody(::IOBuffer, ...)` copies `x.data` into a fresh `Vector{UInt8}`
- `prepBodyMultipart(::IOBuffer, ...)` similarly builds a copied `Vector` and then wraps it in `IOBuffer`

Why this matters:
- after widening low-level writes to `AbstractVector{UInt8}`, these copies are less justified than before

Recommendation:
- for immutable/read-only upload paths, return the underlying buffer or a view instead of copying
- if aliasing semantics are a concern, make that a deliberate opt-in/opt-out policy rather than unconditional copying

### 2. Multipart upload stream only accepts `Vector{UInt8}`

Files:
- `CloudStore/src/object.jl:459-552`

Finding:
- `Base.write(io::MultipartUploadStream, bytes::Vector{UInt8})` is still `Vector`-only

Recommendation:
- widen to `AbstractVector{UInt8}`
- if the input is non-contiguous and needs persistence beyond the call, materialize once at the queue boundary
- otherwise keep zero-copy behavior for views/contiguous buffers

### 3. Multipart GET path copies headers per part

Files:
- `CloudStore/src/get.jl:213-238`

Finding:
- each part request does `copy(headers)` and then mutates the `Range` header

Recommendation:
- build a lighter per-task header scratch object
- or split stable headers from the per-part `Range` mutation

### 4. URL parse/dispatch is regex-heavy but probably not the first benchmark target

Files:
- `CloudStore/src/parse.jl:107-191`

Finding:
- parse/dispatch uses several regex matches and many `String(...)` conversions

Recommendation:
- this is a cleanup opportunity, but it is not likely to be the primary Azure benchmark bottleneck compared with request-path churn lower in the stack

## TLS / TCP / IOPoll Findings

### 1. TLS write path still has avoidable outer copies

Files:
- `Reseau/src/7_4_http2_client.jl:124-135`
- `Reseau/src/7_7_http_server.jl:654-655`
- `Reseau/src/6_tls.jl:1125-1175`

Finding:
- outer loops call `write(conn, bytes[(total + 1):end])`
- that copies the buffer slice before it even reaches `SSL_write`
- `TLS.write` already loops internally until completion

Recommendation:
- switch these outer call sites to `@view`
- better: add an internal pointer/offset write helper so upper layers do not need to slice at all

### 2. HTTP/1 fixed-length body path still separates head and body writes

Files:
- `Reseau/src/7_6_http_client.jl:247-263`
- `Reseau/src/7_1_http1.jl:468-482`

Finding:
- the common fixed-length `BytesBody` path writes the request head, then writes the body
- that likely turns into an extra `SSL_write` and often an extra TLS record

Recommendation:
- for in-memory `BytesBody`, consider coalescing head + first body bytes
- or add a scatter-write helper for TCP and a coalescing scratch path for TLS

### 3. `sendmsg`/`iovec` plumbing exists but is not used

Files:
- `Reseau/src/2_socket_ops.jl:103-127`
- `Reseau/src/2_socket_ops_linux.jl:560-577`
- `Reseau/src/2_socket_ops_darwin.jl:497-514`

Finding:
- there is already `iovec`/`MsgHdr` support in the socket-ops layer
- current HTTP write paths do not use it

Recommendation:
- add a TCP scatter-write fast path for header + body coalescing on Darwin/Linux
- keep a separate TLS strategy because OpenSSL still wants contiguous application buffers

### 4. Transport read buffer size is hardcoded

Files:
- `Reseau/src/7_6_http_client.jl:16`
- `Reseau/src/7_6_http_client.jl:35-38`

Finding:
- `_ConnReader` hardcodes `16 KiB`

Recommendation:
- make this tunable on `Transport`
- benchmark `16 KiB`, `32 KiB`, and `64 KiB` for the Azure GET cases

### 5. Poller structure itself is probably not the first target

Files:
- `Reseau/src/3_internal_poll.jl`
- `Reseau/src/4_tcp.jl`
- `Reseau/src/2_socket_ops_linux.jl`
- `Reseau/src/2_socket_ops_darwin.jl`

Finding:
- the overall `prepare -> wait -> retry on EAGAIN` shape is already fairly close to Go
- current evidence does not show a single catastrophic low-level poller bottleneck

Recommendation:
- do not start with a poller rewrite
- prioritize the request-path and resolver work first

## Breaking-Change-Friendly Ideas Worth Considering Pre-1.0

1. Make the HTTP client fast path operate on a lazy/raw URL representation.
2. Add trusted internal request/response constructors that do not defensively copy owned state.
3. Introduce a real streaming request-body abstraction so `CloudBase` and `CloudStore` stop materializing whole `IO`s by default.
4. Add per-host connection waiter queues and optional max-conns-per-host.
5. Make DNS caching explicit and configurable through resolver composition, not hidden global behavior.
6. Widen remaining upper-layer write APIs to `AbstractVector{UInt8}` where buffer lifetime rules allow it.

## Recommended Benchmark Plan

### Short-term microbenches

1. Response-consumption microbench
- measure current `_consume_incoming_response!`
- compare against `BodyIO` direct-read prototype

2. HTTP/1 request-head serialization microbench
- current `_write_request_head!`
- compare against direct header-entry iteration

3. URL parse microbench
- current `_parse_http_url`
- compare against lazy range-based parser

4. DNS resolver hit-path microbench
- current `_resolve_system_host`
- compare against caching resolver hit with and without defensive copy

5. TLS write microbench
- loopback TLS pair
- fixed-size writes at `4 KiB`, `16 KiB`, `64 KiB`
- measure allocation and throughput before and after removing slice copies

### End-to-end cloud reruns after the first implementation batch

1. `1mb get`
2. `1mb put`
3. `64mb put`

Track:
- throughput
- allocations/request
- DNS lookups/request
- TLS handshakes/request
- new dials/request
- idle-reuse hit rate

## Suggested Implementation Order

1. `Reseau.HTTP`: response-consumption rewrite
2. `Reseau.HTTP`: header serialization + token scanner cleanup
3. `Reseau.HTTP`: internal no-copy request/response constructors
4. `Reseau.HostResolvers`: lookup coalescing
5. `Reseau.HostResolvers`: opt-in caching resolver
6. `Reseau.HTTP transport`: connection waiter queue / max-conns-per-host
7. `Reseau.HTTP`: lazy `URLParts`
8. `CloudBase`: reduce request-shaping/signing duplication on top of the new lower-level primitives
9. `CloudStore`: remove remaining whole-body copies and widen upload write surfaces to `AbstractVector{UInt8}`

## Notes

- I attempted a direct `CloudStore` package-level microbench on the current shared workspace, but the local environment currently hits a `Reseau` precompile failure around a missing `Random` dependency in the modified checkout. That did not block the code review, but it did block a clean package-level `CloudStore` measurement run in this pass.
- The DNS cache idea the user remembered is not implemented in `AwsHTTP.jl` itself. The relevant design is in local `aws-c-io`, which is useful as reference material but too heavy to copy directly.
