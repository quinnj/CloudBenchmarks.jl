# CloudBenchmarks.jl

`CloudBenchmarks` benchmarks the Reseau-backed cloud stack (Reseau, HTTP, CloudBase,
CloudStore) against Azure Blob Storage, Google Cloud Storage, and S3.

The root project and the VM runner project (`vm/`) use registered releases: CloudBase 2.1,
CloudStore 1.12, HTTP 2.13.1, and Reseau 1.6.3 or later. Every runner prints the version,
tree hash, and source of each of the four packages before it starts.

## Local Setup

Instantiate the root project for local development:

```bash
julia --project=. --startup-file=no --history-file=no -e 'using Pkg; Pkg.instantiate()'
```

## Choosing The Stack

The VM runners (`run-cloudbench.sh`, `run-azure-put-get-matrix.sh`, and the profiling
scripts) take the stack from the environment:

- `CLOUDBENCH_STACK=registered` (default): the registered releases resolved in `vm/`.
- `CLOUDBENCH_STACK=main`: the default branch of each package (`master` for HTTP, `main` for
  the others), updated to the latest commit at every start.
- `HTTP_PATH`, `RESEAU_PATH`, `CLOUDBASE_PATH`, `CLOUDSTORE_PATH`: a local checkout for that
  package, with the rest from the selected stack. Relative paths are resolved from
  `vm/Project.toml`.

Overrides run in `vm/.local-overrides/`, a copy of the VM project with `[sources]` entries for
them. `CLOUDBENCH_STACK_LABEL` (default: the `CLOUDBENCH_STACK` value) labels the results.

## Ubuntu VM Bootstrap

For any Ubuntu-based cloud VM, use the single shared bootstrap script:

```bash
bash scripts/setup-cloudbench-vm.sh
```

It:
- installs `git`, `curl`, build tools, and VM/network inspection tools
- installs `juliaup`
- clones this repo on the requested branch
- ensures `vm/bench.env` and `vm/azure.env` exist
- instantiates and precompiles the VM runner project

Optional bootstrap overrides:

```bash
export CLOUDBENCHMARKS_BRANCH=main
export CLOUDBENCHMARKS_DIR="${HOME}/CloudBenchmarks"
export JULIA_CHANNEL=1.13.0-rc1
bash scripts/setup-cloudbench-vm.sh
```

## Env Files

Use:
- `vm/bench.env` for GCP-specific credentials/settings
- `vm/azure.env` for Azure-specific credentials/settings
- `vm/s3.env` for S3 settings
- repo-local `.env` for shared benchmark knobs you want across providers

The runner loads env files literally in Julia, so quoted SAS tokens and connection strings are safe.

## Run Benchmarks

Use the single env-driven runner for every provider:

```bash
CLOUDBENCH_PROVIDER=gcp ./scripts/run-cloudbench.sh
CLOUDBENCH_PROVIDER=azure ./scripts/run-cloudbench.sh
CLOUDBENCH_PROVIDER=s3 ./scripts/run-cloudbench.sh
```

Registered CloudStore releases support S3 and Azure Blob Storage but not Google Cloud
Storage, so `CLOUDBENCH_PROVIDER=gcp` stops with an error unless a `CLOUDSTORE_PATH` checkout
adds it.

S3 needs an existing bucket in `CLOUDBENCH_S3_BUCKET`, with `CLOUDBENCH_S3_REGION` (default
`us-east-1`) and, for S3-compatible servers such as MinIO, `CLOUDBENCH_S3_HOST`
(`http://127.0.0.1:9000`). Credentials come from the AWS default chain
(`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, profiles, instance metadata).

Each timed batch uses a fresh HTTP/1.1 client that opens at most `semaphore_limit`
connections to the host and keeps them all for reuse, as CloudBase 1.6's `CloudPool` did.
`CLOUDBENCH_CLIENT=default` sends every request through HTTP's shared default client
instead, which is what `CloudStore.get`/`put` use when no `client` is passed; that client
has no connection limit, so at most `semaphore_limit` operations run at a time.

Important knobs:
- `CLOUDBENCH_PROVIDER=gcp|azure|s3`
- `CLOUDBENCH_PROFILE=smoke|full`
- `CLOUDBENCH_TLS=reseau`
- `CLOUDBENCH_NTHREADS=16`
- `CLOUDBENCH_SEMAPHORE_LIMITS=16,32,64`
- `CLOUDBENCH_CLIENT=batch|default`
- `CLOUDBENCH_OPERATIONS=put,get,prefetchdownloadstream`
- `CLOUDBENCH_SIZES=262144,1048576,8388608,67108864`
- `CLOUDBENCH_SMOKE_SIZE=1048576`
- `CLOUDBENCH_SMOKE_PARTS=4`
- `CLOUDBENCH_NTIMES=3`
- `CLOUDBENCH_OUTPUT_DIR=vm/results`
- `CLOUDBENCH_DRY_RUN=1`

Provider defaults:
- if `CLOUDBENCH_ENV_FILE` is unset, GCP uses `vm/bench.env`
- if `CLOUDBENCH_ENV_FILE` is unset, Azure uses `vm/azure.env`

Example beefy Azure full run:

```bash
export CLOUDBENCH_PROVIDER=azure
export CLOUDBENCH_PROFILE=full
export JULIA_NUM_THREADS=24
export CLOUDBENCH_NTHREADS=24
export CLOUDBENCH_SEMAPHORE_LIMITS=32,64,128,192
export CLOUDBENCH_SIZES=1048576,8388608,67108864,268435456
./scripts/run-cloudbench.sh
```

Results are written under `vm/results/` as a TSV with a header row, one row per
configuration. `rate` is the best Gbps of the `CLOUDBENCH_NTIMES` samples. The other columns
are medians over the samples: `gbps_median`, `wall_s_median`, and client CPU as
`cpu_s_median` (process CPU seconds, all threads, summed over worker processes),
`cpu_cores_median` (CPU seconds per wall second), `cpu_s_per_gb_median`, and
`gc_s_median`. A put's wall time includes generating its payload once per batch; its CPU
time does not.

## Azure Preflight

Before trusting performance numbers from a fresh Azure VM, capture the VM/network facts:

```bash
./scripts/azure-vm-preflight.sh
```

This writes `vm/results/azure-vm-preflight-*.txt` with the Azure instance metadata, kernel,
NIC driver, VF counters, queue info, qdisc, TCP buffers, congestion control, IRQ service,
CPU, memory, and disk basics.

For a high-throughput experiment, you can apply the repo's Azure TCP/qdisc tuning helper:

```bash
./scripts/apply-azure-network-tuning.sh
sudo reboot
```

After reboot, run preflight again and compare the files before timing Julia.

## Azure Put/Get Matrix

Use the dedicated matrix runner for the smaller/larger put/get grid we were running by hand:

```bash
export CLOUDBENCH_PROVIDER=azure
export JULIA_NUM_THREADS=64
export CLOUDBENCH_STACK_LABEL=http2-reseau
export CLOUDBENCH_MATRIX_SEMAPHORES=96
export CLOUDBENCH_MATRIX_SIZES=67108864
export CLOUDBENCH_MATRIX_PARTS=256
export CLOUDBENCH_MATRIX_REPEATS=3
export CLOUDBENCH_MATRIX_WARMUP_PARTS=8
export CLOUDBENCH_MATRIX_ALLOW_MULTIPART=false
export CLOUDBENCH_MATRIX_OPERATIONS=put,get
./scripts/run-azure-put-get-matrix.sh
```

This runner uses one Julia process and multiple Julia threads. It does not use
worker processes. HTTP selects the protocol automatically. The runner does not
force HTTP/1.1 or HTTP/2. `CLOUDBENCH_PROVIDER=s3` runs the same matrix against an
existing S3 bucket (see Run Benchmarks).

The matrix runner prints the exact package versions and sources. It writes a
headered TSV under `vm/results/`. Each row includes the negotiated protocol,
thread count, semaphore, multipart mode, operation, object size, bytes, seconds,
Gbps, request latency percentiles, and the client's CPU over the batch: `cpu_s`
(process CPU seconds, all threads), `cpu_cores`, `cpu_s_per_gb`, and `gc_s`. Set
`CLOUDBENCH_MATRIX_CLEANUP_OBJECTS=false` only when you need to inspect the
generated objects after a run.

For an HTTP 1.x comparison, instantiate `legacy/Project.toml`. It pins
CloudBase 1.5.1, CloudStore 1.7.0, and HTTP 1.11.0. Run the same matrix script
with that project and the same Julia thread count, object sizes, and repeat
count. Its manifest was resolved with Julia 1.13; on Julia 1.12, run `Pkg.resolve()` in
it first. The stack variables above apply only to `vm/`.

## Profile One Case

Use the generic profiling entrypoint:

```bash
CLOUDBENCH_PROVIDER=azure ./scripts/run-cloudbench-profile.sh
```

Useful profiling knobs:
- `CLOUDBENCH_PROVIDER=gcp|azure`
- `CLOUDBENCH_PROFILE_OPERATION=get|put|prefetchdownloadstream`
- `CLOUDBENCH_PROFILE_SIZE=1048576`
- `CLOUDBENCH_PROFILE_PARTS=4096`
- `CLOUDBENCH_PROFILE_SEMAPHORE_LIMIT=192`
- `CLOUDBENCH_PROFILE_NTIMES=3`
- `JULIA_NUM_THREADS=24`

Profiling artifacts are written under `vm/results/`:
- `*-cpu.txt`
- `*-wall.txt`
- `*-alloc.txt`
- `*-alloc-summary.tsv`
- `*-metrics.txt`

The Azure matrix also has focused CPU and allocation profilers. Both use one
Julia process and the same automatic protocol selection as the matrix:

```bash
julia -t64 --project=vm scripts/profile_azure_matrix.jl
julia -t64 --project=vm scripts/profile_azure_allocs.jl
```

Configure them with `CLOUDBENCH_PROFILE_OPERATION`,
`CLOUDBENCH_PROFILE_SIZE`, `CLOUDBENCH_PROFILE_PARTS`, and
`CLOUDBENCH_PROFILE_SEMAPHORE`. CPU profiles use `Profile`. Allocation
profiles use `Profile.Allocs` and accept
`CLOUDBENCH_PROFILE_ALLOC_SAMPLE_RATE`.
The CPU profiler also exports a portable PProf `*-cpu.pb.gz` file. Set
`CLOUDBENCH_PROFILE_PPROF_OUTPUT` to select a different path.

## Optional Local Clone Helper

To clone the four packages' default branches into `~/.julia/dev` for local editing and point
the VM runner at them:

```bash
./scripts/install-jq-reseau-http.sh
```
