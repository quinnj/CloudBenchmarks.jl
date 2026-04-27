# CloudBenchmarks.jl

`CloudBenchmarks` is set up to benchmark the current Reseau-backed cloud stack directly.

Both the root project and the VM runner project pin:
- `CloudBase.jl#codex/http2-native-tls-bench`
- `CloudStore.jl#codex/http2-native-tls-bench`
- `HTTP.jl#codex/http2-native-tls-bench`
- `Reseau.jl#codex/tls-crypto-phase0`

That means a fresh `Pkg.instantiate()` lands on the intended benchmark stack without manual `Pkg.develop`.

## Local Setup

Instantiate the root project for local development:

```bash
julia --project=. --startup-file=no --history-file=no -e 'using Pkg; Pkg.instantiate()'
```

If you want editable sibling worktrees instead of the pinned branch sources, set:
- `CLOUDBASE_PATH`
- `CLOUDSTORE_PATH`
- `HTTP_PATH`
- `RESEAU_PATH`

Those overrides are resolved relative to `vm/Project.toml` and are applied only in the VM runner env.

## Ubuntu VM Bootstrap

For any Ubuntu-based cloud VM, use the single shared bootstrap script:

```bash
bash scripts/setup-cloudbench-vm.sh
```

It:
- installs `git`, `curl`, and build tools
- installs `juliaup`
- clones this repo on the requested branch
- ensures `vm/bench.env` and `vm/azure.env` exist
- instantiates and precompiles the VM runner project

Optional bootstrap overrides:

```bash
export CLOUDBENCHMARKS_BRANCH=codex/http2-native-tls-bench
export CLOUDBENCHMARKS_DIR="${HOME}/CloudBenchmarks"
export JULIA_CHANNEL=1.12
bash scripts/setup-cloudbench-vm.sh
```

## Env Files

Use:
- `vm/bench.env` for GCP-specific credentials/settings
- `vm/azure.env` for Azure-specific credentials/settings
- repo-local `.env` for shared benchmark knobs you want across providers

The runner loads env files literally in Julia, so quoted SAS tokens and connection strings are safe.

## Run Benchmarks

Use the single env-driven runner for both providers:

```bash
CLOUDBENCH_PROVIDER=gcp ./scripts/run-cloudbench.sh
CLOUDBENCH_PROVIDER=azure ./scripts/run-cloudbench.sh
```

Important knobs:
- `CLOUDBENCH_PROVIDER=gcp|azure`
- `CLOUDBENCH_PROFILE=smoke|full`
- `CLOUDBENCH_TLS=reseau`
- `CLOUDBENCH_NTHREADS=16`
- `CLOUDBENCH_NWORKERS=0`
- `CLOUDBENCH_SEMAPHORE_LIMITS=16,32,64`
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

Results are written under `vm/results/`.

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

## Optional Local Clone Helper

If you want the pinned stack cloned into `~/.julia/dev` for local editing, you can still use:

```bash
./scripts/install-jq-reseau-http.sh
```

But for benchmarking, the branch-pinned `[sources]` setup is now the primary path.
