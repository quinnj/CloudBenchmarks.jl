# CloudBenchmarks.jl

Bootstrap the full Reseau-backed stack on a fresh machine with:

```bash
./scripts/install-jq-reseau-http.sh
```

That installs:
- `CloudBase.jl#jq-reseau-http`
- `CloudStore.jl#jq-reseau-http`
- `Reseau.jl#main`

Then you can run the benchmark project in this repo normally with `julia --project=. ...`.

## VM setup

For disposable cloud runners, this repo uses the separate runner project in [vm/Project.toml](/Users/jacob.quinn/.julia/dev/CloudBenchmarks/vm/Project.toml) for both GCP and Azure.

The VM setup script:
- installs `git`, `curl`, and basic build tools
- installs `juliaup`
- sets the default Julia channel to `1.12`
- clones this repo on the requested branch
- rewrites [vm/Project.toml](/Users/jacob.quinn/.julia/dev/CloudBenchmarks/vm/Project.toml) from the chosen branch settings
- creates `vm/bench.env` and `vm/azure.env` from the examples if they do not exist
- runs `Pkg.instantiate()` and `Pkg.precompile()`

Example:

```bash
bash scripts/setup-gcp-vm.sh
```

If you want non-default branches/urls, export them first:

```bash
export CLOUDBENCHMARKS_BRANCH=jq-reseau-http
export CLOUDBASE_BRANCH=jq-reseau-http
export CLOUDSTORE_BRANCH=jq-reseau-http
export RESEAU_BRANCH=main
bash scripts/setup-gcp-vm.sh
```

All cloud wrappers are thin launchers:
- they always run with `--project=vm`
- they do not `source` env files in the shell
- Julia loads `.env` files literally via [vm/src/CloudBenchVM.jl](/Users/jacob.quinn/.julia/dev/CloudBenchmarks/vm/src/CloudBenchVM.jl), so quoted connection strings and SAS tokens are safe
- if `CLOUDBASE_PATH`, `CLOUDSTORE_PATH`, or `RESEAU_PATH` are set, the runner will build a persistent local override env under `vm/.local-overrides/` so profiling can target sibling worktrees directly without rewriting `vm/Project.toml`

## Running GCP benchmarks

Put your bucket and credentials in repo-local `.env` or [vm/bench.env](/Users/jacob.quinn/.julia/dev/CloudBenchmarks/vm/bench.env), then run:

```bash
./scripts/run-gcp-cloudbench.sh
```

Useful knobs:
- `CLOUDBENCH_PROFILE=smoke` for a tiny validation run
- `CLOUDBENCH_PROFILE=full` for the normal benchmark matrix
- `CLOUDBENCH_TLS=reseau`
- `CLOUDBENCH_NTHREADS=16`
- `CLOUDBENCH_SEMAPHORE_LIMITS=16,32,64`
- `CLOUDBENCH_SIZES=1048576,8388608,67108864`
- `CLOUDBENCH_DRY_RUN=1` to print the resolved config and exit

Results are written under `vm/results/`.

## Azure smoke run

Put your Azure account/container/auth settings in repo-local `.env` or [vm/azure.env](/Users/jacob.quinn/.julia/dev/CloudBenchmarks/vm/azure.env), then run:

```bash
./scripts/run-azure-cloud-smoke.sh
```

Set either `CLOUDBENCH_AZURE_CONNECTION_STRING` or direct Azure account/key/token env vars. If `CLOUDBENCH_AZURE_CONTAINER` is set, the runner uses that existing container directly. Otherwise it creates a disposable container, runs the smoke pass, and deletes it afterwards.

## Profiling one case

Use the generic profiler wrapper for either provider:

```bash
CLOUDBENCH_PROVIDER=azure ./scripts/run-cloudbench-profile.sh
```

Useful profiling knobs:
- `CLOUDBENCH_PROVIDER=gcp|azure`
- `CLOUDBENCH_PROFILE_OPERATION=get|put|prefetchdownloadstream`
- `CLOUDBENCH_PROFILE_SIZE=1048576`
- `CLOUDBENCH_PROFILE_SEMAPHORE_LIMIT=192`
- `CLOUDBENCH_PROFILE_NTIMES=3`
- `CLOUDBENCH_PROFILE_PARTS=4096`
- `JULIA_NUM_THREADS=24`

Profiling artifacts are written under `vm/results/`:
- `*-cpu.txt`
- `*-wall.txt`
- `*-alloc.txt`
- `*-alloc-summary.tsv`
- `*-metrics.txt`
