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

## GCP VM setup

For a disposable GCP VM runner, this repo also includes a separate runner project in [vm/Project.toml](/Users/jacob.quinn/.julia/dev/CloudBenchmarks/vm/Project.toml) plus scripts to bootstrap and execute live benchmark runs.

The VM setup script:
- installs `git`, `curl`, and basic build tools
- installs `juliaup`
- sets the default Julia channel to `1.12`
- clones this repo on the requested branch
- rewrites [vm/Project.toml](/Users/jacob.quinn/.julia/dev/CloudBenchmarks/vm/Project.toml) from the chosen branch settings
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

## Running GCP benchmarks

Put your bucket and credentials in repo-local `.env` or copy [vm/bench.env.example](/Users/jacob.quinn/.julia/dev/CloudBenchmarks/vm/bench.env.example) to `vm/bench.env`, then run:

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

There is also a small Azure blob-storage smoke runner that uses the main project environment and defaults to a temporary container:

```bash
./scripts/run-azure-cloud-smoke.sh
```

Set either `CLOUDBENCH_AZURE_CONNECTION_STRING` or direct Azure account/key env vars in `.env` or `vm/azure.env`. By default the script creates a disposable container, runs a tiny `put/get/prefetchdownloadstream` benchmark pass, and deletes the container afterwards.
