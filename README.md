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
