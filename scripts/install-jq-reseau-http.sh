#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
JULIA_BIN="${JULIA_BIN:-julia}"

"${JULIA_BIN}" --startup-file=no --project="${ROOT_DIR}" -e '
using Pkg

root = ARGS[1]
Pkg.activate(root)

specs = (
    PackageSpec(url="https://github.com/JuliaServices/Reseau.jl", rev="main"),
    PackageSpec(url="https://github.com/JuliaServices/CloudBase.jl.git", rev="jq-reseau-http"),
    PackageSpec(url="https://github.com/JuliaServices/CloudStore.jl.git", rev="jq-reseau-http"),
)

for spec in specs
    Pkg.develop(spec)
end

Pkg.resolve()
Pkg.instantiate()
Pkg.precompile()
Pkg.status(["CloudBase", "CloudStore", "Reseau"]; mode=Pkg.PKGMODE_MANIFEST)
' "${ROOT_DIR}"
