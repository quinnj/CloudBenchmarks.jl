#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
JULIA_BIN="${JULIA_BIN:-julia}"
DEV_DIR="${JULIA_PKG_DEVDIR:-${HOME}/.julia/dev}"

mkdir -p "${DEV_DIR}"

sync_repo() {
    local name="$1"
    local url="$2"
    local rev="$3"
    local path="${DEV_DIR}/${name}"

    if [[ ! -d "${path}/.git" ]]; then
        git clone "${url}" "${path}"
    fi

    git -C "${path}" fetch --all --prune
    git -C "${path}" checkout "${rev}"
    git -C "${path}" pull --ff-only origin "${rev}"
}

sync_repo "Reseau" "https://github.com/JuliaServices/Reseau.jl.git" "codex/tls-crypto-phase0"
sync_repo "HTTP" "https://github.com/JuliaWeb/HTTP.jl.git" "codex/http2-native-tls-bench"
sync_repo "CloudBase" "https://github.com/JuliaServices/CloudBase.jl.git" "codex/http2-native-tls-bench"
sync_repo "CloudStore" "https://github.com/JuliaServices/CloudStore.jl.git" "codex/http2-native-tls-bench"

"${JULIA_BIN}" --startup-file=no --project="${ROOT_DIR}" -e '
using Pkg

root, devdir = ARGS
Pkg.activate(root)
Pkg.resolve()
Pkg.instantiate()
Pkg.precompile()
Pkg.status(["CloudBase", "CloudStore", "HTTP", "Reseau"]; mode=Pkg.PKGMODE_MANIFEST)
' "${ROOT_DIR}" "${DEV_DIR}"

CLOUDBASE_PATH="${DEV_DIR}/CloudBase" \
CLOUDSTORE_PATH="${DEV_DIR}/CloudStore" \
HTTP_PATH="${DEV_DIR}/HTTP" \
RESEAU_PATH="${DEV_DIR}/Reseau" \
"${JULIA_BIN}" --startup-file=no --project="${ROOT_DIR}/vm" -e '
using Pkg
using CloudBenchVM

vm_dir = ARGS[1]
CloudBenchVM.instantiate_project!(vm_dir)
Pkg.precompile()
Pkg.status(["CloudBase", "CloudStore", "HTTP", "Reseau"]; mode=Pkg.PKGMODE_MANIFEST)
' "${ROOT_DIR}/vm"
