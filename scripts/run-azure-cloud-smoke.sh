#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
VM_DIR="${ROOT_DIR}/vm"

export PATH="${HOME}/.juliaup/bin:${PATH}"
export CLOUDBENCH_ENV_FILE="${CLOUDBENCH_ENV_FILE:-${VM_DIR}/azure.env}"

if ! command -v julia >/dev/null 2>&1; then
    echo "julia not found on PATH" >&2
    exit 1
fi

julia --startup-file=no --project="${VM_DIR}" "${ROOT_DIR}/scripts/run_azure_cloud_smoke.jl" "$@"
