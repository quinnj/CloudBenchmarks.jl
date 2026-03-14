#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
VM_DIR="${ROOT_DIR}/vm"
PROVIDER="${CLOUDBENCH_PROVIDER:-azure}"

export PATH="${HOME}/.juliaup/bin:${PATH}"

if ! command -v julia >/dev/null 2>&1; then
    echo "julia not found on PATH; run scripts/setup-gcp-vm.sh first" >&2
    exit 1
fi

if [[ -z "${CLOUDBENCH_ENV_FILE:-}" ]]; then
    if [[ "${PROVIDER}" == "gcp" ]]; then
        export CLOUDBENCH_ENV_FILE="${VM_DIR}/bench.env"
    else
        export CLOUDBENCH_ENV_FILE="${VM_DIR}/azure.env"
    fi
fi

exec julia --startup-file=no --threads="${JULIA_NUM_THREADS:-auto}" --project="${VM_DIR}" "${ROOT_DIR}/scripts/profile_cloudbench_case.jl" "$@"
