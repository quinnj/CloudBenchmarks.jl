#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
VM_DIR="${ROOT_DIR}/vm"

export PATH="${HOME}/.juliaup/bin:${PATH}"

if ! command -v julia >/dev/null 2>&1; then
    echo "julia not found on PATH; run scripts/setup-cloudbench-vm.sh first" >&2
    exit 1
fi

exec julia --startup-file=no --threads="${JULIA_NUM_THREADS:-auto}" --project="${VM_DIR}" "${ROOT_DIR}/scripts/run_cloudbench.jl" "$@"
