#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
VM_DIR="${ROOT_DIR}/vm"
ROOT_ENV_FILE="${ROOT_DIR}/.env"
ENV_FILE="${CLOUDBENCH_AZURE_ENV_FILE:-${VM_DIR}/azure.env}"

if [[ -f "${ROOT_ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ROOT_ENV_FILE}"
    set +a
fi

if [[ -f "${ENV_FILE}" && "${ENV_FILE}" != "${ROOT_ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
fi

export PATH="${HOME}/.juliaup/bin:${PATH}"

if ! command -v julia >/dev/null 2>&1; then
    echo "julia not found on PATH" >&2
    exit 1
fi

if [[ -z "${CLOUDBENCH_AZURE_CONNECTION_STRING:-}" && -z "${AZURE_STORAGE_CONNECTION_STRING:-}" && -z "${CLOUDBENCH_AZURE_KEY:-}" && -z "${AZURE_STORAGE_KEY:-}" && -z "${CLOUDBENCH_AZURE_ACCESS_TOKEN:-}" && -z "${AZURE_STORAGE_ACCESS_TOKEN:-}" && -z "${AZURE_STORAGE_SAS_TOKEN:-}" && -z "${AZURE_SAS_TOKEN:-}" && -z "${SAS_TOKEN:-}" ]]; then
    echo "set CLOUDBENCH_AZURE_CONNECTION_STRING, AZURE_STORAGE_CONNECTION_STRING, or direct Azure auth env vars" >&2
    exit 1
fi

if [[ -z "${CLOUDBENCH_OUTPUT_DIR:-}" ]]; then
    export CLOUDBENCH_OUTPUT_DIR="${VM_DIR}/results"
fi
mkdir -p "${CLOUDBENCH_OUTPUT_DIR}"

julia --startup-file=no --project="${ROOT_DIR}" "${ROOT_DIR}/scripts/run_azure_cloud_smoke.jl" "$@"
