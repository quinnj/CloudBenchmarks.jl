#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
VM_DIR="${ROOT_DIR}/vm"
ROOT_ENV_FILE="${ROOT_DIR}/.env"
ENV_FILE="${CLOUDBENCH_ENV_FILE:-${VM_DIR}/bench.env}"

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
    echo "julia not found on PATH; run scripts/setup-gcp-vm.sh first" >&2
    exit 1
fi

export CLOUDBENCH_OUTPUT_DIR="${CLOUDBENCH_OUTPUT_DIR:-${VM_DIR}/results}"
mkdir -p "${CLOUDBENCH_OUTPUT_DIR}"

if [[ -z "${CLOUDBENCH_GCP_BUCKET:-}" ]]; then
    echo "set CLOUDBENCH_GCP_BUCKET in ${ENV_FILE} or the environment" >&2
    exit 1
fi

if [[ -z "${CLOUDBENCH_GCP_ACCESS_TOKEN:-}" && -z "${CLOUDBASE_GCP_LIVE_ACCESS_TOKEN:-}" && -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
    echo "set CLOUDBENCH_GCP_ACCESS_TOKEN, CLOUDBASE_GCP_LIVE_ACCESS_TOKEN, or GOOGLE_APPLICATION_CREDENTIALS" >&2
    exit 1
fi

if [[ -n "${CLOUDBENCH_GCP_ACCESS_TOKEN:-}" && -z "${CLOUDBASE_GCP_LIVE_ACCESS_TOKEN:-}" ]]; then
    export CLOUDBASE_GCP_LIVE_ACCESS_TOKEN="${CLOUDBENCH_GCP_ACCESS_TOKEN}"
fi

julia --startup-file=no --project="${VM_DIR}" "${ROOT_DIR}/scripts/run_gcp_cloudbench.jl" "$@"
