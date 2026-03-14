#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

export CLOUDBENCH_PROVIDER="${CLOUDBENCH_PROVIDER:-gcp}"

exec "${ROOT_DIR}/scripts/run-cloudbench.sh" "$@"
