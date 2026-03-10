#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${TRACE:-}" ]]; then
    set -x
fi

REPO_URL="${CLOUDBENCHMARKS_URL:-https://github.com/quinnj/CloudBenchmarks.jl.git}"
REPO_BRANCH="${CLOUDBENCHMARKS_BRANCH:-jq-reseau-http}"
REPO_DIR="${CLOUDBENCHMARKS_DIR:-${HOME}/CloudBenchmarks}"

CLOUDBASE_URL="${CLOUDBASE_URL:-https://github.com/JuliaServices/CloudBase.jl.git}"
CLOUDBASE_BRANCH="${CLOUDBASE_BRANCH:-jq-reseau-http}"
CLOUDSTORE_URL="${CLOUDSTORE_URL:-https://github.com/JuliaServices/CloudStore.jl.git}"
CLOUDSTORE_BRANCH="${CLOUDSTORE_BRANCH:-jq-reseau-http}"
RESEAU_URL="${RESEAU_URL:-https://github.com/JuliaServices/Reseau.jl.git}"
RESEAU_BRANCH="${RESEAU_BRANCH:-main}"

JULIA_CHANNEL="${JULIA_CHANNEL:-1.12}"

if command -v sudo >/dev/null 2>&1 && [[ "$(id -u)" -ne 0 ]]; then
    SUDO=(sudo)
else
    SUDO=()
fi

install_system_packages() {
    if command -v apt-get >/dev/null 2>&1; then
        "${SUDO[@]}" apt-get update -y
        DEBIAN_FRONTEND=noninteractive "${SUDO[@]}" apt-get install -y git curl ca-certificates build-essential pkg-config
        return 0
    fi
    if command -v dnf >/dev/null 2>&1; then
        "${SUDO[@]}" dnf install -y git curl ca-certificates gcc gcc-c++ make pkgconf-pkg-config
        return 0
    fi
    if command -v yum >/dev/null 2>&1; then
        "${SUDO[@]}" yum install -y git curl ca-certificates gcc gcc-c++ make pkgconfig
        return 0
    fi
    echo "unsupported package manager; install git/curl/build tools manually" >&2
    return 1
}

install_juliaup() {
    if ! command -v juliaup >/dev/null 2>&1; then
        curl -fsSL https://install.julialang.org | sh -s -- --yes
    fi
    export PATH="${HOME}/.juliaup/bin:${PATH}"
    juliaup add "${JULIA_CHANNEL}"
    juliaup default "${JULIA_CHANNEL}"
    return 0
}

sync_repo() {
    local url="$1"
    local branch="$2"
    local path="$3"

    if [[ ! -d "${path}/.git" ]]; then
        git clone --branch "${branch}" "${url}" "${path}"
        return 0
    fi

    git -C "${path}" fetch --all --prune
    git -C "${path}" checkout "${branch}"
    git -C "${path}" pull --ff-only origin "${branch}"
    return 0
}

write_runner_project() {
    local vm_dir="$1"
    mkdir -p "${vm_dir}" "${vm_dir}/results"
    cat > "${vm_dir}/Project.toml" <<EOF
name = "CloudBenchVM"
uuid = "86ef4d2b-47b1-460e-8b4f-783fac126226"
version = "0.1.0"
authors = ["quinnj <quinn.jacobd@gmail.com>"]

[deps]
CloudBase = "85eb1798-d7c4-4918-bb13-c944d38e27ed"
CloudBenchmarks = "128a3188-ac61-41e7-9373-75758cb91c5b"
CloudStore = "3365d9ee-d53b-4a56-812d-5344d5b716d7"
Reseau = "802f3686-a58f-41ce-bb0c-3c43c75bba36"

[sources]
CloudBase = {url = "${CLOUDBASE_URL}", rev = "${CLOUDBASE_BRANCH}"}
CloudBenchmarks = {path = ".."}
CloudStore = {url = "${CLOUDSTORE_URL}", rev = "${CLOUDSTORE_BRANCH}"}
Reseau = {url = "${RESEAU_URL}", rev = "${RESEAU_BRANCH}"}
EOF
    if [[ ! -f "${vm_dir}/bench.env" ]]; then
        cp "${vm_dir}/bench.env.example" "${vm_dir}/bench.env"
    fi
    return 0
}

main() {
    install_system_packages
    install_juliaup
    sync_repo "${REPO_URL}" "${REPO_BRANCH}" "${REPO_DIR}"
    export PATH="${HOME}/.juliaup/bin:${PATH}"
    local vm_dir="${REPO_DIR}/vm"
    write_runner_project "${vm_dir}"
    julia --startup-file=no --project="${vm_dir}" -e '
using Pkg
Pkg.instantiate()
Pkg.precompile()
Pkg.status(; mode=Pkg.PKGMODE_MANIFEST)
'
    cat <<MSG

VM bootstrap complete.

Repo: ${REPO_DIR}
Julia channel: ${JULIA_CHANNEL}
Runner project: ${vm_dir}/Project.toml

Next steps:
1. Edit ${vm_dir}/bench.env
2. Run: ${REPO_DIR}/scripts/run-gcp-cloudbench.sh

MSG
    return 0
}

main "$@"
