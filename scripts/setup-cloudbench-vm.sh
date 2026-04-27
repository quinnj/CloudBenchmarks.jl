#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${TRACE:-}" ]]; then
    set -x
fi

REPO_URL="${CLOUDBENCHMARKS_URL:-https://github.com/quinnj/CloudBenchmarks.jl.git}"
REPO_BRANCH="${CLOUDBENCHMARKS_BRANCH:-codex/http2-native-tls-bench}"
JULIA_CHANNEL="${JULIA_CHANNEL:-1.12}"

# Use repo containing this script when run from scripts/ within a clone
if [[ -n "${BASH_SOURCE[0]:-}" ]]; then
    SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
    if [[ -d "${SCRIPT_DIR}/../.git" ]]; then
        REPO_DIR="${CLOUDBENCHMARKS_DIR:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
    else
        REPO_DIR="${CLOUDBENCHMARKS_DIR:-${HOME}/CloudBenchmarks}"
    fi
else
    REPO_DIR="${CLOUDBENCHMARKS_DIR:-${HOME}/CloudBenchmarks}"
fi

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
    export PATH="${HOME}/.juliaup/bin:${PATH}"
    if ! command -v juliaup >/dev/null 2>&1; then
        curl -fsSL https://install.julialang.org | sh -s -- --yes
    fi
    juliaup add "${JULIA_CHANNEL}"
    juliaup default "${JULIA_CHANNEL}"
    return 0
}

install_node_and_codex() {
    export NVM_DIR="${HOME}/.nvm"
    if [[ ! -s "${NVM_DIR}/nvm.sh" ]]; then
        curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
    fi
    # nvm.sh references unbound variables; disable set -u for this block
    set +u
    # shellcheck source=/dev/null
    [[ -s "${NVM_DIR}/nvm.sh" ]] && . "${NVM_DIR}/nvm.sh"
    nvm install --lts
    nvm use --lts
    npm install -g @openai/codex@latest
    set -u
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

initialize_runner_files() {
    local repo_dir="$1"
    local vm_dir="${repo_dir}/vm"
    mkdir -p "${vm_dir}/results"
    chmod +x "${repo_dir}/scripts/"*.sh
    if [[ ! -f "${vm_dir}/bench.env" ]]; then
        cp "${vm_dir}/bench.env.example" "${vm_dir}/bench.env"
    fi
    if [[ ! -f "${vm_dir}/azure.env" ]]; then
        cp "${vm_dir}/azure.env.example" "${vm_dir}/azure.env"
    fi
    return 0
}

instantiate_runner_env() {
    local repo_dir="$1"
    local vm_dir="${repo_dir}/vm"
    julia --startup-file=no --project="${vm_dir}" -e '
using Pkg
general_toml = joinpath(first(DEPOT_PATH), "registries", "General.toml")
general_git = joinpath(first(DEPOT_PATH), "registries", "General")
if !isfile(general_toml) && !isdir(general_git)
    Pkg.Registry.add("General")
else
    Pkg.Registry.update()
end
Pkg.resolve()
Pkg.instantiate()
Pkg.precompile()
Pkg.status(; mode=Pkg.PKGMODE_MANIFEST)
'
    return 0
}

main() {
    install_system_packages
    install_juliaup
    install_node_and_codex
    sync_repo "${REPO_URL}" "${REPO_BRANCH}" "${REPO_DIR}"
    export PATH="${HOME}/.juliaup/bin:${PATH}"
    initialize_runner_files "${REPO_DIR}"
    instantiate_runner_env "${REPO_DIR}"
    cat <<MSG

VM bootstrap complete.

Repo: ${REPO_DIR}
Julia channel: ${JULIA_CHANNEL}
Node.js: $(command -v node >/dev/null && node -v || echo 'installed')
Codex CLI: installed
Runner project: ${REPO_DIR}/vm/Project.toml

Next steps:
1. Edit ${REPO_DIR}/vm/bench.env or ${REPO_DIR}/vm/azure.env
2. Run benchmarks: ${REPO_DIR}/scripts/run-cloudbench.sh
3. Profile one case: ${REPO_DIR}/scripts/run-cloudbench-profile.sh

MSG
    return 0
}

main "$@"
