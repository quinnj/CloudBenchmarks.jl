#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${CLOUDBENCH_OUTPUT_DIR:-${ROOT_DIR}/vm/results}"
mkdir -p "${OUT_DIR}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${OUT_DIR}/azure-vm-preflight-${STAMP}.txt"

run() {
    {
        printf '\n## %s\n' "$*"
        "$@"
    } >>"${OUT}" 2>&1 || true
}

{
    echo "timestamp_utc=${STAMP}"
    echo "hostname=$(hostname)"
    echo "kernel=$(uname -r)"
    echo "machine=$(uname -m)"
    echo
    echo "## Azure instance metadata"
    curl -fsS -H Metadata:true \
        'http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01' \
        || true
    echo
} >"${OUT}"

run ip -br addr
run ip route
run ip -s link
run lspci
for dev in /sys/class/net/*; do
    [[ -e "${dev}" ]] || continue
    iface="$(basename "${dev}")"
    [[ "${iface}" == "lo" ]] && continue
    run ethtool -i "${iface}"
    run ethtool -l "${iface}"
    run ethtool -k "${iface}"
    run ethtool -S "${iface}"
    run tc qdisc show dev "${iface}"
done
run sysctl net.ipv4.tcp_congestion_control
run sysctl net.ipv4.tcp_available_congestion_control
run sysctl net.core.rmem_max
run sysctl net.core.wmem_max
run sysctl net.ipv4.tcp_rmem
run sysctl net.ipv4.tcp_wmem
run sysctl net.ipv4.tcp_mtu_probing
run systemctl is-active irqbalance
run systemctl status irqbalance --no-pager
run nproc
run lscpu
run free -h
run df -h
run awk 'NR == 1 || /eth|ens|mlx|hv/ { print }' /proc/interrupts

echo "preflight=${OUT}"
