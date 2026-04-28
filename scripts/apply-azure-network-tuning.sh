#!/usr/bin/env bash
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
    exec sudo "$0" "$@"
fi

modprobe tcp_bbr 2>/dev/null || true
printf 'tcp_bbr\n' >/etc/modules-load.d/99-cloudbench-bbr.conf

rm -f /etc/sysctl.d/99-cloudbench-azure-network.conf
cat >/etc/sysctl.d/99zz-cloudbench-azure-network.conf <<'CONF'
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.ipv4.tcp_rmem = 4096 87380 134217728
net.ipv4.tcp_wmem = 4096 65536 134217728
net.ipv4.tcp_mtu_probing = 1
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
CONF

sysctl --system >/dev/null
sysctl -w \
    net.core.rmem_max=134217728 \
    net.core.wmem_max=134217728 \
    'net.ipv4.tcp_rmem=4096 87380 134217728' \
    'net.ipv4.tcp_wmem=4096 65536 134217728' \
    net.ipv4.tcp_mtu_probing=1 \
    net.core.default_qdisc=fq \
    net.ipv4.tcp_congestion_control=bbr \
    >/dev/null

cat >/etc/udev/rules.d/99-cloudbench-azure-net.rules <<'CONF'
SUBSYSTEM=="net", ACTION=="add|change", KERNEL=="eth*", ATTR{tx_queue_len}="10000"
SUBSYSTEM=="net", ACTION=="add|change", KERNEL=="en*", ATTR{tx_queue_len}="10000"
ACTION=="add|change", SUBSYSTEM=="net", KERNEL=="eth*", RUN+="/sbin/tc qdisc replace dev %k root fq"
ACTION=="add|change", SUBSYSTEM=="net", KERNEL=="en*", RUN+="/sbin/tc qdisc replace dev %k root fq"
CONF

udevadm control --reload-rules || true

enable_irqbalance() {
    if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files irqbalance.service >/dev/null 2>&1; then
        systemctl enable --now irqbalance >/dev/null 2>&1 || true
    elif command -v service >/dev/null 2>&1; then
        service irqbalance start >/dev/null 2>&1 || true
    fi
}

tune_channels() {
    local iface="$1"
    command -v ethtool >/dev/null 2>&1 || return 0

    local max_combined
    max_combined="$(ethtool -l "${iface}" 2>/dev/null | awk '
        /^Pre-set maximums:/ { inmax = 1; next }
        /^Current hardware settings:/ { inmax = 0; next }
        inmax && $1 == "Combined:" { print $2; exit }
    ')"
    if [[ "${max_combined}" =~ ^[0-9]+$ ]] && (( max_combined > 0 )); then
        ethtool -L "${iface}" combined "${max_combined}" >/dev/null 2>&1 || true
    fi
}

enable_irqbalance
for dev in /sys/class/net/*; do
    [[ -e "${dev}" ]] || continue
    iface="$(basename "${dev}")"
    [[ "${iface}" == "lo" ]] && continue
    ip link set dev "${iface}" txqueuelen 10000 || true
    tc qdisc replace dev "${iface}" root fq || true
    tune_channels "${iface}"
done

echo "Applied CloudBench Azure network tuning. Reboot before final benchmark runs for the cleanest state."
