#!/usr/bin/env bash
set -euo pipefail

LOGFILE="/var/log/egressrouter-init.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "[$(date -Is)] === Egress Router Init Script Starting ==="

# --- IP Forwarding & conntrack ---
echo "[$(date -Is)] Enabling IP forwarding and connection tracking..."
modprobe nf_conntrack
modprobe xt_mark
modprobe xt_connmark
modprobe xt_statistic

echo nf_conntrack >> /etc/modules-load.d/egressrouter.conf

echo 'net.ipv4.ip_forward=1' > /etc/sysctl.d/99-egressrouter.conf
echo 'net.netfilter.nf_conntrack_max=1048576' >> /etc/sysctl.d/99-egressrouter.conf
echo 'net.ipv4.ip_local_port_range=1024 65535' >> /etc/sysctl.d/99-egressrouter.conf
sysctl --system

echo 262144 > /sys/module/nf_conntrack/parameters/hashsize

echo 'options nf_conntrack hashsize=262144' > /etc/modprobe.d/nf_conntrack.conf

# --- Install dependencies ---
echo "[$(date -Is)] Installing iptables, conntrack, ipset..."
echo iptables-persistent iptables-persistent/autosave_v4 boolean true | debconf-set-selections || true
echo iptables-persistent iptables-persistent/autosave_v6 boolean true | debconf-set-selections || true
echo ipset-persistent ipset-persistent/autosave boolean true | debconf-set-selections || true
DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y \
  iptables iptables-persistent conntrack ipset ipset-persistent bmon nload iftop nethogs

# --- Interface discovery ---
MGMT_SUBNET="10.0.4."
WAN_SUBNET="10.0.5."

WAN_IPS=( $(ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1 | grep "^${WAN_SUBNET}") )
[[ ${#WAN_IPS[@]} -gt 0 ]] || { echo "No WAN IPs detected."; exit 1; }

MGMT_IP=$(ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1 | grep "^${MGMT_SUBNET}" | head -n1)
[[ -n "$MGMT_IP" ]] || { echo "No MGMT IP detected."; exit 1; }

WAN_IFACE=$(ip -4 -o addr show | grep "$WAN_SUBNET" | awk '{print $2}' | head -n1)
MGMT_IFACE=$(ip -4 -o addr show | grep "$MGMT_SUBNET" | awk '{print $2}' | head -n1)

WAN_GW=$(ip route | grep "$WAN_IFACE" | awk '/via/ {print $3}' | head -n1)
[[ -n "$WAN_GW" ]] || WAN_GW="${WAN_SUBNET}1"

WAN_PRIMARY_IP=$(ip route show default dev "$WAN_IFACE" \
  | awk '{for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}')

MGMT_GW=$(ip route | grep "$MGMT_IFACE" | awk '/via/ {print $3}' | head -n1)
[[ -n "$MGMT_GW" ]] || MGMT_GW="${MGMT_SUBNET}1"

echo "[$(date -Is)] WAN iface: $WAN_IFACE, IPs: ${WAN_IPS[*]}"
echo "[$(date -Is)] MGMT iface: $MGMT_IFACE, IP: $MGMT_IP"

# --- Disable reverse-path filtering ---
cat <<EOF >/etc/sysctl.d/99-rpfilter.conf
net.ipv4.conf.all.rp_filter=0
net.ipv4.conf.default.rp_filter=0
net.ipv4.conf.$WAN_IFACE.rp_filter=0
net.ipv4.conf.$MGMT_IFACE.rp_filter=0
EOF
sysctl --system

# --- Flush existing rules ---
echo "[$(date -Is)] Flushing existing iptables and ipsets..."
iptables -t nat    -F POSTROUTING
iptables -t nat    -F
iptables -t nat    -X
iptables -t mangle -F PREROUTING
iptables -t mangle -F
iptables -t mangle -X

# --- Set up NOSNAT destinations ---
ipset create nosnat-dests hash:net || true
ipset add nosnat-dests 10.0.0.0/8    || true
ipset add nosnat-dests 168.63.129.16 || true
ipset add nosnat-dests 169.254.169.254 || true
ipset add nosnat-dests 100.64.0.0/10 || true
ipset add nosnat-dests 192.168.0.0/16 || true
ipset add nosnat-dests 172.16.0.0/12 || true
ipset save > /etc/ipset.conf

# --- Restore connmark early ---
iptables -t nat    -I POSTROUTING 1 -j CONNMARK --restore-mark
iptables -t mangle -I PREROUTING 1 -j CONNMARK --restore-mark

# --- Round-robin marking & save ---
COUNT=${#WAN_IPS[@]}
for i in "${!WAN_IPS[@]}"; do
  MARK=$((i+1))
  iptables -t mangle -A PREROUTING \
    -s 10.0.0.0/8 \
    -m conntrack --ctstate NEW \
    -m statistic --mode nth --every "$COUNT" --packet "$i" \
    -j MARK --set-mark "$MARK"
  iptables -t mangle -A PREROUTING \
    -m conntrack --ctstate NEW -j CONNMARK --save-mark
done

# --- Exempt internal/Azure infra ---
iptables -t nat -A POSTROUTING -m set --match-set nosnat-dests dst -j RETURN

# --- SNAT based on mark ---
for i in "${!WAN_IPS[@]}"; do
  MARK=$((i+1))
  iptables -t nat -A POSTROUTING \
    -s 10.0.0.0/8 -o "$WAN_IFACE" \
    -m mark --mark "$MARK" \
    -j SNAT --to-source "${WAN_IPS[$i]}" 
done

# --- Azure metadata & DNS routing via WAN iface ---
# Route 168.63.129.16 (Azure DNS) and 169.254.169.254 (IMDS) on primary WAN link
ip route replace 168.63.129.16/32 via $WAN_GW dev $WAN_IFACE proto dhcp src $WAN_PRIMARY_IP metric 100
ip route replace 169.254.169.254/32 via $WAN_GW dev $WAN_IFACE proto dhcp src $WAN_PRIMARY_IP metric 100

# --- egresswan table for WAN subnet ---
if ! grep -q '^100 egresswan' /etc/iproute2/rt_tables; then
  echo "100 egresswan" >> /etc/iproute2/rt_tables
fi
ip route replace default via "$WAN_GW" dev "$WAN_IFACE" table egresswan
while ip rule show | grep -q "from ${WAN_SUBNET}0/24 lookup egresswan"; do
  ip rule del from ${WAN_SUBNET}0/24 table egresswan || true
done
ip rule add from ${WAN_SUBNET}0/24 table egresswan
ip route flush cache

# --- Persist rules ---
if command -v netfilter-persistent &>/dev/null; then
  netfilter-persistent save
fi

echo "[$(date -Is)] === Egress Router Init Script Complete ==="