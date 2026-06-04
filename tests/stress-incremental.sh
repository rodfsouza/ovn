#!/bin/bash
#
# OVN Incremental Processing Stress Test
#
# Usage: ./tests/stress-incremental.sh [northd_ctl_path]
#
# Prerequisites:
#   - NB and SB ovsdb-servers running
#   - northd running with --unixctl
#   - OVN_NB_DB and OVN_SB_DB set (or defaults to /tmp/ovn-test sockets)
#
# Example:
#   export OVN_NB_DB=unix:/tmp/ovn-test/nb/ovnnb.sock
#   export OVN_SB_DB=unix:/tmp/ovn-test/sb/ovnsb.sock
#   ./tests/stress-incremental.sh /tmp/ovn-test/northd.ctl

set -e

NORTHD_CTL="${1:-/var/run/ovn/ovn-northd.ctl}"
PASS=0
FAIL=0
TOTAL=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${YELLOW}[INFO]${NC} $*"; }
log_pass()  { echo -e "${GREEN}[PASS]${NC} $*"; PASS=$((PASS + 1)); TOTAL=$((TOTAL + 1)); }
log_fail()  { echo -e "${RED}[FAIL]${NC} $*"; FAIL=$((FAIL + 1)); TOTAL=$((TOTAL + 1)); }

# Clear engine stats
clear_stats() {
    ovs-appctl -t "$NORTHD_CTL" inc-engine/clear-stats
}

# Get engine stat value
# Usage: get_stat <node> <field>  (field: recompute or compute)
get_stat() {
    local node=$1 field=$2
    local stats
    stats=$(ovs-appctl -t "$NORTHD_CTL" inc-engine/show-stats "$node")
    case "$field" in
        recompute) echo "$stats" | grep recompute | awk -F: '{print $2}' | tr -d ' ' ;;
        compute)   echo "$stats" | grep -v recompute | grep 'compute:' | awk -F: '{print $2}' | tr -d ' ' ;;
    esac
}

# Assert engine stats
# Usage: assert_incremental <test_name>
assert_incremental() {
    local name=$1
    local northd_recompute northd_compute lflow_recompute lflow_compute

    northd_recompute=$(get_stat northd recompute)
    northd_compute=$(get_stat northd compute)
    lflow_recompute=$(get_stat lflow recompute)
    lflow_compute=$(get_stat lflow compute)

    local ok=true
    if [ "$northd_recompute" -ne 0 ]; then
        log_fail "$name: northd recompute=$northd_recompute (expected 0)"
        ok=false
    fi
    if [ "$northd_compute" -eq 0 ]; then
        log_fail "$name: northd compute=$northd_compute (expected > 0)"
        ok=false
    fi
    if [ "$lflow_recompute" -ne 0 ]; then
        log_fail "$name: lflow recompute=$lflow_recompute (expected 0)"
        ok=false
    fi
    if [ "$ok" = true ]; then
        log_pass "$name (northd compute=$northd_compute, lflow compute=$lflow_compute)"
    fi
}

# Assert SB row count
# Usage: assert_row_count <table> <expected> [condition...]
assert_row_count() {
    local table=$1 expected=$2
    shift 2
    local actual
    actual=$(ovn-sbctl --no-headings --columns=_uuid find "$table" "$@" | grep -c . || true)
    if [ "$actual" -eq "$expected" ]; then
        log_pass "SB $table count=$actual (expected $expected) $*"
    else
        log_fail "SB $table count=$actual (expected $expected) $*"
    fi
}

# Assert NB row count
assert_nb_row_count() {
    local table=$1 expected=$2
    shift 2
    local actual
    actual=$(ovn-nbctl --no-headings --columns=_uuid find "$table" "$@" | grep -c . || true)
    if [ "$actual" -eq "$expected" ]; then
        log_pass "NB $table count=$actual (expected $expected) $*"
    else
        log_fail "NB $table count=$actual (expected $expected) $*"
    fi
}

# Verify SB flows exist for a datapath
assert_has_flows() {
    local datapath=$1
    local count
    count=$(ovn-sbctl dump-flows "$datapath" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$count" -gt 0 ]; then
        log_pass "SB flows for $datapath: $count flows"
    else
        log_fail "SB flows for $datapath: 0 flows (expected > 0)"
    fi
}

# Verify incremental matches full recompute
verify_recompute_consistency() {
    log_info "Verifying incremental state matches full recompute..."

    # Dump SB state before recompute
    ovn-sbctl dump-flows | sort > /tmp/ovn-stress-before.txt
    ovn-sbctl --no-headings --columns=logical_port,type,tunnel_key list Port_Binding | sort > /tmp/ovn-stress-pb-before.txt
    ovn-sbctl --no-headings --columns=tunnel_key list Datapath_Binding | sort > /tmp/ovn-stress-dp-before.txt

    # Force full recompute
    ovs-appctl -t "$NORTHD_CTL" inc-engine/recompute
    ovn-nbctl --wait=sb sync

    # Dump SB state after recompute
    ovn-sbctl dump-flows | sort > /tmp/ovn-stress-after.txt
    ovn-sbctl --no-headings --columns=logical_port,type,tunnel_key list Port_Binding | sort > /tmp/ovn-stress-pb-after.txt
    ovn-sbctl --no-headings --columns=tunnel_key list Datapath_Binding | sort > /tmp/ovn-stress-dp-after.txt

    local ok=true
    if ! diff -q /tmp/ovn-stress-before.txt /tmp/ovn-stress-after.txt > /dev/null 2>&1; then
        log_fail "Logical_Flow mismatch after recompute"
        diff /tmp/ovn-stress-before.txt /tmp/ovn-stress-after.txt | head -20
        ok=false
    fi
    if ! diff -q /tmp/ovn-stress-pb-before.txt /tmp/ovn-stress-pb-after.txt > /dev/null 2>&1; then
        log_fail "Port_Binding mismatch after recompute"
        diff /tmp/ovn-stress-pb-before.txt /tmp/ovn-stress-pb-after.txt | head -20
        ok=false
    fi
    if ! diff -q /tmp/ovn-stress-dp-before.txt /tmp/ovn-stress-dp-after.txt > /dev/null 2>&1; then
        log_fail "Datapath_Binding mismatch after recompute"
        diff /tmp/ovn-stress-dp-before.txt /tmp/ovn-stress-dp-after.txt | head -20
        ok=false
    fi
    if [ "$ok" = true ]; then
        log_pass "Incremental state matches full recompute"
    fi

    rm -f /tmp/ovn-stress-before.txt /tmp/ovn-stress-after.txt
    rm -f /tmp/ovn-stress-pb-before.txt /tmp/ovn-stress-pb-after.txt
    rm -f /tmp/ovn-stress-dp-before.txt /tmp/ovn-stress-dp-after.txt
}

###############################################################################
# TESTS
###############################################################################

echo ""
echo "=============================================="
echo "  OVN Incremental Processing Stress Test"
echo "=============================================="
echo ""

# Check connectivity
log_info "Checking northd connectivity..."
ovs-appctl -t "$NORTHD_CTL" version > /dev/null 2>&1 || {
    echo "ERROR: Cannot connect to northd at $NORTHD_CTL"
    exit 1
}
log_info "northd is running."
echo ""

# -------------------------------------------------------------------
echo "--- Phase 0: Pre-test Cleanup ---"
# -------------------------------------------------------------------
# Remove any leftover resources from a previous run.
cleanup_stale() {
    local found=false
    for i in $(seq 1 10); do
        ovn-nbctl --if-exists lr-del "stress-lr$i" 2>/dev/null && found=true
    done
    for i in $(seq 1 3); do
        ovn-nbctl --if-exists lr-del "stress-lr-lb$i" 2>/dev/null && found=true
    done
    for i in $(seq 1 3); do
        ovn-nbctl --if-exists lr-del "stress-bulk-lr$i" 2>/dev/null && found=true
    done
    for i in $(seq 1 5); do
        ovn-nbctl --if-exists ls-del "stress-ls$i" 2>/dev/null && found=true
    done
    for i in $(seq 1 3); do
        ovn-nbctl --if-exists lb-del "stress-lb$i" 2>/dev/null && found=true
    done
    if [ "$found" = true ]; then
        ovn-nbctl --wait=sb sync 2>/dev/null || true
    fi
    echo "$found"
}

stale=$(cleanup_stale)
if [ "$stale" = true ]; then
    log_info "Cleaned up stale resources from previous run."
    sleep 1
else
    log_info "No stale resources found."
fi
echo ""

# -------------------------------------------------------------------
echo "--- Phase 1: Standalone Router Creation (x5) ---"
# -------------------------------------------------------------------
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb lr-add "stress-lr$i"
    assert_incremental "standalone router stress-lr$i"
done
assert_row_count Datapath_Binding 5

echo ""
# -------------------------------------------------------------------
echo "--- Phase 2: Router + Port Creation (x5) ---"
# -------------------------------------------------------------------
for i in $(seq 6 10); do
    clear_stats
    ovn-nbctl --wait=sb lr-add "stress-lr$i" \
        -- lrp-add "stress-lr$i" "stress-rp$i" \
           "00:00:00:00:$(printf '%02x' $i):01" "10.$i.0.1/24"
    assert_incremental "router+port stress-lr$i"
done
assert_row_count Datapath_Binding 10

echo ""
# -------------------------------------------------------------------
echo "--- Phase 3: Logical Switches + VIF Ports ---"
# -------------------------------------------------------------------
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb ls-add "stress-ls$i"
    # Add 3 VIF ports per switch
    for j in $(seq 1 3); do
        ovn-nbctl --wait=sb \
            lsp-add "stress-ls$i" "stress-vif${i}_${j}" \
            -- lsp-set-addresses "stress-vif${i}_${j}" \
               "00:00:0a:00:$(printf '%02x' $i):$(printf '%02x' $j) 10.100.$i.$j"
    done
done
assert_nb_row_count Logical_Switch_Port 15

echo ""
# -------------------------------------------------------------------
echo "--- Phase 4: Connect Routers to Switches ---"
# -------------------------------------------------------------------
for i in $(seq 1 5); do
    clear_stats
    # Add router port on lr$((i+5)) connecting to ls$i
    ovn-nbctl --wait=sb \
        lrp-add "stress-lr$((i+5))" "stress-lrp-to-ls$i" \
            "00:00:00:00:$(printf '%02x' $i):ff" "10.100.$i.254/24"
    # Add switch port of type=router on ls$i
    ovn-nbctl --wait=sb \
        lsp-add "stress-ls$i" "stress-lsp-to-lr$((i+5))" \
        -- lsp-set-type "stress-lsp-to-lr$((i+5))" router \
        -- lsp-set-addresses "stress-lsp-to-lr$((i+5))" router \
        -- lsp-set-options "stress-lsp-to-lr$((i+5))" \
           "router-port=stress-lrp-to-ls$i"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 5: Static Routes (x10) ---"
# -------------------------------------------------------------------
for i in $(seq 1 10); do
    clear_stats
    ovn-nbctl --wait=sb lr-route-add "stress-lr6" \
        "192.168.$i.0/24" "10.100.1.$(( (i % 254) + 1 ))"
    assert_incremental "static route 192.168.$i.0/24"
done
assert_has_flows stress-lr6

echo ""
# -------------------------------------------------------------------
echo "--- Phase 6: Policies (x5) ---"
# -------------------------------------------------------------------
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb lr-policy-add "stress-lr6" \
        "$((100 + i))" "ip4.src == 172.16.$i.0/24" allow
    assert_incremental "policy priority=$((100 + i))"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 7: NAT Rules ---"
# -------------------------------------------------------------------
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb lr-nat-add "stress-lr6" \
        dnat_and_snat "200.0.0.$i" "10.100.1.$i"
    assert_incremental "NAT dnat_and_snat 200.0.0.$i"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 8: Load Balancers ---"
# -------------------------------------------------------------------
for i in $(seq 1 3); do
    ovn-nbctl --wait=sb lb-add "stress-lb$i" \
        "10.200.$i.1:80" "10.100.1.1:80,10.100.1.2:80"
done

for i in $(seq 1 3); do
    clear_stats
    ovn-nbctl --wait=sb lr-add "stress-lr-lb$i" \
        -- lr-lb-add "stress-lr-lb$i" "stress-lb$i"
    assert_incremental "router+LB stress-lr-lb$i"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 9: LRP Add on Existing Routers (x10) ---"
# -------------------------------------------------------------------
for i in $(seq 1 10); do
    clear_stats
    ovn-nbctl --wait=sb lrp-add "stress-lr1" \
        "stress-extra-rp$i" \
        "00:00:aa:00:$(printf '%02x' $i):01" "10.200.$i.1/24"
    assert_incremental "LRP add stress-extra-rp$i on stress-lr1"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 10: LRP Delete (x5) ---"
# -------------------------------------------------------------------
# LRP deletion may trigger recompute depending on port type.
# Just verify northd processed (compute > 0) without asserting
# recompute == 0.
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb lrp-del "stress-extra-rp$i"
    local_compute=$(get_stat northd compute)
    local_recompute=$(get_stat northd recompute)
    if [ "$local_compute" -gt 0 ] || [ "$local_recompute" -gt 0 ]; then
        log_pass "LRP delete stress-extra-rp$i (northd compute=$local_compute recompute=$local_recompute)"
    else
        log_fail "LRP delete stress-extra-rp$i: northd did not process"
    fi
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 11: Bulk Operations (single transaction) ---"
# -------------------------------------------------------------------
clear_stats
ovn-nbctl --wait=sb \
    lr-add stress-bulk-lr1 \
    -- lrp-add stress-bulk-lr1 stress-bulk-rp1 00:00:bb:00:00:01 10.250.1.1/24 \
    -- lr-add stress-bulk-lr2 \
    -- lrp-add stress-bulk-lr2 stress-bulk-rp2 00:00:bb:00:00:02 10.250.2.1/24 \
    -- lr-add stress-bulk-lr3 \
    -- lrp-add stress-bulk-lr3 stress-bulk-rp3 00:00:bb:00:00:03 10.250.3.1/24
assert_incremental "bulk create 3 routers + 3 ports in 1 txn"

echo ""
# -------------------------------------------------------------------
echo "--- Phase 12: SB State Verification ---"
# -------------------------------------------------------------------

# Count total datapaths (routers)
total_lr=$(ovn-nbctl --no-headings --columns=_uuid list Logical_Router | grep -c . || true)
total_ls=$(ovn-nbctl --no-headings --columns=_uuid list Logical_Switch | grep -c . || true)
total_dp=$(ovn-sbctl --no-headings --columns=_uuid list Datapath_Binding | grep -c . || true)
expected_dp=$((total_lr + total_ls))

if [ "$total_dp" -eq "$expected_dp" ]; then
    log_pass "Datapath_Binding count: $total_dp (LR=$total_lr + LS=$total_ls)"
else
    log_fail "Datapath_Binding count: $total_dp (expected $expected_dp = LR=$total_lr + LS=$total_ls)"
fi

# Verify every router has flows
for lr in stress-lr6 stress-lr7 stress-lr-lb1 stress-bulk-lr1; do
    assert_has_flows "$lr"
done

# Verify Port_Bindings exist for router ports
for rp in stress-rp6 stress-rp7 stress-bulk-rp1 stress-extra-rp6; do
    count=$(ovn-sbctl --no-headings find Port_Binding logical_port="$rp" | grep -c . || true)
    if [ "$count" -gt 0 ]; then
        log_pass "Port_Binding exists for $rp"
    else
        log_fail "Port_Binding missing for $rp"
    fi
done

# Verify deleted ports are gone
for rp in stress-extra-rp1 stress-extra-rp2 stress-extra-rp3; do
    count=$(ovn-sbctl --no-headings find Port_Binding logical_port="$rp" | grep -c . || true)
    if [ "$count" -eq 0 ]; then
        log_pass "Deleted Port_Binding gone for $rp"
    else
        log_fail "Deleted Port_Binding still exists for $rp"
    fi
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 13: Consistency Check (incremental vs recompute) ---"
# -------------------------------------------------------------------
verify_recompute_consistency

echo ""
# -------------------------------------------------------------------
echo "--- Phase 14: Cleanup and Delete ---"
# -------------------------------------------------------------------
clear_stats
for i in $(seq 1 10); do
    ovn-nbctl --wait=sb lr-del "stress-lr$i" 2>/dev/null || true
done
for i in $(seq 1 3); do
    ovn-nbctl --wait=sb lr-del "stress-lr-lb$i" 2>/dev/null || true
done
for i in $(seq 1 3); do
    ovn-nbctl --wait=sb lr-del "stress-bulk-lr$i" 2>/dev/null || true
done
for i in $(seq 1 5); do
    ovn-nbctl --wait=sb ls-del "stress-ls$i" 2>/dev/null || true
done
for i in $(seq 1 3); do
    ovn-nbctl --wait=sb lb-del "stress-lb$i" 2>/dev/null || true
done
log_info "Cleanup complete."

# Verify everything is cleaned up
remaining=$(ovn-sbctl --no-headings --columns=_uuid list Datapath_Binding | grep -c . || true)
if [ "$remaining" -eq 0 ]; then
    log_pass "All datapaths cleaned up ($remaining remaining)"
else
    log_info "Remaining datapaths: $remaining (may be from prior tests)"
fi

###############################################################################
# SUMMARY
###############################################################################
echo ""
echo "=============================================="
echo "  RESULTS: $PASS passed, $FAIL failed ($TOTAL total)"
echo "=============================================="

if [ "$FAIL" -gt 0 ]; then
    echo -e "${RED}SOME TESTS FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}ALL TESTS PASSED${NC}"
    exit 0
fi
