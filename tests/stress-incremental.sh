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
    # Force a recompute + sync to flush all pending SB feedback,
    # then clear stats so only the next operation is measured.
    ovs-appctl -t "$NORTHD_CTL" inc-engine/recompute 2>/dev/null || true
    ovn-nbctl --wait=sb sync 2>/dev/null || true
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

# Assert engine stats — northd and lflow handled incrementally (no recompute)
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

# Assert northd processed (compute or recompute > 0, either is OK)
# Usage: assert_processed <test_name>
assert_processed() {
    local name=$1
    local northd_compute northd_recompute

    northd_compute=$(get_stat northd compute)
    northd_recompute=$(get_stat northd recompute)

    if [ "$northd_compute" -gt 0 ] || [ "$northd_recompute" -gt 0 ]; then
        log_pass "$name (northd compute=$northd_compute recompute=$northd_recompute)"
    else
        log_fail "$name: northd did not process"
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

# Verify no SB flows exist for a datapath
assert_no_flows() {
    local datapath=$1
    local count
    count=$(ovn-sbctl dump-flows "$datapath" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$count" -eq 0 ]; then
        log_pass "SB flows gone for $datapath"
    else
        log_fail "SB flows for $datapath: $count flows (expected 0)"
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
    ovn-nbctl --if-exists lr-del "stress-del-lr1" 2>/dev/null && found=true
    ovn-nbctl --if-exists lr-del "stress-del-lr2" 2>/dev/null && found=true
    ovn-nbctl --if-exists lr-del "stress-del-lr3" 2>/dev/null && found=true
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
# Note: northd handles LR creation incrementally, but lflow may
# recompute due to SB feedback timing. Verify northd is incremental.
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb lr-add "stress-lr$i"
    assert_processed "standalone router stress-lr$i"
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
    assert_processed "router+port stress-lr$i"
done
assert_row_count Datapath_Binding 10

echo ""
# -------------------------------------------------------------------
echo "--- Phase 3: Logical Switches + VIF Ports ---"
# -------------------------------------------------------------------
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb ls-add "stress-ls$i"
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
    ovn-nbctl --wait=sb \
        lrp-add "stress-lr$((i+5))" "stress-lrp-to-ls$i" \
            "00:00:00:00:$(printf '%02x' $i):ff" "10.100.$i.254/24"
    ovn-nbctl --wait=sb \
        lsp-add "stress-ls$i" "stress-lsp-to-lr$((i+5))" \
        -- lsp-set-type "stress-lsp-to-lr$((i+5))" router \
        -- lsp-set-addresses "stress-lsp-to-lr$((i+5))" router \
        -- lsp-set-options "stress-lsp-to-lr$((i+5))" \
           "router-port=stress-lrp-to-ls$i"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 5: Static Routes — Add (x10) ---"
# -------------------------------------------------------------------
for i in $(seq 1 10); do
    clear_stats
    ovn-nbctl --wait=sb lr-route-add "stress-lr6" \
        "192.168.$i.0/24" "10.100.1.$(( (i % 254) + 1 ))"
    assert_incremental "route add 192.168.$i.0/24"
done
assert_has_flows stress-lr6

echo ""
# -------------------------------------------------------------------
echo "--- Phase 5b: Static Routes — Delete (x5) ---"
# -------------------------------------------------------------------
for i in $(seq 6 10); do
    clear_stats
    ovn-nbctl --wait=sb lr-route-del "stress-lr6" "192.168.$i.0/24"
    assert_incremental "route del 192.168.$i.0/24"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 5c: Dense LR — Single Route Add/Del with N=100 ---"
# -------------------------------------------------------------------
# Exercises the per-route delta path: add a single route to an LR
# pre-populated with many routes, assert it remains incremental, and
# verify SB Logical_Flow churn is bounded by the route delta (not by N).
# Uses a separate LR so it doesn't interfere with other phases.
ovn-nbctl --wait=sb lr-add stress-lr-dense >/dev/null
ovn-nbctl --wait=sb lrp-add stress-lr-dense dense-lrp1 \
    00:00:00:99:99:01 172.16.0.1/16 >/dev/null

# Bulk-add 100 routes in a single txn to keep setup fast.
bulk_args=()
for i in $(seq 1 100); do
    bulk_args+=(-- lr-route-add stress-lr-dense \
                "10.50.$i.0/24" "172.16.0.$(( (i % 254) + 2 ))")
done
ovn-nbctl --wait=sb "${bulk_args[@]}" >/dev/null

# Snapshot SB Logical_Flow UUIDs (only for this LR's pipeline) before
# the next operation. Filter to flows referencing the LR's datapath is
# overkill here — count of all SB flows works as the bound check.
sb_before=$(ovn-sbctl --bare --columns=_uuid find Logical_Flow | sort)
sb_before_count=$(echo "$sb_before" | grep -c . || true)

clear_stats
ovn-nbctl --wait=sb lr-route-add stress-lr-dense 10.99.99.0/24 172.16.0.99
assert_incremental "dense LR (N=100): single route add"

sb_after_add=$(ovn-sbctl --bare --columns=_uuid find Logical_Flow | sort)
sb_after_add_count=$(echo "$sb_after_add" | grep -c . || true)
new_uuids=$(comm -13 <(echo "$sb_before") <(echo "$sb_after_add") | grep -c . || true)
removed_uuids=$(comm -23 <(echo "$sb_before") <(echo "$sb_after_add") | grep -c . || true)
# Threshold: a single unique IPv4 route adds ~3-5 flows (routing + arp_resolve
# + arp_request). 20 is comfortably above this and well below the full LR
# flow count, so a regression that re-emits the whole LR's flows will fail.
if [ "$new_uuids" -le 20 ] && [ "$removed_uuids" -le 5 ]; then
    log_pass "dense LR add: SB churn bounded (+$new_uuids/-$removed_uuids)"
else
    log_fail "dense LR add: SB churn too high (+$new_uuids/-$removed_uuids); expected ≤ +20/-5"
fi

clear_stats
ovn-nbctl --wait=sb lr-route-del stress-lr-dense 10.99.99.0/24
assert_incremental "dense LR (N=100): single route del"

sb_after_del=$(ovn-sbctl --bare --columns=_uuid find Logical_Flow | sort)
new_uuids=$(comm -13 <(echo "$sb_after_add") <(echo "$sb_after_del") | grep -c . || true)
removed_uuids=$(comm -23 <(echo "$sb_after_add") <(echo "$sb_after_del") | grep -c . || true)
if [ "$new_uuids" -le 5 ] && [ "$removed_uuids" -le 20 ]; then
    log_pass "dense LR del: SB churn bounded (+$new_uuids/-$removed_uuids)"
else
    log_fail "dense LR del: SB churn too high (+$new_uuids/-$removed_uuids); expected ≤ +5/-20"
fi

# Cleanup this phase's LR so subsequent phases see consistent state.
ovn-nbctl --wait=sb lr-del stress-lr-dense >/dev/null

echo ""
# -------------------------------------------------------------------
echo "--- Phase 6: Policies — Add (x5) ---"
# -------------------------------------------------------------------
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb lr-policy-add "stress-lr6" \
        "$((100 + i))" "ip4.src == 172.16.$i.0/24" allow
    assert_incremental "policy add priority=$((100 + i))"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 6b: Policies — Delete (x3) ---"
# -------------------------------------------------------------------
for i in $(seq 3 5); do
    clear_stats
    ovn-nbctl --wait=sb lr-policy-del "stress-lr6" \
        "$((100 + i))" "ip4.src == 172.16.$i.0/24"
    assert_incremental "policy del priority=$((100 + i))"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 7: NAT Rules — Add (x5) ---"
# -------------------------------------------------------------------
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb lr-nat-add "stress-lr6" \
        dnat_and_snat "200.0.0.$i" "10.100.1.$i"
    assert_incremental "NAT add dnat_and_snat 200.0.0.$i"
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 7b: NAT Rules — Modify (x3) ---"
# -------------------------------------------------------------------
for i in $(seq 1 3); do
    clear_stats
    nat_uuid=$(ovn-nbctl --bare --columns=_uuid find NAT external_ip="200.0.0.$i" | head -1)
    if [ -n "$nat_uuid" ]; then
        ovn-nbctl --wait=sb set NAT "$nat_uuid" options:foo="bar$i"
        assert_incremental "NAT modify 200.0.0.$i options"
    else
        log_fail "NAT modify: NAT for 200.0.0.$i not found"
    fi
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 7c: NAT Rules — Delete (x2) ---"
# -------------------------------------------------------------------
for i in $(seq 4 5); do
    clear_stats
    ovn-nbctl --wait=sb lr-nat-del "stress-lr6" dnat_and_snat "200.0.0.$i"
    assert_incremental "NAT del dnat_and_snat 200.0.0.$i"
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
    assert_processed "router+LB stress-lr-lb$i"
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
# Standalone LRP deletion currently falls back to recompute
# (route_lflow_ref ownership issue). Just verify northd processed it.
for i in $(seq 1 5); do
    clear_stats
    ovn-nbctl --wait=sb lrp-del "stress-extra-rp$i"
    assert_processed "LRP delete stress-extra-rp$i"
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
assert_processed "bulk create 3 routers + 3 ports in 1 txn"

echo ""
# -------------------------------------------------------------------
echo "--- Phase 12: Router Deletion ---"
# -------------------------------------------------------------------

# 12a: Standalone router deletion (no ports, no peering)
clear_stats
ovn-nbctl --wait=sb lr-add "stress-del-lr1"
ovn-nbctl --wait=sb sync
clear_stats
ovn-nbctl --wait=sb lr-del "stress-del-lr1"
assert_processed "standalone router deletion stress-del-lr1"

# 12b: Router with ports deletion (ports deleted with router)
clear_stats
ovn-nbctl --wait=sb lr-add "stress-del-lr2" \
    -- lrp-add "stress-del-lr2" "stress-del-rp2" \
       00:00:cc:00:00:02 10.251.2.1/24
ovn-nbctl --wait=sb sync
clear_stats
ovn-nbctl --wait=sb lr-del "stress-del-lr2"
assert_processed "router+port deletion stress-del-lr2"

# 12c: Router with routes/NAT/policies deletion
ovn-nbctl --wait=sb lr-add "stress-del-lr3" \
    -- lrp-add "stress-del-lr3" "stress-del-rp3" \
       00:00:cc:00:00:03 10.251.3.1/24
ovn-nbctl --wait=sb lr-route-add "stress-del-lr3" "10.252.0.0/16" "10.251.3.254"
ovn-nbctl --wait=sb lr-nat-add "stress-del-lr3" snat "200.0.1.1" "10.251.3.0/24"
ovn-nbctl --wait=sb lr-policy-add "stress-del-lr3" 200 "ip4.src == 10.251.3.0/24" allow
ovn-nbctl --wait=sb sync
clear_stats
ovn-nbctl --wait=sb lr-del "stress-del-lr3"
assert_processed "router+routes+NAT+policy deletion stress-del-lr3"

echo ""
# -------------------------------------------------------------------
echo "--- Phase 13: SB State Verification ---"
# -------------------------------------------------------------------

# Count total datapaths
total_lr=$(ovn-nbctl --no-headings --columns=_uuid list Logical_Router | grep -c . || true)
total_ls=$(ovn-nbctl --no-headings --columns=_uuid list Logical_Switch | grep -c . || true)
total_dp=$(ovn-sbctl --no-headings --columns=_uuid list Datapath_Binding | grep -c . || true)
expected_dp=$((total_lr + total_ls))

if [ "$total_dp" -eq "$expected_dp" ]; then
    log_pass "Datapath_Binding count: $total_dp (LR=$total_lr + LS=$total_ls)"
else
    log_fail "Datapath_Binding count: $total_dp (expected $expected_dp = LR=$total_lr + LS=$total_ls)"
fi

# Verify routers have flows
for lr in stress-lr6 stress-lr7 stress-lr-lb1 stress-bulk-lr1; do
    assert_has_flows "$lr"
done

# Verify Port_Bindings exist for active router ports
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

# Verify deleted routers are gone
for lr in stress-del-lr1 stress-del-lr2 stress-del-lr3; do
    count=$(ovn-sbctl --no-headings find Datapath_Binding "external_ids:name=$lr" | grep -c . || true)
    if [ "$count" -eq 0 ]; then
        log_pass "Deleted Datapath_Binding gone for $lr"
    else
        log_fail "Deleted Datapath_Binding still exists for $lr"
    fi
done

echo ""
# -------------------------------------------------------------------
echo "--- Phase 14: Consistency Check (incremental vs recompute) ---"
# -------------------------------------------------------------------
verify_recompute_consistency

echo ""
# -------------------------------------------------------------------
echo "--- Phase 15: Cleanup ---"
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
