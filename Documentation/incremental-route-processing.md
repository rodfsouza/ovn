# Incremental Route Processing in OVN northd

## Overview

The `en_group_ecmp_route` engine node provides per-route and per-ECMP-group
incremental processing of static routes in northd. It sits between `en_northd`
and `en_lflow` in the incremental processing engine (IPE) DAG:

```
NB route change
  -> en_northd           (detects change, sets NORTHD_TRACKED_LR_ROUTES)
  -> en_group_ecmp_route (regroups ECMP, populates tracked deleted/crupdated)
  -> en_lflow            (unlinks old flows, builds new flows, syncs to SB)
  -> SB Logical_Flow     (only changed rows written)
  -> ovn-controller      (processes only the diff)
```

Without this node, any route change causes a full lflow recompute, rewriting
ALL route flows for ALL routers to SB, which triggers ovn-controller to
flood-remove and re-add OpenFlow rules on every chassis.

## Architecture

### Engine Node DAG

```
en_northd
  |
  +-- en_group_ecmp_route  (handler: en_group_ecmp_route_northd_handler)
  |     |
  |     +-- en_lflow       (handler: lflow_group_ecmp_route_handler)
  |
  +-- en_lflow             (handler: lflow_northd_handler)
```

### Key Data Structures

**File: `northd/en-group-ecmp-route.h`**

```c
struct ecmp_route_node {
    struct hmap_node hmap_node;      /* In group_ecmp_datapath.route_nodes */
    const struct ovn_datapath *od;
    struct lflow_ref *lflow_ref;     /* Per-route/group flow tracking */
    bool is_ecmp;
    union {
        const struct parsed_route *route;   /* Non-ECMP: single route */
        struct ecmp_groups_node *group;      /* ECMP: group of routes */
    };
};

struct group_ecmp_datapath {
    struct hmap_node hmap_node;      /* In group_ecmp_route_data.datapaths */
    const struct ovn_datapath *od;
    struct hmap ecmp_groups;         /* ECMP groups for this router */
    struct hmap unique_routes;       /* Non-ECMP unique routes */
    struct ovs_list parsed_routes;   /* All parsed routes */
    struct hmap route_nodes;         /* Per-route/group nodes with lflow_refs */
};

struct group_ecmp_route_tracked_data {
    struct hmapx deleted_datapath_routes;    /* ecmp_route_node * */
    struct hmapx crupdated_datapath_routes;  /* ecmp_route_node * */
};
```

### Flow of Data

1. **Full recompute** (`en_group_ecmp_route_run`):
   - Iterates all LR datapaths
   - For each router: parses routes, computes ECMP groups, creates `ecmp_route_node` entries
   - Each node gets its own `lflow_ref` for independent SB flow tracking

2. **Incremental handler** (`en_group_ecmp_route_northd_handler`):
   - Triggered when northd sets `NORTHD_TRACKED_LR_ROUTES` or `NORTHD_TRACKED_LR_CREATED`
   - For route changes: removes old route_nodes from hmap -> marks as deleted -> destroys old groups -> rebuilds -> marks new nodes as crupdated
   - For new routers: builds from scratch, marks all as crupdated

3. **Lflow handler** (`lflow_group_ecmp_route_handler`):
   - Iterates `deleted_datapath_routes`: unlinks lflow_ref, syncs to SB (removes flows)
   - Iterates `crupdated_datapath_routes`: unlinks lflow_ref, rebuilds flows, syncs to SB (adds/updates flows)

### Route Table ID Assignment

Route table IDs are synthetic integers assigned by a `simap`. Both the
pre-flow builder (`build_route_table_lflow`) and the route parser
(`parsed_routes_add`) must use consistent IDs.

The `group_ecmp_route()` function pre-populates the `route_tables` simap
from LRP options before parsing routes, ensuring IDs match the pre-flows:

```c
/* Pre-populate from LRP options (same order as build_route_table_lflow) */
for (int i = 0; i < od->nbr->n_ports; i++) {
    const char *rt = smap_get(&od->nbr->ports[i]->options, "route_table");
    if (rt && rt[0]) {
        get_route_table_id(&route_tables, rt);
    }
}
```

### BFD Handling

BFD (Bidirectional Forwarding Detection) is handled specially because:

1. `en_group_ecmp_route` runs before `en_lflow`, so `build_bfd_table()` hasn't
   executed yet when routes are parsed
2. `bfd_connections` is only available during `en_lflow_run()`

**Design:**
- `parsed_routes_add()`: excludes routes with BFD status `admin_down`, `down`,
  or `NULL` (even when `bfd_connections` is NULL)
- `bfd_update_static_route_refs()`: called in `en_lflow_run()` after
  `build_bfd_table()` to set BFD refs and do the `admin_down` -> `down`
  status transition
- Northd handler: falls back to full recompute when any route has BFD
  (`goto fail` in `northd_handle_lr_changes`)

### Lifecycle of Deleted Route Nodes

When routes change incrementally:
1. Old `ecmp_route_node`s are removed from `ged->route_nodes` (hmap_remove)
2. Added to `trk_data.deleted_datapath_routes`
3. `parsed_routes_destroy()` frees old parsed_routes (making `rn->route` stale)
4. Lflow handler uses only `rn->lflow_ref` (never accesses stale pointers)
5. `clear_tracked_data()` frees the deleted nodes and their lflow_refs

This prevents use-after-free when `en_lflow_run()` does a full recompute
(iterating `ged->route_nodes` would hit stale pointers if nodes weren't removed).

## Files

| File | Purpose |
|------|---------|
| `northd/en-group-ecmp-route.h` | Struct definitions, function declarations |
| `northd/en-group-ecmp-route.c` | ECMP grouping, engine handlers, lifecycle |
| `northd/en-lflow.c` | `lflow_group_ecmp_route_handler`, BFD ref update |
| `northd/en-lflow.h` | Handler declaration |
| `northd/en-northd.c` | `northd_nb_static_route_handler`, `northd_nb_logical_router_handler` |
| `northd/northd.c` | `parsed_routes_add`, `bfd_update_static_route_refs`, flow builders |
| `northd/northd.h` | `parsed_route` struct, `get_route_table_id`, exported flow builders |
| `northd/inc-proc-northd.c` | Engine DAG wiring |
| `northd/automake.mk` | Build system registration |

## Testing

### Test Infrastructure

**Key macros** (defined in `tests/ovn-northd.at`):

| Macro | Purpose |
|-------|---------|
| `CHECK_NO_CHANGE_AFTER_RECOMPUTE` | Dumps SB tables before/after forced recompute, diffs to ensure incremental == full |
| `check_engine_stats <node> <recompute> <compute>` | Verifies engine node recompute/compute counts |
| `OVN_FOR_EACH_NORTHD_NO_HV` | Runs test with both single-threaded and 4-thread parallelization |
| `wait_row_count <table> <count> [conditions]` | Waits for SB table to have expected row count |
| `wait_column <value> <table> <column> [conditions]` | Waits for SB column to reach expected value |

**How `CHECK_NO_CHANGE_AFTER_RECOMPUTE` works:**
```
1. Dump SB logical_flow, port_binding, address_set, etc. to "before"
2. ovn-appctl inc-engine/recompute  (force full recompute)
3. ovn-nbctl --wait=sb sync
4. Dump same tables to "after"
5. diff before after  (must be empty)
```

### Running Tests

```bash
# Run specific test by number
make check TESTSUITEFLAGS="709"

# Run multiple tests
make check TESTSUITEFLAGS="624 659 700 709 725"

# Run tests matching a keyword
make check TESTSUITEFLAGS="-k incremental"

# Run with verbose output
make check TESTSUITEFLAGS="-v 709"
```

### Test Scenarios

#### 1. Static Route Add (Non-ECMP, Non-BFD)

**What to test:** Adding a single static route to an existing router is
handled incrementally (no northd/lflow recompute).

**Test number:** 709 (ovn -- LR static routes and policies trigger recompute)

```bash
# Setup
ovn-nbctl --wait=sb lr-add lr0
ovn-nbctl --wait=sb lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24

# Clear stats, add route
ovn-appctl -t ovn-northd inc-engine/clear-stats
ovn-nbctl --wait=sb lr-route-add lr0 192.168.0.0/16 10.0.0.254

# Verify incremental processing
check_engine_stats northd norecompute compute
check_engine_stats lflow norecompute compute

# Verify correctness
CHECK_NO_CHANGE_AFTER_RECOMPUTE
```

**Expected behavior:**
- northd detects `NORTHD_TRACKED_LR_ROUTES`
- `en_group_ecmp_route_northd_handler` rebuilds route_nodes for affected router
- `lflow_group_ecmp_route_handler` syncs only the new route's flows to SB
- No full recompute of northd or lflow

#### 2. Static Route Delete

**What to test:** Deleting a route removes only that route's SB flows.

```bash
# Setup (continuing from scenario 1)
ovn-appctl -t ovn-northd inc-engine/clear-stats
ovn-nbctl --wait=sb lr-route-del lr0 192.168.0.0/16

# Verify
check_engine_stats northd norecompute compute
check_engine_stats lflow norecompute compute
CHECK_NO_CHANGE_AFTER_RECOMPUTE
```

**Expected behavior:**
- Old route_node marked as deleted
- `lflow_group_ecmp_route_handler` unlinks and syncs (removes SB flows)
- New route_nodes created (without the deleted route)

#### 3. ECMP Route Group

**What to test:** Adding multiple routes with the same prefix creates an
ECMP group, and modifications to the group are handled incrementally.

```bash
# Setup
ovn-nbctl --wait=sb lr-add lr0
ovn-nbctl --wait=sb lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24

# Add two routes with same prefix -> ECMP
ovn-nbctl --wait=sb lr-route-add lr0 192.168.0.0/16 10.0.0.1
ovn-nbctl --wait=sb --ecmp lr-route-add lr0 192.168.0.0/16 10.0.0.2

# Verify ECMP flows exist
ovn-sbctl dump-flows lr0 | grep "lr_in_ip_routing_ecmp"

# Add third ECMP member incrementally
ovn-appctl -t ovn-northd inc-engine/clear-stats
ovn-nbctl --wait=sb --ecmp lr-route-add lr0 192.168.0.0/16 10.0.0.3

check_engine_stats northd norecompute compute
check_engine_stats lflow norecompute compute
CHECK_NO_CHANGE_AFTER_RECOMPUTE
```

**Expected behavior:**
- ECMP group rebuilt with 3 members
- Old group's route_node deleted, new group's route_node crupdated
- `lr_in_ip_routing` and `lr_in_ip_routing_ecmp` flows updated

#### 4. Named Route Tables

**What to test:** Routes with named route tables (`--route-table=rtb-1`) get
consistent `reg7` values between pre-flows and routing flows.

**Test number:** 659 (route tables -- flows)

```bash
# Setup
ovn-nbctl --wait=sb lr-add lr0
ovn-nbctl --wait=sb lrp-add lr0 lrp0 00:00:00:00:00:01 192.168.0.1/24
ovn-nbctl --wait=sb lrp-add lr0 lrp1 00:00:00:00:01:01 192.168.1.1/24
ovn-nbctl lrp-set-options lrp1 route_table=rtb-1

# Add route in named table
ovn-nbctl --route-table=rtb-1 --wait=sb lr-route-add lr0 0.0.0.0/0 192.168.1.10

# Verify reg7 consistency
ovn-sbctl dump-flows lr0 | grep "lr_in_ip_routing_pre"
# Should show: inport == "lrp1" -> reg7 = 1
ovn-sbctl dump-flows lr0 | grep "lr_in_ip_routing.*reg7"
# Should show: reg7 == 1 for rtb-1 routes
```

**Expected behavior:**
- Pre-flow assigns `reg7 = 1` to `lrp1` (rtb-1)
- Route flow uses `reg7 == 1` for rtb-1 routes
- IDs are consistent because `group_ecmp_route()` pre-populates simap from LRPs

#### 5. BFD Route (Fallback to Recompute)

**What to test:** Routes with BFD references trigger a full recompute
to ensure `build_bfd_table()` and `bfd_cleanup_connections()` run.

**Test number:** 624 (check BFD config propagation to SBDB)

```bash
# Setup
ovn-nbctl --wait=sb lr-add r0
ovn-nbctl --wait=sb lrp-add r0 r0-sw1 00:00:00:00:00:01 192.168.1.1/24

# Create BFD entry
uuid=$(ovn-nbctl create bfd logical_port=r0-sw1 dst_ip=192.168.1.2 \
    status=down min_tx=250 min_rx=250 detect_mult=10)

# BFD starts as admin_down (no route references it)
wait_row_count bfd 1 logical_port=r0-sw1 status=admin_down

# Add route with BFD -> triggers recompute, BFD transitions to down
ovn-nbctl --bfd=$uuid lr-route-add r0 100.0.0.0/8 192.168.1.2
wait_column down bfd status logical_port=r0-sw1
```

**Expected behavior:**
- `northd_handle_lr_changes()` detects BFD route -> `goto fail`
- Full recompute: `northd_run()` -> `en_group_ecmp_route_run()` ->
  `en_lflow_run()` -> `build_bfd_table()` + `bfd_update_static_route_refs()`
- BFD status transitions: `admin_down` -> `down`
- Route is excluded from flows (BFD not `up`)

#### 6. Router Creation with Routes

**What to test:** Creating a new router (incrementally) that has routes
builds route_nodes and flows without duplication.

```bash
# Clear stats
ovn-appctl -t ovn-northd inc-engine/clear-stats

# Create router with route in one transaction
ovn-nbctl --wait=sb lr-add lr0 \
    -- lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24 \
    -- lr-route-add lr0 0.0.0.0/0 10.0.0.254

# Both NORTHD_TRACKED_LR_CREATED and NORTHD_TRACKED_LR_ROUTES fire
# The handler skips the od in LR_CREATED if already processed by LR_ROUTES
CHECK_NO_CHANGE_AFTER_RECOMPUTE
```

**Expected behavior:**
- northd sets both `NORTHD_TRACKED_LR_CREATED` and `NORTHD_TRACKED_LR_ROUTES`
- `en_group_ecmp_route_northd_handler` processes od in ROUTES block (else: new router)
- CREATED block skips it (`group_ecmp_datapath_lookup` returns existing entry)
- No duplicate route_nodes or SB flows

#### 7. Policy Change (Independent Path)

**What to test:** Policy changes go through `lflow_northd_handler` via
`policy_lflow_ref`, not through the group_ecmp_route node.

```bash
# Setup
ovn-nbctl --wait=sb lr-add lr0
ovn-nbctl --wait=sb lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24

ovn-appctl -t ovn-northd inc-engine/clear-stats
ovn-nbctl --wait=sb lr-policy-add lr0 100 "ip4.src == 10.0.0.0/24" allow

check_engine_stats northd norecompute compute
check_engine_stats lflow norecompute compute
CHECK_NO_CHANGE_AFTER_RECOMPUTE
```

**Expected behavior:**
- northd sets `NORTHD_TRACKED_LR_POLICIES`
- `lflow_northd_handler` rebuilds `od->policy_lflow_ref`
- `group_ecmp_route` is not involved (no tracked data)

#### 8. ECMP Route with Discard

**What to test:** An ECMP group that includes a discard route is handled correctly.

**Test number:** 700 (Static routes - ECMP with discard)

```bash
# Setup
ovn-nbctl --wait=sb lr-add lr0
ovn-nbctl --wait=sb lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24

# Create ECMP group with normal + discard routes
ovn-nbctl --wait=sb lr-route-add lr0 192.168.0.0/16 10.0.0.1
ovn-nbctl --wait=sb --ecmp lr-route-add lr0 192.168.0.0/16 discard

# Verify discard route generates correct flows
ovn-sbctl dump-flows lr0 | grep "192.168.0.0"
CHECK_NO_CHANGE_AFTER_RECOMPUTE
```

### Stress Test

The script `tests/stress-incremental.sh` runs a comprehensive battery of
incremental operations and verifies no recomputes occur:

```bash
# Prerequisites: NB/SB ovsdb-servers + northd running
export OVN_NB_DB=unix:/path/to/nb.sock
export OVN_SB_DB=unix:/path/to/sb.sock
./tests/stress-incremental.sh /path/to/northd.ctl
```

The stress test covers:
- Router creation/deletion
- LRP add/delete
- Static route add/delete
- Policy add/delete
- NAT add/delete
- Multiple routes on same router
- Cross-operation interactions

### Performance Measurement

Use `--print-wait-time` to measure northd processing time:

```bash
# Baseline: full recompute with 2000 routes
for i in $(seq 1 2000); do
    ovn-nbctl lr-route-add lr0 10.$((i/256)).$((i%256)).0/24 192.168.0.1
done
ovn-nbctl --wait=sb --print-wait-time sync

# Incremental: add 1 more route
ovn-nbctl --wait=sb --print-wait-time lr-route-add lr0 10.8.0.0/24 192.168.0.1
```

**Target:** 3x improvement for single route add with 2000 existing routes
(matching upstream results: 62ms -> 21ms).

### Debugging

```bash
# Show engine node stats
ovn-appctl -t ovn-northd inc-engine/show-stats northd
ovn-appctl -t ovn-northd inc-engine/show-stats lflow
ovn-appctl -t ovn-northd inc-engine/show-stats group_ecmp_route

# Force a full recompute
ovn-appctl -t ovn-northd inc-engine/recompute

# Clear stats before measuring
ovn-appctl -t ovn-northd inc-engine/clear-stats

# Check northd logs for crashes
grep -E "SIGSEGV|SIGABRT|assert" /path/to/ovn-northd.log

# Dump all route flows for a router
ovn-sbctl dump-flows <router-name> | grep "lr_in_ip_routing"
```

## Known Limitations

1. **BFD routes always trigger full recompute.** Incremental BFD handling
   requires a separate `en_bfd` node (like upstream) to handle BFD status
   transitions (admin_down → down) in the incremental path. Without it,
   `bfd_update_static_route_refs()` only runs in `en_lflow_run()`.

2. **Router deletion triggers full lflow recompute** (NORTHD_TRACKED_LR_DELETED
   returns false from the handler).

3. **LRP deletion falls back to recompute** because some per-port flows are
   generated under `od->route_lflow_ref`.

4. **Route modification** (changing nexthop/policy on existing route row) is
   handled incrementally only for non-BFD routes.

5. **Unchanged unique routes reuse lflow_refs** but ECMP groups always get
   new lflow_refs since membership may have changed. A future optimization
   could compare ECMP group membership to detect unchanged groups.

6. **IC route sync tests** (923, 924, 927, 928, 933, 934) may fail due to
   `build_arp_request_flows_for_lrouter` being called from the incremental
   handler with per-route lflow_ref while the full recompute uses
   `od->lflow_ref`. This needs investigation.
