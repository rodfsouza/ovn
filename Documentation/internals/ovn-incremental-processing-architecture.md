# OVN Incremental Processing Architecture

## Overview

This document describes the incremental processing changes in OVN northd
that avoid full recompute of logical flows when router configuration changes.

Binary transport optimization (Phase A) is documented in the OVS repo at
`Documentation/internals/ovsdb-binary-transport-architecture.md`.

Key optimization: per-datapath `lflow_ref` system enabling targeted flow
rebuild for router creation, static route changes, and policy changes
without rebuilding all flows in the deployment.

## 1. Binary Transport Direct Path (Phase A)

### Problem

The binary transport protocol sends OVSDB data in a compact binary wire format,
but the client-side IDL was converting it back to JSON before parsing:

```
BEFORE (wasteful round-trip):

  Server                    Client (IDL)
    |                           |
    |--- ROW_BATCH (binary) --->|
    |                           |-- ovsdb_binary_deserialize_datum() --> datum
    |                           |-- ovsdb_datum_to_json()            --> JSON  (!)
    |                           |-- ovsdb_datum_from_json()          --> datum (!)
    |                           |-- write to row->old_datum
```

### Solution

New event type `OVSDB_CS_EVENT_TYPE_BINARY_UPDATE` carries pre-deserialized
`ovsdb_datum` values directly from binary wire to IDL row storage:

```
AFTER (direct path):

  Server                    Client (IDL)
    |                           |
    |--- ROW_BATCH (binary) --->|
    |                           |-- ovsdb_binary_deserialize_datum() --> datum
    |                           |-- ovsdb_datum_clone()              --> row->old_datum
    |                           |   (no JSON intermediate)
```

### Data Flow Diagram

```
                    Binary Wire
                        |
                        v
            +-------------------------+
            | ovsdb_cs_process_       |
            | binary_row_batch()      |
            | (ovsdb-cs.c)           |
            +-------------------------+
                        |
            Produces BINARY_UPDATE event
            with ovsdb_cs_binary_db_update
                        |
                        v
            +-------------------------+
            | ovsdb_idl_run()         |
            | (ovsdb-idl.c)          |
            +-------------------------+
                        |
                        v
            +-------------------------+
            | ovsdb_idl_process_      |
            | binary_update()         |
            +-------------------------+
                    |       |
            +-------+       +--------+
            v                        v
  +-------------------+    +-------------------+
  | binary_insert_row |    | binary_modify_row |
  | - init defaults   |    | - remove indexes  |
  | - binary_row_     |    | - unparse         |
  |   change()        |    | - binary_row_     |
  | - parse           |    |   change()        |
  | - add indexes     |    | - parse           |
  +-------------------+    | - add indexes     |
                           +-------------------+
                                    |
                                    v
                         +-------------------+
                         | binary_row_change |
                         | - clone datum     |
                         | - compare + swap  |
                         | - track changes   |
                         |   (seqno, bitmap, |
                         |    track_list)    |
                         +-------------------+
```

### Key Structures (ovsdb-cs.h)

```c
struct ovsdb_cs_binary_column {
    char *col_name;              // Column name from wire
    struct ovsdb_datum datum;    // Pre-deserialized value
    struct ovsdb_type col_type;  // Wire-format type (permissive)
};

struct ovsdb_cs_binary_row_update {
    struct uuid row_uuid;
    enum ovsdb_cs_row_update_type type;  // INSERT (Phase A), future: UPDATE/DELETE
    struct ovsdb_cs_binary_column *columns;
    size_t n_columns;
};

struct ovsdb_cs_binary_db_update {
    struct ovsdb_cs_binary_table_update *table_updates;
    size_t n;                    // Always 1 per ROW_BATCH frame
};
```

### Change Tracking Parity

`ovsdb_idl_binary_row_change()` replicates exact same tracking as the JSON path:
- `change_seqno` incremented on ALERT-mode column changes
- `row->updated` bitmap set for TRACK-mode columns
- `row->track_node` added to `table->track_list`
- `add_tracked_change_for_references()` called for recursive ref tracking

## 2. Incremental Router Creation (Phase C)

### Problem

When a router is created, northd's incremental processing engine (IPE) triggers
a **full recompute** of ALL datapaths, ports, load balancers, and logical flows:

```
BEFORE:

  NB: lr-add lr_new
       |
       v
  northd_handle_lr_changes()
       |
       +-- nbrec_logical_router_is_new() == true
       |
       +-- goto fail  -->  FULL RECOMPUTE
                           |
                           +-- destroy ALL datapaths
                           +-- rebuild ALL datapaths
                           +-- rebuild ALL ports
                           +-- rebuild ALL LB associations
                           +-- rebuild ALL logical flows
                           +-- sync ALL to SB
```

At scale (1,000+ routers), this rebuilds hundreds of thousands of flows.

### Solution

Standalone router creation (no ports, NATs, LBs, policies, or static routes)
is handled incrementally — only the new router's datapath is created:

```
AFTER:

  NB: lr-add lr_new -- lrp-add lr_new rp1 ...
       |
       v
  northd_handle_lr_changes()
       |
       +-- nbrec_logical_router_is_new() == true
       +-- reject only: LB, disabled, DGW ports
       |
       +-- INCREMENTAL:
           |
           +-- ovn_datapath_create()
           +-- allocate tunnel key
           +-- insert SB datapath_binding
           +-- init mcast info
           +-- add to lr_list, rebuild array index
           +-- for each LRP:
           |     +-- ovn_port_create()
           |     +-- extract_lrp_networks()
           |     +-- ovn_port_allocate_key()
           |     +-- sbrec_port_binding_insert()
           |     +-- ovn_port_update_sbrec()
           +-- track NATs/routes/policies if present
           +-- track as NORTHD_TRACKED_LR_CREATED
           |
           v
       en_lflow detects LR_CREATED
           |
           +-- build_lr_flows_for_datapath(od)
           |     generates base router flows
           |     using od->lflow_ref, route_lflow_ref,
           |     policy_lflow_ref
           +-- lflow_ref_sync_lflows() x3
           |     pushes new flows to SB
           |
           +-- NO full lflow recompute needed
```

### Engine Node DAG

```
                    NB Database Changes
                    |               |
          +---------+               +----------+
          v                                    v
  +----------------+                  +------------------+
  | en_nb_logical_ |                  | en_nb_logical_   |
  | router         |                  | router_port      |  <-- NEW (Phase C.2)
  +----------------+                  +------------------+
          |                                    |
          v                                    v
  +----------------+                  northd_nb_logical_
  | northd_nb_     |                  router_port_handler()
  | logical_router_|                  (returns false =
  | handler()      |                   safe recompute)
  +----------------+
          |
          v
  +------------------------------------------------+
  |              en_northd                          |
  |                                                 |
  |  northd_handle_lr_changes():                    |
  |    is_new + standalone → INCREMENTAL            |
  |    is_new + ports/NATs → goto fail (recompute)  |
  |    is_deleted → goto fail (recompute)           |
  |    modified + NAT/LB → existing handlers        |
  |                                                 |
  |  New inputs (NULL handlers = safe recompute):   |
  |    en_nb_logical_router_static_route            |
  |    en_nb_logical_router_policy                  |
  |    en_nb_nat                                    |
  +------------------------------------------------+
          |
          | NORTHD_TRACKED_LR_CREATED
          v
  +------------------------------------------------+
  |              en_lflow                           |
  |                                                 |
  |  lflow_northd_handler():                        |
  |    if LR_CREATED → return false (recompute)     |
  |    else → existing incremental handlers         |
  +------------------------------------------------+
          |
          v
  +------------------------------------------------+
  |  en_sync_to_sb, en_northd_output, etc.          |
  +------------------------------------------------+
```

### Incremental vs Recompute Behavior

| Operation | northd | lflow | Phase | Notes |
|-----------|--------|-------|-------|-------|
| Router add (no LB, no DGW) | **norecompute** | **norecompute** | C.1+C.4+C.6 | |
| Router + ports + NAT + routes + policies | **norecompute** | **norecompute** | C.6+C.5+C.7 | |
| Router + LB | recompute | recompute | C.11 reverted | Bitmap OOB — needs C.11.1 |
| LRP add on existing router (no DGW) | recompute | recompute | C.8 partial | Creates port+SB, returns false — needs C.8.1 |
| LRP delete on existing router (no DGW) | recompute | recompute | C.8 partial | Deletes port+SB, returns false — needs C.8.1 |
| Router delete (with/without ports) | **norecompute** | recompute | C.10 | Lflow returns false (future: keep OD alive for resync) |
| Static route change | **norecompute** | **norecompute** | C.5 | |
| Policy change | **norecompute** | **norecompute** | C.7 | |
| NAT modify (existing) | **norecompute** | recompute | Existing | |
| LB modify (existing) | **norecompute** | **norecompute** | Existing | |
| LRP modify (MAC/IP change) | recompute | recompute | Fallback | |
| DGW port changes | recompute | recompute | Fallback | |
| Multi-router group deletion | recompute | recompute | Fallback | |

### Tunnel Key Allocation Strategy

During full build, `build_datapaths()` maintains a `dp_tnlids` hmap of all used
tunnel keys. This is a local variable not stored in `northd_data`.

For incremental creation, we build a temporary set by scanning both `lr_datapaths`
and `ls_datapaths` (tunnel keys must be unique across routers AND switches):

```
  Scan lr_datapaths.datapaths  →  collect used keys
  Scan ls_datapaths.datapaths  →  collect used keys
           |
           v
  ovn_datapath_assign_requested_tnl_id()  →  try "requested-tnl-key" option
           |
           v (if no requested key)
  ovn_allocate_tnlid()  →  find first free key in [MIN_DP_KEY_LOCAL, max]
           |
           v
  ovn_destroy_tnlids()  →  free temporary set
```

### Fallback Strategy

Router creation falls back to full recompute only for:

```c
if (changed_lr->n_load_balancer > 0      // LB bitmap OOB (C.11 reverted)
    || changed_lr->n_load_balancer_group > 0  // Same issue
    || !lrouter_is_enabled(changed_lr)   // Disabled router
    || has_dgw_ports(changed_lr)) {      // DGW ports (complex cr- creation)
    goto fail;  // Full recompute — safe, correct
}
```

Ports, NATs, routes, and policies on new routers are handled incrementally.
LRP add/delete on existing routers creates/deletes the port and SB binding
incrementally but then returns false to trigger lflow recompute (per-LRP
lflow tracking is not yet implemented — planned in C.8.1).

## 3. Engine Nodes for Router Sub-Tables (Phase C.2)

Four new engine nodes detect changes to router sub-objects independently:

| Engine Node | NB Table | Handler | Behavior |
|-------------|----------|---------|----------|
| `en_nb_logical_router_port` | Logical_Router_Port | `northd_nb_logical_router_port_handler` | Returns false (recompute) |
| `en_nb_logical_router_static_route` | Logical_Router_Static_Route | NULL | Any change → recompute |
| `en_nb_logical_router_policy` | Logical_Router_Policy | NULL | Any change → recompute |
| `en_nb_nat` | NAT | NULL | Any change → recompute |

These nodes enable independent change detection. Previously, sub-object changes
were only detected when the parent `Logical_Router` row's column changed. With
dedicated nodes, the IDL tracks each sub-table independently via
`nbrec_*_table_track_get_first()`.

### Parent Lookup Strategy

Sub-objects have no back-reference column to their parent router in the schema.
Two lookup strategies are available:

1. **`lr_ports` hmap** (used): `ovn_port_find(&nd->lr_ports, lrp->name)` → `op->od`
2. **IDL `dst_arcs`** (not usable): `ovsdb_idl_arc` is private to `ovsdb-idl.c`

## 4. Files Modified

### OVS (`/ovs/`)

| File | Change |
|------|--------|
| `lib/ovsdb-cs.h` | `OVSDB_CS_EVENT_TYPE_BINARY_UPDATE`, binary update structs, `#include ovsdb-data.h/ovsdb-types.h` |
| `lib/ovsdb-cs.c` | Rewritten `ovsdb_cs_process_binary_row_batch()`, `ovsdb_cs_binary_db_update_destroy()`, event cleanup |
| `lib/ovsdb-idl.c` | `ovsdb_idl_binary_row_change/insert_row/modify_row/process_binary_row_update/process_binary_update`, forward declarations |
| `ovsdb/relay.c` | Handle new event type in switch |

### OVN (`/ovn/`)

| File | Change |
|------|--------|
| `northd/northd.h` | `NORTHD_TRACKED_LR_*` enums (CREATED, DELETED, ROUTES, POLICIES). Tracked data hmapx fields. Per-datapath `lflow_ref`, `route_lflow_ref`, `policy_lflow_ref`. `northd_handle_lrp_changes()` signature with txn + LRP table. |
| `northd/northd.c` | `northd_handle_lr_changes()`: datapath materialization + inline port creation (C.6) + deletion with port cleanup (C.10) + LB association (C.11) + route/policy tracking. `northd_handle_lrp_changes()`: LRP add/delete on existing routers with parent lookup, port creation, SB binding, peer disconnect (C.8). `lr_changes_can_be_handled()`: allows COL_STATIC_ROUTES/POLICIES, removed LRP/route/policy seqno rejection. Per-datapath lflow_ref threaded through 16 flow builders. Public wrappers: `build_lr_flows_for_datapath()`, `build_lr_route_flows_for_datapath()`, `build_lr_policy_flows_for_datapath()`. Change detection: `is_lr_static_routes_changed()`, `is_lr_policies_changed()`. |
| `northd/en-northd.h` | `northd_nb_logical_router_port_handler` declaration |
| `northd/en-northd.c` | LR handler passes txn. LRP handler gets LRP table via `EN_OVSDB_GET`, passes txn + table to `northd_handle_lrp_changes()`. |
| `northd/en-lflow.c` | `lflow_northd_handler()`: handles `LR_CREATED` (build + sync flows), `LR_ROUTES` (unlink + rebuild route flows), `LR_POLICIES` (unlink + rebuild policy flows), `LR_DELETED` (falls back to lflow recompute) |
| `northd/inc-proc-northd.c` | Four new NB_NODE entries (LRP, static_route, policy, NAT), DAG wiring with handlers |
| `tests/ovn-northd.at` | 7 new tests + updated existing tests for incremental expectations |
| `tests/perf-northd.at` | Binary transport performance test |
| `Documentation/automake.mk` | New doc files in distribution |

## 5. How Incremental Processing Works (Deep Dive)

### 5.1 The Change Detection → Tracking → Rebuild → Sync Pipeline

Every incremental update follows a four-stage pipeline:

```
Stage 1: DETECTION
  IDL receives NB database update
      ↓
  ovsdb_idl_run() populates track lists
      ↓
  Engine input nodes check: _table_track_get_first()
      ↓
  If changes detected → set node state to EN_UPDATED

Stage 2: TRACKING (in engine handler)
  northd_handle_lr_changes() iterates tracked rows:
      ↓
  For each changed Logical_Router:
    ├─ is_new? → create ovn_datapath → hmapx_add(trk_created_lrs)
    ├─ is_deleted? → goto fail (recompute)
    ├─ NAT changed? → hmapx_add(trk_nat_lrs)
    ├─ routes changed? → hmapx_add(lr_with_changed_routes)
    └─ policies changed? → hmapx_add(lr_with_changed_policies)
      ↓
  Set type flags: NORTHD_TRACKED_LR_CREATED | LR_ROUTES | LR_POLICIES
      ↓
  engine_set_node_state(node, EN_UPDATED)

Stage 3: FLOW REBUILD (in downstream handler)
  lflow_northd_handler() reads tracked data:
      ↓
  For NORTHD_TRACKED_LR_CREATED:
    build_lr_flows_for_datapath(od, ...) → populates od->lflow_ref
      ↓
  For NORTHD_TRACKED_LR_ROUTES:
    lflow_ref_unlink_lflows(od->route_lflow_ref) → marks old flows
    build_lr_route_flows_for_datapath(od, ...) → populates new flows
      ↓
  For NORTHD_TRACKED_LR_POLICIES:
    lflow_ref_unlink_lflows(od->policy_lflow_ref) → marks old flows
    build_lr_policy_flows_for_datapath(od, ...) → populates new flows

Stage 4: SB SYNC
  lflow_ref_sync_lflows(od->lflow_ref, ...)
      ↓
  For each lflow_ref_node in the ref:
    ├─ linked=true → sync_lflow_to_sb() (insert/update SB row)
    └─ linked=false → delete SB row (if no other refs)
      ↓
  Transaction committed by ovsdb_idl_loop_commit_and_wait()
```

### 5.2 The lflow_ref Reference Counting System

Each entity (port, LB, datapath) owns an `lflow_ref` that tracks which
logical flows belong to it:

```
                    Global lflow_table
                    (hmap of ovn_lflow)
                           │
                           │ hash(stage, priority, match, actions)
                           ▼
                    ┌──────────────┐
                    │  ovn_lflow   │──── referenced_by (list)
                    │  stage=...   │          │
                    │  priority=.. │          ▼
                    │  match=...   │    ┌──────────────┐
                    │  actions=... │    │ lflow_ref_node│───── in lflow_ref A
                    │  dpg_bitmap  │    │  dp_index=3   │     (od->lflow_ref)
                    └──────────────┘    │  linked=true  │
                                       └──────────────┘
                                             │
                                             ▼
                                       ┌──────────────┐
                                       │ lflow_ref_node│───── in lflow_ref B
                                       │  dp_index=7   │     (other_od->lflow_ref)
                                       │  linked=true  │
                                       └──────────────┘

  Multiple datapaths can share the same lflow (e.g., default drop rules).
  Each lflow_ref_node tracks which datapath(s) this ref covers.
  dp_refcnts_map counts how many refs exist per datapath.
  An lflow is deleted from SB only when ALL refs are unlinked.
```

**Key operations:**
- `lflow_table_add_lflow(... lflow_ref)` — creates lflow_ref_node if lflow_ref != NULL
- `lflow_ref_unlink_lflows(ref)` — sets `linked=false` on all nodes (does NOT delete)
- `lflow_ref_sync_lflows(ref, ...)` — syncs to SB: linked=true → insert/update, linked=false → delete

### 5.3 Database Representation

**NB side (input):**
```
Logical_Router (nbrec_logical_router)
  ├── ports[] → Logical_Router_Port (strong ref, separate table)
  ├── nat[] → NAT (strong ref, separate table)
  ├── static_routes[] → Logical_Router_Static_Route (strong ref)
  ├── policies[] → Logical_Router_Policy (strong ref)
  ├── load_balancer[] → Load_Balancer (weak ref)
  └── options, name, enabled, copp, ...
```

**SB side (output):**
```
Datapath_Binding (sbrec_datapath_binding)
  ├── tunnel_key (uint32)
  ├── external_ids: {logical-router: <UUID>, name: <name>}
  └── (created by northd_handle_lr_changes for new routers)

Port_Binding (sbrec_port_binding)
  ├── logical_port (string)
  ├── datapath (ref to Datapath_Binding)
  ├── tunnel_key (uint32)
  ├── type (patch, chassisredirect, l3gateway, ...)
  ├── options: {peer, chassis-redirect-port, ...}
  └── (created by ovn_port_update_sbrec for new LRPs)

Logical_Flow (sbrec_logical_flow)
  ├── logical_datapath (ref to Datapath_Binding)
  ├── pipeline (ingress/egress)
  ├── table_id (stage number)
  ├── priority
  ├── match, actions
  └── (created by lflow_ref_sync_lflows)
```

### 5.4 The Engine DAG and Change Propagation

```
NB Database Tables
  │
  ├── en_nb_logical_router ──────────────────────────────────┐
  │     Detects: new/deleted/modified router rows             │
  │     Handler: northd_nb_logical_router_handler()           │
  │                                                           │
  ├── en_nb_logical_router_port ─────────────────────────┐   │
  │     Detects: new/modified/deleted LRP rows            │   │
  │     Handler: northd_nb_logical_router_port_handler()  │   │
  │     (currently returns false = recompute)              │   │
  │                                                       │   │
  ├── en_nb_logical_router_static_route ─────────────┐   │   │
  │     Detects: static route changes                 │   │   │
  │     Handler: NULL (any change = recompute)        │   │   │
  │                                                   │   │   │
  ├── en_nb_logical_router_policy ───────────────┐   │   │   │
  │     Detects: policy changes                   │   │   │   │
  │     Handler: NULL (any change = recompute)    │   │   │   │
  │                                               │   │   │   │
  ├── en_nb_nat ─────────────────────────────┐   │   │   │   │
  │     Detects: NAT row changes              │   │   │   │   │
  │     Handler: NULL (any change = recompute)│   │   │   │   │
  │                                           │   │   │   │   │
  └── (other NB/SB inputs...)                 │   │   │   │   │
                                              ▼   ▼   ▼   ▼   ▼
                                     ┌────────────────────────────┐
                                     │        en_northd           │
                                     │  northd_handle_lr_changes()│
                                     │  northd_handle_lrp_changes()
                                     │                            │
                                     │  Produces tracked data:    │
                                     │  - trk_created_lrs         │
                                     │  - trk_nat_lrs             │
                                     │  - lr_with_changed_routes  │
                                     │  - lr_with_changed_policies│
                                     │  - trk_lsps (switch ports) │
                                     │  - trk_lbs (load balancers)│
                                     └────────────┬───────────────┘
                                                  │ EN_UPDATED
                                                  ▼
                                     ┌────────────────────────────┐
                                     │        en_lflow            │
                                     │  lflow_northd_handler()    │
                                     │                            │
                                     │  Consumes tracked data:    │
                                     │  - LR_CREATED → build flows│
                                     │  - LR_ROUTES → rebuild rt  │
                                     │  - LR_POLICIES → rebuild pl│
                                     │  - PORTS → rebuild port fl │
                                     │  - LBS → rebuild lb flows  │
                                     │  - LR_DELETED → return false
                                     └────────────┬───────────────┘
                                                  │ EN_UPDATED
                                                  ▼
                                     ┌────────────────────────────┐
                                     │  en_sync_to_sb, en_northd_ │
                                     │  output, en_lr_nat,        │
                                     │  en_lr_stateful, ...       │
                                     └────────────────────────────┘

  When a handler returns false, the engine falls back to the node's
  recompute function (en_northd_run / en_lflow_run), which rebuilds
  everything from scratch.
```

### 5.5 Testing the Incremental Path

Tests use three mechanisms to verify incremental behavior:

**1. Engine statistics (`check_engine_stats`)**
```bash
# Clear counters before the operation
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats

# Perform the operation
check ovn-nbctl --wait=sb lr-add lr1

# Check engine node behavior:
# "norecompute" = handler succeeded (incremental)
# "recompute"   = handler failed or NULL handler (full rebuild)
# "compute"     = handler was called
# "nocompute"   = handler was not called (no changes detected)
check_engine_stats northd norecompute compute
check_engine_stats lflow norecompute compute
```

**2. SB state correctness (`CHECK_NO_CHANGE_AFTER_RECOMPUTE`)**
```bash
# Dumps SB tables before and after forcing a full recompute.
# If incremental produced correct state, diff is empty.
CHECK_NO_CHANGE_AFTER_RECOMPUTE
```

This is the golden test: it proves the incremental path produces
the exact same SB state as a full recompute.

**3. Row count verification (`check_row_count`)**
```bash
# Verify expected number of SB records
check_row_count Datapath_Binding 3
wait_row_count Port_Binding 1 logical_port=rp1
```

### 5.6 Per-Datapath lflow_ref Threading

The three `lflow_ref` fields on `struct ovn_datapath` partition flows
by concern, enabling targeted rebuilds:

```
struct ovn_datapath {
    lflow_ref ──────────────── General router flows (12 builders)
    │                          adm_ctrl, neigh_learning, ND_RA,
    │                          mcast_lookup, arp_resolve, check_pkt_len,
    │                          gateway_redirect, arp_request, network_id,
    │                          misc_local_traffic_drop, nat_defrag_lb,
    │                          lb_affinity, default_drop
    │
    route_lflow_ref ─────────── Route-specific flows (2 builders)
    │                          ip_routing_pre, static_route
    │
    policy_lflow_ref ────────── Policy-specific flows (1 builder)
                               ingress_policy
};

When a route changes:
  Only route_lflow_ref is unlinked + rebuilt + synced.
  lflow_ref and policy_lflow_ref are untouched.
  → O(routes_on_this_router) instead of O(all_flows)
```

## 6. Future Work

### C.11.1: LB Bitmap Resize (Next)

**Problem**: `ovn_lb_datapaths.nb_lr_map` allocated with old router count.
When a new router is created incrementally, `od->index` can exceed the bitmap
boundary, causing OOB write in `bitmap_set1()`.

**Fix**: Add `nb_lr_map_n_bits` field to `struct ovn_lb_datapaths`. In
`ovn_lb_datapaths_add_lr()`, check `ods[i]->index >= lb_dps->nb_lr_map_n_bits`
and `xrealloc` + `memset` the bitmap to the new size. Same pattern for
`ovn_lb_group_datapaths` (add `max_lr` field, realloc `lr[]` array).

Then restore the LB association block in `northd_handle_lr_changes()` and
remove the `n_load_balancer > 0` fallback.

### C.8.1: Per-LRP Incremental Lflow Tracking (Next)

**Problem**: LRP create/delete handler creates/deletes the port and SB
port_binding correctly but returns false to trigger full lflow recompute,
because per-LRP lflow tracking was not implemented.

**Fix**: Follow the `tracked_ovn_ports` (LSP) pattern:
1. Add `tracked_lr_ports` struct with `created`/`deleted` hmapx fields
2. Add `NORTHD_TRACKED_LR_PORTS` enum value (1 << 9)
3. LRP create: resolve peer LSP via `ls_ports` scan, add to `trk_lrps.created`
4. LRP delete: keep port alive (don't destroy), add to `trk_lrps.deleted`
5. Lflow handler: for created, call `build_lswitch_and_lrouter_iterate_by_lrp()`
   + `build_lbnat_lflows_iterate_by_lrp()` + sync both `lflow_ref` and
   `stateful_lflow_ref`. For deleted, call `lflow_ref_resync_flows()` on both refs.
6. Port destroyed in `destroy_northd_data_tracked_changes()` (same as LSP pattern)

### C.9: DGW Port Support (Tier 1 — New Routers Only)

Create cr- port on new routers via `ovn_chassis_redirect_name()` +
`ovn_port_create()`. Set `crp->primary_port = op; op->cr_port = crp`.
Populate `od->l3dgw_ports[]`. Insert SB port_binding for cr- port.
DGW on existing routers remains fallback due to complexity of
HA chassis group handling and 40+ downstream flow generation sites.

### Router Deletion Lflow

Currently `lflow_northd_handler` returns false for `NORTHD_TRACKED_LR_DELETED`.
To make this incremental, need to keep the deleted `ovn_datapath` alive (in
`trk_deleted_lrs`) until lflow handler calls `lflow_ref_resync_flows()` on
all 3 per-datapath refs. Same deferred-destruction pattern as LRP/LSP.

### LRP Modify on Existing Routers

MAC/IP changes affect many flow builders (admission control, routing,
ARP resolution, etc.). Requires unlink+rebuild of all per-LRP flows
and possibly per-datapath flows. Complex due to peer port flow
regeneration requirements.

### Multi-Router Group Deletion

When `lr_group->n_router_dps > 1`, deleting a router requires
rebuilding the group recursively via `build_lrouter_groups__()` for
all remaining members. Falls back to full recompute.

### Binary UPDATE_BATCH (Phase D)

Extend server to send incremental updates as binary frames.
Add XOR support to `ovsdb_idl_binary_row_change()`.

### Batch Accumulation (Phase E)

Accumulate multiple binary frames before triggering `engine_run()`.
