# OVN Incremental Processing Architecture

## 1. Overview

This document explains how OVN's northd daemon processes configuration changes
incrementally, avoiding expensive full recomputation of logical flows. It covers
the core infrastructure (engine, DAG, IDL change tracking, lflow_ref system) and
the specific optimizations for router operations.

Binary transport optimization (Phase A) is documented separately in the OVS repo
at `Documentation/internals/ovsdb-binary-transport-architecture.md`.

## 2. OVN Architecture Context

OVN translates high-level network configuration into OpenFlow rules:

```
  Northbound DB (NB)           ovn-northd              Southbound DB (SB)
  ┌──────────────────┐    ┌─────────────────┐    ┌──────────────────────┐
  │ Logical_Switch   │    │                 │    │ Datapath_Binding     │
  │ Logical_Router   │───>│  Reads NB       │───>│ Port_Binding         │
  │ ACL, NAT, LB     │    │  Computes flows │    │ Logical_Flow         │
  │ LRP, LSP, Policy │    │  Writes SB      │    │ Multicast_Group      │
  └──────────────────┘    └─────────────────┘    └──────────────────────┘
                                                          │
                                                          v
                                                  ovn-controller
                                                  (on each hypervisor)
                                                          │
                                                          v
                                                    OpenFlow rules
                                                    (in OVS bridge)
```

**northd** is the centralized daemon that reads the NB database (network intent)
and writes the SB database (computed forwarding state). It runs in a loop:

1. Fetch NB/SB changes via OVSDB IDL
2. Process changes through the engine
3. Commit SB writes

At scale (1,000+ routers, 10,000+ ports), a naive approach rebuilds ALL computed
state on every change. A single router addition would regenerate hundreds of
thousands of logical flows across all datapaths. The incremental processing
engine exists to avoid this.

## 3. The Incremental Processing Engine

### Why It Exists

Without incremental processing, northd's main loop does this on every change:

```
Full recompute (expensive):
  destroy ALL datapaths, ports, LB mappings
  rebuild ALL datapaths from NB
  rebuild ALL ports from NB
  rebuild ALL LB associations
  generate ALL logical flows (100K+ at scale)
  diff ALL flows against SB
  write changes to SB
```

This is O(total_state) per change. With incremental processing, adding a single
router only creates that router's datapath and generates its ~50-100 flows:

```
Incremental (cheap):
  create ONE new datapath
  create its ports
  generate ~50-100 flows for the new router
  insert those flows into SB
  done — existing routers untouched
```

This is O(changed_state) per change.

### What It Is

The engine is a DAG (Directed Acyclic Graph) of processing nodes. Each node:
- Holds some computed state (e.g., the set of all datapaths, or all logical flows)
- Declares which other nodes it depends on (its inputs)
- Has a **handler** for each input that tries to apply changes incrementally
- Has a **recompute function** (`run()`) that rebuilds everything from scratch

On each iteration, the engine evaluates nodes in topological order. For each node,
it checks whether any input changed. If so, it calls the handler. If the handler
succeeds (returns `true`), the change was applied incrementally. If it fails
(returns `false`), the engine falls back to the node's recompute function.

### The Key Insight

A handler returning `true` means: "I updated this node's data to reflect the
input change, without touching anything else."

A handler returning `false` means: "This change is too complex for me to handle
surgically. Please rebuild everything from scratch."

This gives developers a safe escape hatch — any change that's too complex to
handle incrementally simply returns `false`, and the engine does the right thing.
New incremental handlers can be added one operation at a time without risk.

## 4. The Engine DAG

### Nodes

The engine has three kinds of nodes:

**Leaf nodes** (NB_NODE, SB_NODE) — represent OVSDB tables. They have no inputs.
Their `run()` function checks the IDL for tracked row changes. If any rows
changed, the node state becomes `EN_UPDATED`.

**Computed nodes** (ENGINE_NODE) — hold derived state computed from inputs. They
have a `run()` recompute function and per-input handlers. Examples: `en_northd`
(datapaths, ports), `en_lflow` (logical flows), `en_lb_data` (load balancers).

**Output nodes** — the root of the DAG. `en_northd_output` is the final node
whose completion means all SB writes are done.

### Wiring Inputs

Dependencies and handlers are wired in `inc-proc-northd.c`:

```c
/* en_northd depends on NB logical_router changes.
 * When the table changes, call northd_nb_logical_router_handler(). */
engine_add_input(&en_northd, &en_nb_logical_router,
                 northd_nb_logical_router_handler);

/* en_northd depends on NB mirror changes.
 * NULL handler means ANY change triggers full recompute. */
engine_add_input(&en_northd, &en_nb_mirror, NULL);

/* en_lflow depends on en_northd.
 * When northd data changes, call lflow_northd_handler(). */
engine_add_input(&en_lflow, &en_northd, lflow_northd_handler);
```

### Node States

Each node has a state that drives the evaluation:

| State | Meaning |
|-------|---------|
| `EN_STALE` | Data not yet computed this iteration (initial state) |
| `EN_UPDATED` | Data was recomputed or incrementally updated |
| `EN_UNCHANGED` | Inputs were checked but nothing changed |
| `EN_ABORTED` | Recompute needed but not allowed (engine stops) |

### Evaluation Algorithm

`engine_run()` processes nodes in topological order (leaves first, root last):

```
for each node in topological order:
    if node has no inputs (leaf):
        node->run()          # check IDL for tracked changes
        continue

    if any input has state == EN_UPDATED:
        if input has no handler (NULL):
            RECOMPUTE this node     # full rebuild via run()
        else:
            call handler()
            if handler returns false:
                RECOMPUTE this node
            else:
                node stays as-is (handler updated it)
    else:
        node state = EN_UNCHANGED   # nothing to do
```

When recompute happens, the engine calls `clear_tracked_data()` first (to discard
any partial tracking from handlers that ran before the failure), then calls
`run()` for a full rebuild.

### The Northd DAG

```
  NB Tables                          SB Tables
  ─────────                          ─────────
  nb_logical_switch ──┐              sb_port_binding ──┐
  nb_logical_router ──┤              sb_chassis ───────┤
  nb_logical_router_port ─┤          sb_datapath_binding ─┤
  nb_logical_router_      │          ...                │
    static_route ─────────┤                             │
  nb_logical_router_      │                             │
    policy ───────────────┤                             │
  nb_nat ─────────────────┤                             │
  nb_load_balancer ──┐    │                             │
  nb_load_balancer_  │    │                             │
    group ───────────┤    │                             │
  nb_acl ────────────┤    │                             │
  nb_mirror ─────────┤    │                             │
  ...                │    │                             │
                     v    v                             v
                  ┌──────────┐                   ┌──────────────┐
                  │ lb_data  │                   │ global_config│
                  └────┬─────┘                   └──────┬───────┘
                       │                                │
                       v                                v
                  ┌──────────────────────────────────────────┐
                  │              en_northd                    │
                  │  Produces: datapaths, ports, LB maps     │
                  │  Tracked: trk_created_lrs, trk_lrps,     │
                  │           trk_lsps, trk_lbs, ...         │
                  └───────────────────┬──────────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              v                       v                       v
        ┌───────────┐          ┌────────────┐          ┌────────────┐
        │ lr_nat    │          │ lr_stateful│          │ ls_stateful│
        └─────┬─────┘          └──────┬─────┘          └──────┬─────┘
              │                       │                       │
              └───────────────────────┼───────────────────────┘
                                      v
                  ┌──────────────────────────────────────────┐
                  │              en_lflow                     │
                  │  Consumes tracked data from northd       │
                  │  Generates/syncs Logical_Flow rows in SB │
                  └───────────────────┬──────────────────────┘
                                      │
                                      v
                  ┌──────────────────────────────────────────┐
                  │         en_sync_to_sb                     │
                  │         en_northd_output                  │
                  └──────────────────────────────────────────┘
```

## 5. OVSDB IDL and Change Tracking

### How Database Changes Become Engine Inputs

The OVSDB IDL (Interface Definition Language) layer provides automatic change
tracking. northd enables it at startup:

```c
ovsdb_idl_track_add_all(ovnnb_idl_loop.idl);
ovsdb_idl_track_add_all(ovnsb_idl_loop.idl);
```

This tells the IDL to record which rows were inserted, deleted, or modified
between iterations. The engine's leaf nodes (NB_NODE, SB_NODE) check these
tracked changes in their `run()` function. If any tracked rows exist, the
node state becomes `EN_UPDATED`, which triggers handlers in downstream nodes.

### Iterating Tracked Changes

Handlers iterate only the rows that changed, not all rows:

```c
/* Only iterates rows that were inserted, deleted, or modified */
NBREC_LOGICAL_ROUTER_TABLE_FOR_EACH_TRACKED(changed_lr,
                                              ni->nbrec_logical_router_table) {
    if (nbrec_logical_router_is_new(changed_lr)) {
        /* Row was inserted this iteration */
    } else if (nbrec_logical_router_is_deleted(changed_lr)) {
        /* Row was deleted this iteration */
    } else {
        /* Row was modified — check which columns changed */
        if (nbrec_logical_router_is_updated(changed_lr,
                NBREC_LOGICAL_ROUTER_COL_STATIC_ROUTES)) {
            /* Static routes column changed */
        }
    }
}
```

This fine-grained change detection is what makes incremental processing possible.
A handler can check exactly what changed and decide whether it can handle the
change or needs to fall back to recompute.

### Accessing Tables in Handlers

Engine handlers access OVSDB tables via `EN_OVSDB_GET`:

```c
const struct nbrec_logical_router_port_table *lrp_table =
    EN_OVSDB_GET(engine_get_input("nb_logical_router_port", node));
```

This extracts the table pointer from the leaf node's data, giving the handler
access to both the full table and its tracked changes.

## 6. The lflow_ref System

### Problem: Shared Flows and Datapath Groups

Logical flows can be shared across multiple datapaths. For example, a default
drop rule might apply to all 1,000 routers. In the SB database, this is stored
once with a "datapath group" bitmap indicating which datapaths use it.

When a single router is deleted, we need to remove it from the datapath group
bitmap — but NOT delete the flow row, because 999 other routers still use it.
We need reference counting.

### How lflow_ref Works

Each entity (datapath, port, load balancer) owns an `lflow_ref` that tracks
which logical flows it generated:

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
                    │  dpg_bitmap  │    │  dp_index=3   │     (router3->lflow_ref)
                    └──────────────┘    │  linked=true  │
                                       └──────────────┘
                                             │
                                             ▼
                                       ┌──────────────┐
                                       │ lflow_ref_node│───── in lflow_ref B
                                       │  dp_index=7   │     (router7->lflow_ref)
                                       │  linked=true  │
                                       └──────────────┘

  Multiple datapaths can share the same lflow (e.g., default drop rules).
  Each lflow_ref_node tracks which datapath(s) this ref covers.
  dp_refcnts_map counts how many refs exist per datapath.
  An lflow is deleted from SB only when ALL refs are unlinked.
```

### Key Operations

| Operation | What it does | When used |
|-----------|-------------|-----------|
| `lflow_table_add_lflow(... lflow_ref)` | Creates lflow + links to ref | Flow generation |
| `lflow_ref_unlink_lflows(ref)` | Sets `linked=false` on all nodes | Before rebuilding flows |
| `lflow_ref_sync_lflows(ref)` | Syncs to SB: linked→insert, unlinked→delete | After building new flows |
| `lflow_ref_resync_flows(ref)` | Unlink + sync (for deletions) | Deleting an entity |

The unlink/sync separation is important: unlinking marks flows for potential
deletion, but the actual SB row is only deleted during sync if no other ref
still links to it. This handles shared flows correctly.

## 7. Per-Datapath lflow_ref Partitioning

Each router datapath has three separate `lflow_ref` fields that partition flows
by concern:

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
```

When a static route changes on a router, only `route_lflow_ref` is unlinked
and rebuilt. The general flows and policy flows are untouched. This reduces
the work from O(all_flows_on_router) to O(route_flows_on_router).

Each router port also has two `lflow_ref` fields:

```
struct ovn_port {
    lflow_ref ──────────────── Per-port flows (10 builders)
    │                          adm_ctrl, neigh_learning, ip_routing,
    │                          ND_RA, arp_resolve, egress_delivery,
    │                          dhcpv6_reply, ipv6_input, ipv4_input,
    │                          icmp_packet_toobig
    │
    stateful_lflow_ref ──────── LB/NAT-related port flows
                               lrp_lflows_for_lbnats,
                               routable_flows_for_router_port
};
```

## 8. Incremental Router Processing

### The Complete Pipeline

When a router is created in the NB database, the following pipeline executes:

```
NB Database: "lr-add lr_new -- lrp-add lr_new rp1 ... -- lr-lb-add lr_new lb1"
                    │
                    ▼
    ┌───────────────────────────────────────────────────────────┐
    │  OVSDB IDL: ovsdb_idl_run()                              │
    │  Detects: Logical_Router inserted, LRP inserted,         │
    │           LR.load_balancer column updated                │
    │  Marks rows as tracked                                   │
    └───────────────────────┬───────────────────────────────────┘
                            │
                            ▼
    ┌───────────────────────────────────────────────────────────┐
    │  Engine Leaf: en_nb_logical_router                        │
    │  Checks: tracked rows exist? → YES → state = EN_UPDATED  │
    └───────────────────────┬───────────────────────────────────┘
                            │
                            ▼
    ┌───────────────────────────────────────────────────────────┐
    │  Handler: northd_nb_logical_router_handler()              │
    │           → northd_handle_lr_changes()                    │
    │                                                           │
    │  1. Reject if disabled or has DGW ports → goto fail       │
    │  2. Create ovn_datapath, allocate tunnel key              │
    │  3. Insert SB Datapath_Binding                            │
    │  4. Resize ALL existing LB bitmaps to new size            │
    │  5. For each LRP on the router:                           │
    │     - Create ovn_port, parse networks                     │
    │     - Allocate port tunnel key                            │
    │     - Insert SB Port_Binding                              │
    │  6. Associate LBs: ovn_lb_datapaths_add_lr() for each LB │
    │  7. Track NATs/routes/policies if present                 │
    │  8. Add to trk_created_lrs                                │
    │  9. Set NORTHD_TRACKED_LR_CREATED | LR_ROUTES | ...      │
    │                                                           │
    │  Return: true (handled incrementally)                     │
    └───────────────────────┬───────────────────────────────────┘
                            │ EN_UPDATED (tracked data populated)
                            ▼
    ┌───────────────────────────────────────────────────────────┐
    │  Handler: lflow_northd_handler()                          │
    │                                                           │
    │  Reads trk_created_lrs:                                   │
    │    For each new router:                                   │
    │      build_lr_flows_for_datapath(od)                      │
    │        → generates ~50-100 flows using od->lflow_ref,     │
    │          od->route_lflow_ref, od->policy_lflow_ref        │
    │      lflow_ref_sync_lflows() × 3                          │
    │        → inserts new Logical_Flow rows into SB            │
    │                                                           │
    │  Reads lr_with_changed_routes (if routes on new router):  │
    │    build_lr_route_flows_for_datapath(od)                  │
    │    lflow_ref_sync_lflows(od->route_lflow_ref)             │
    │                                                           │
    │  Return: true                                             │
    └───────────────────────┬───────────────────────────────────┘
                            │ EN_UPDATED
                            ▼
    ┌───────────────────────────────────────────────────────────┐
    │  SB Transaction: ovsdb_idl_loop_commit_and_wait()         │
    │  Writes: Datapath_Binding, Port_Binding, Logical_Flow     │
    │  Existing routers: completely untouched                   │
    └───────────────────────────────────────────────────────────┘
```

### LRP Add/Delete on Existing Routers

When a port is added to an existing router (`lrp-add lr0 rp_new ...`):

1. Engine leaf `en_nb_logical_router_port` detects the tracked LRP row
2. `northd_handle_lrp_changes()` is called:
   - Finds parent router by iterating `lr_datapaths` (O(routers × ports))
   - Creates `ovn_port`, parses networks, allocates tunnel key
   - Inserts SB `Port_Binding`
   - Resolves peer LSP by scanning `ls_ports` for `options:router-port` match
   - Sets bidirectional peer (`op->peer = lsp; lsp->peer = op`)
   - Marks peer LSP as updated (its flows reference the new peer)
   - Adds to `trk_lrps.created`, sets `NORTHD_TRACKED_LR_PORTS`
3. `lflow_handle_northd_lr_port_changes()` generates per-LRP flows:
   - `build_lswitch_and_lrouter_iterate_by_lrp(op)` → 10 flow builders
   - `build_lbnat_lflows_iterate_by_lrp(op)` → LB/NAT port flows
   - `lflow_ref_sync_lflows()` for both `op->lflow_ref` and `op->stateful_lflow_ref`

For deleted LRPs, the port is removed from operational maps but kept alive.
The lflow handler calls `lflow_ref_resync_flows()` which unlinks and syncs
(deleting flows from SB). The port is then destroyed during
`destroy_northd_data_tracked_changes()` at the end of the cycle.

### Tracked Data Flow Between Nodes

The `en_northd` node produces tracked data that downstream nodes consume:

```
northd_tracked_data {
    type: bitmask of NORTHD_TRACKED_*
    
    trk_created_lrs    — new router datapaths (ovn_datapath *)
    trk_deleted_lrs    — deleted router datapaths
    trk_lrps.created   — new router ports (ovn_port *)
    trk_lrps.deleted   — deleted router ports (kept alive for lflow cleanup)
    trk_lsps           — switch port changes (created/updated/deleted)
    trk_lbs            — load balancer changes
    trk_nat_lrs        — routers with NAT changes
    lr_with_changed_routes   — routers with static route changes
    lr_with_changed_policies — routers with policy changes
    ls_with_changed_lbs      — switches with LB changes
    ls_with_changed_acls     — switches with ACL changes
}
```

The lflow handler checks each flag and processes only the relevant tracked data.
This is how change information propagates through the DAG without requiring
full recomputation.

### Fallback Conditions

The incremental path falls back to full recompute for:

| Condition | Why |
|-----------|-----|
| Disabled router (`!lrouter_is_enabled`) | Rare edge case, not worth optimizing |
| DGW ports (`ha_chassis_group` or `gateway_chassis`) | Creates derived cr- port, affects `l3dgw_ports[]`, 40+ flow builders |
| LRP modify (MAC/IP change) | Affects many flow builders (admission, routing, ARP resolution) |
| Router deletion (lflow stage) | Datapath group membership cleanup is complex |
| Multi-router group deletion | `lr_group` requires recursive rebuild across all members |

Everything else is handled incrementally: router creation with ports, NATs,
routes, policies, and LBs; LRP add/delete on existing routers; static route
and policy changes; LB association changes.

## 9. Testing the Incremental Path

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

## 10. Incremental Behavior Matrix

| Operation | northd | lflow |
|-----------|--------|-------|
| Router add (any config, no DGW) | **norecompute** | **norecompute** |
| Router + ports + NAT + routes + policies + LB | **norecompute** | **norecompute** |
| LRP add on existing router (no DGW) | **norecompute** | **norecompute** |
| LRP delete on existing router (no DGW) | **norecompute** | **norecompute** |
| Router delete (with/without ports) | **norecompute** | recompute |
| Static route change | **norecompute** | **norecompute** |
| Policy change | **norecompute** | **norecompute** |
| NAT change | **norecompute** | recompute |
| LB change | **norecompute** | **norecompute** |
| LRP modify (MAC/IP change) | recompute | recompute |
| DGW port changes | recompute | recompute |
| Multi-router group deletion | recompute | recompute |
| Disabled router | recompute | recompute |

## 11. NB/SB Database Representation

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
Datapath_Binding
  ├── tunnel_key (uint32)
  ├── external_ids: {logical-router: <UUID>, name: <name>}

Port_Binding
  ├── logical_port (string), datapath (ref), tunnel_key (uint32)
  ├── type (patch, chassisredirect, l3gateway, ...)

Logical_Flow
  ├── logical_datapath (ref) or logical_dp_group (ref)
  ├── pipeline (ingress/egress), table_id, priority
  ├── match, actions
```

## 12. Future Work

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

## 13. Datapath Indexing and Bitmap System

### How dp_group Bitmaps Work

Each datapath has an integer index into `datapaths->array`. Bitmaps use
these indices as bit positions to represent sets of datapaths:

```
8 routers, indices 0-7:
  array:  [R0][R1][R2][R3][R4][R5][R6][R7]    n_array_alloc = 8

Flow "ip4.src == 10.0.0.0/8 → drop" applies to R0,R1,R2,R4,R6,R7:

  dpg_bitmap: [1][1][1][0][1][0][1][1]
               0   1   2  3   4  5   6  7

  → ONE SB Logical_Flow + Logical_DP_Group = {R0,R1,R2,R4,R6,R7}
  → Without dp_groups: 6 separate Logical_Flow rows
```

### Load Balancer Bitmaps (nb_lr_map)

```
LB "web-lb" associated with routers R1, R3, R5:

  nb_lr_map: [0][1][0][1][0][1][0][0]
              0   1  2   3  4   5  6  7

  BITMAP_FOR_EACH_1(index, ods_array_size(), nb_lr_map):
    → index=1 → array[1] = R1
    → index=3 → array[3] = R3
    → index=5 → array[5] = R5
```

### Incremental Creation (ods_append_datapath)

```
Before: 3 routers, n_array_alloc = 3
  [R0][R1][R2]

lr-add R3 → ods_append_datapath():
  n = ods_size() = 4 (R3 already in hmap)
  xrealloc(array, 4 * sizeof(ptr))
  R3->index = 3, array[3] = R3, n_array_alloc = 4

After: [R0][R1][R2][R3]   n_array_alloc = 4
  Existing indices UNCHANGED → downstream tables valid
```

### Incremental Deletion (NULL gap + LB cleanup)

```
Before: 4 routers, n_array_alloc = 4
  [R0][R1][R2][R3]

lr-del R1:
  1. Clear R1's bit from ALL LB bitmaps
  2. array[1] = NULL (gap)
  3. ovn_datapath_destroy() removes from hmap
  4. n_array_alloc stays at 4 (high watermark)

After: [R0][NULL][R2][R3]   n_array_alloc = 4, ods_size() = 3

  ods_size() = 3      (hmap_count — for counting live datapaths)
  ods_array_size() = 4 (n_array_alloc — for sizing index-based arrays)

  WHY NOT reshuffle indices?
  → LB bitmaps reference indices by position
  → dp_group bitmaps reference indices
  → Reshuffling invalidates ALL bitmaps
  → Would need O(flows × datapaths) to remap every bitmap
```

### Why n_array_alloc Matters

```
After deleting R1: ods_size() = 3, but R3 has index 3

  Downstream array allocation:
    WRONG: xrealloc(array, ods_size() * sizeof(ptr))   → 3 elements
           R3 at index 3 → OUT OF BOUNDS!

    RIGHT: xrealloc(array, ods_array_size() * sizeof(ptr)) → 4 elements
           R3 at index 3 → VALID

  n_array_alloc is a HIGH WATERMARK:
    - Set by ods_build_array_index() during full recompute
    - Set by ods_append_datapath() during creation
    - NEVER decreased during deletion
    - Reset only on next full recompute
```

### Binary UPDATE_BATCH (Phase D)

Extend server to send incremental updates as binary frames.
Add XOR support to `ovsdb_idl_binary_row_change()`.

### Batch Accumulation (Phase E)

Accumulate multiple binary frames before triggering `engine_run()`.
