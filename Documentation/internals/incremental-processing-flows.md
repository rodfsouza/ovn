# Incremental Processing Flows — Code Change Explanation

For data structure details and handler implementation, see also
[incremental-route-processing.md](../incremental-route-processing.md).

## What Changed

This branch (`feature/northd-incremental-processing`) restructures how northd
handles static route changes. Previously, any route change triggered a full
lflow recompute — rebuilding all logical flows for all routers and rewriting
them to SB. Now, route changes are processed incrementally: only the affected
route's flows are added, removed, or updated in SB.

### Key Architectural Change: Three-Tier lflow_ref System

Before (main):
```
ovn_datapath {
    lflow_ref          // ALL router flows tracked here
}
```

After (this branch):
```
ovn_datapath {
    lflow_ref          // General router flows (ARP, neighbor, mcast, etc.)
    route_lflow_ref    // Skeleton route flows (default drops, route_table pre-flows)
    policy_lflow_ref   // Policy flows (lr-policy-add)
}

ecmp_route_node {
    lflow_ref          // Per-route/ECMP-group flows (individual routing decisions)
}
```

This separation enables surgical updates: changing one route only rebuilds and
syncs the flows under that route's `lflow_ref`, leaving the thousands of other
flows untouched.

### New Engine Node: en_group_ecmp_route

A new engine node sits between `en_northd` and `en_lflow`:

```
en_northd ──→ en_group_ecmp_route ──→ en_lflow ──→ SB Logical_Flow
```

Note: `en_lflow` has many other inputs (BFD tables, ACL tables, sync_meters,
stateful nodes, etc.) that can independently trigger recompute. The chain
above shows only the route-processing path.

Its job is to precompute ECMP groupings and maintain per-route `lflow_ref`
objects. This decouples "which routes exist and how are they grouped" from
"generate the actual flows," enabling the lflow handler to operate on
individual routes rather than rebuilding everything.

---

## Flow Diagrams

### 1. Router Creation (Incremental Path)

When a new logical router is created via `ovn-nbctl lr-add`, here is the
complete sequence through the engine DAG:

```
 NB Transaction: lr-add lr0 + lrp-add + lr-route-add
 ┌────────────────────────────────────────────────────────────────────────┐
 │                                                                        │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │                    en_northd handler                             │  │
 │  │                                                                  │  │
 │  │  1. Detect new LR (nbrec_logical_router_is_new)                  │  │
 │  │  2. Create ovn_datapath with:                                    │  │
 │  │     - od->lflow_ref         (general flows)                      │  │
 │  │     - od->route_lflow_ref   (skeleton route flows)               │  │
 │  │     - od->policy_lflow_ref  (policy flows)                       │  │
 │  │  3. Create ovn_port for each LRP, insert SB port_binding         │  │
 │  │  4. Add to route_to_lr_map for each static route                 │  │
 │  │  5. Set tracked data flags:                                      │  │
 │  │     - NORTHD_TRACKED_LR_CREATED  (od in trk_created_lrs)        │  │
 │  │     - NORTHD_TRACKED_LR_ROUTES   (od in lr_with_changed_routes) │  │
 │  │     - NORTHD_TRACKED_LR_PORTS    (ports in trk_lrps.created)    │  │
 │  │     - NORTHD_TRACKED_LR_POLICIES (if policies exist)            │  │
 │  └──────────────────┬───────────────────────────────────────────────┘  │
 │                     │                                                  │
 │                     ▼                                                  │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │            en_group_ecmp_route handler                           │  │
 │  │                                                                  │  │
 │  │  Processes NORTHD_TRACKED_LR_ROUTES first:                       │  │
 │  │  1. No existing group_ecmp_datapath → build from scratch:        │  │
 │  │     a. Parse all static routes → parsed_route list               │  │
 │  │     b. Group into ECMP groups vs unique routes                   │  │
 │  │     c. Create ecmp_route_node for each route/group               │  │
 │  │        each with its own lflow_ref                               │  │
 │  │  2. Add all route_nodes to trk_data.crupdated_datapath_routes    │  │
 │  │                                                                  │  │
 │  │  Then processes NORTHD_TRACKED_LR_CREATED:                       │  │
 │  │  3. group_ecmp_datapath_lookup(od) → already exists (step 1)     │  │
 │  │     → SKIP (no duplicate work)                                   │  │
 │  └──────────────────┬───────────────────────────────────────────────┘  │
 │                     │                                                  │
 │                     ▼                                                  │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │                    en_lflow handlers                             │  │
 │  │                                                                  │  │
 │  │  lflow_northd_handler (NORTHD_TRACKED_LR_CREATED):               │  │
 │  │  1. build_lr_flows_for_datapath(od):                             │  │
 │  │     ┌─────────────────────────────────────────────────────────┐  │  │
 │  │     │ General flows → od->lflow_ref:                          │  │  │
 │  │     │   build_adm_ctrl_flows_for_lrouter                     │  │  │
 │  │     │   build_neigh_learning_flows_for_lrouter                │  │  │
 │  │     │   build_ND_RA_flows_for_lrouter                        │  │  │
 │  │     │   build_mcast_lookup_flows_for_lrouter                 │  │  │
 │  │     │   build_arp_resolve_flows_for_lrouter                  │  │  │
 │  │     │   build_check_pkt_len_flows_for_lrouter                │  │  │
 │  │     │   build_gateway_redirect_flows_for_lrouter             │  │  │
 │  │     │   build_arp_request_flows_for_lrouter  (*)              │  │  │
 │  │     │   build_lrouter_network_id_flows                       │  │  │
 │  │     │   build_misc_local_traffic_drop_flows_for_lrouter      │  │  │
 │  │     │   build_lr_nat_defrag_and_lb_default_flows             │  │  │
 │  │     │   build_lrouter_lb_affinity_default_flows              │  │  │
 │  │     │                                                         │  │  │
 │  │     │   (*) Also called per-route in group_ecmp_route handler │  │  │
 │  │     │       with rn->lflow_ref — see note on dual ownership   │  │  │
 │  │     │       in lflow_ref Ownership Map below.                 │  │  │
 │  │     ├─────────────────────────────────────────────────────────┤  │  │
 │  │     │ Skeleton route flows → od->route_lflow_ref:             │  │  │
 │  │     │   build_ip_routing_pre_flows_for_lrouter                │  │  │
 │  │     │   build_static_route_flows_for_lrouter (skeleton only)  │  │  │
 │  │     │     - default drops for IP_ROUTING, IP_ROUTING_ECMP     │  │  │
 │  │     │     - ECMP bypass (ecmp_group_id == 0 → next)           │  │  │
 │  │     │     - per-LRP route_table pre-flows                     │  │  │
 │  │     │     - iterates route_nodes → calls build per rn->lflow_ref│ │  │
 │  │     ├─────────────────────────────────────────────────────────┤  │  │
 │  │     │ Policy flows → od->policy_lflow_ref:                    │  │  │
 │  │     │   build_ingress_policy_flows_for_lrouter                │  │  │
 │  │     └─────────────────────────────────────────────────────────┘  │  │
 │  │  2. Sync od->lflow_ref to SB                                    │  │
 │  │  3. Sync od->route_lflow_ref to SB                              │  │
 │  │  4. Sync od->policy_lflow_ref to SB                             │  │
 │  │  5. Handle per-port flows via lflow_handle_northd_lr_port_changes│  │
 │  │                                                                  │  │
 │  │  lflow_group_ecmp_route_handler (crupdated routes):              │  │
 │  │  6. For each crupdated ecmp_route_node:                          │  │
 │  │     a. Unlink existing flows from lflow_ref (none for new)       │  │
 │  │     b. Build route-specific flows:                               │  │
 │  │        - ECMP: build_ecmp_route_flow → rn->lflow_ref            │  │
 │  │        - Non-ECMP: build_static_route_flow → rn->lflow_ref      │  │
 │  │     c. build_arp_request_flows_for_lrouter → rn->lflow_ref      │  │
 │  │        NOTE: adds ARP/ND flows for ALL route nexthops on this    │  │
 │  │        router, not just this route's nexthop. The dp_refcnt      │  │
 │  │        mechanism in lflow-mgr.c handles the resulting duplicate   │  │
 │  │        references correctly.                                     │  │
 │  │     d. Sync rn->lflow_ref to SB                                 │  │
 │  └──────────────────────────────────────────────────────────────────┘  │
 │                                                                        │
 │  Result: SB Logical_Flow has all flows for lr0                         │
 │  - General flows (admission, ARP, ND, mcast, etc.)                     │
 │  - Route skeleton flows (default drops, pre-flows)                     │
 │  - Per-route flows (ip.dst == X → nexthop Y)                           │
 │  - Policy flows (lr-policy rules)                                      │
 │  - Per-port flows (egress delivery, connected routes)                  │
 └────────────────────────────────────────────────────────────────────────┘
```

### 2. Route Add (Incremental — Steady State)

When a route is added to an existing router:

```
 NB Transaction: lr-route-add lr0 192.168.0.0/16 10.0.0.254
 ┌────────────────────────────────────────────────────────────────────┐
 │                                                                    │
 │  en_northd handler                                                 │
 │  ├─ Detects static_routes column changed on lr0                    │
 │  ├─ Updates route_to_lr_map                                        │
 │  ├─ Adds od to lr_with_changed_routes                              │
 │  └─ Sets NORTHD_TRACKED_LR_ROUTES                                 │
 │                       │                                            │
 │                       ▼                                            │
 │  en_group_ecmp_route handler                                       │
 │  ├─ Finds existing group_ecmp_datapath for lr0                     │
 │  ├─ Saves old route_nodes (with their lflow_refs)                  │
 │  ├─ Destroys old ECMP groups + unique routes + parsed routes       │
 │  ├─ Rebuilds from current NB routes:                               │
 │  │   parse routes → group ECMP → create new route_nodes            │
 │  ├─ For unchanged unique routes (same UUID):                       │
 │  │   REUSE old lflow_ref → no crupdated → no SB write             │
 │  │   (Note: ECMP groups always get new lflow_refs since            │
 │  │    membership may have changed — no reuse optimization)         │
 │  ├─ For new/changed routes:                                        │
 │  │   New lflow_ref → add to crupdated_datapath_routes              │
 │  └─ Old route_nodes with no match → deleted_datapath_routes        │
 │                       │                                            │
 │                       ▼                                            │
 │  en_lflow: lflow_group_ecmp_route_handler                          │
 │  ├─ For each DELETED route_node:                                   │
 │  │   lflow_ref_unlink_lflows → sync → removes SB rows             │
 │  ├─ For each CRUPDATED route_node:                                 │
 │  │   unlink → build_static_route_flow → sync → adds SB rows       │
 │  └─ Unchanged routes: nothing happens (lflow_ref reused as-is)     │
 │                                                                    │
 │  Result: Only 1 route's flows written to SB (not all routes)       │
 └────────────────────────────────────────────────────────────────────┘
```

### 3. Route Delete (Incremental)

```
 NB Transaction: lr-route-del lr0 192.168.0.0/16
 ┌────────────────────────────────────────────────────────────────────┐
 │                                                                    │
 │  en_northd handler                                                 │
 │  └─ Same as route add: NORTHD_TRACKED_LR_ROUTES                   │
 │                       │                                            │
 │                       ▼                                            │
 │  en_group_ecmp_route handler                                       │
 │  ├─ Saves old route_nodes                                          │
 │  ├─ Rebuilds (deleted route is gone from NB)                       │
 │  ├─ Old route_node for deleted route → deleted_datapath_routes     │
 │  └─ Remaining routes reuse lflow_refs (unchanged UUIDs)            │
 │                       │                                            │
 │                       ▼                                            │
 │  en_lflow handler                                                  │
 │  ├─ DELETED: unlink + sync → SB flows removed                     │
 │  └─ No crupdated (all other routes unchanged)                      │
 │                                                                    │
 │  Result: Only deleted route's SB flows removed                     │
 └────────────────────────────────────────────────────────────────────┘
```

### 4. Policy Change (Independent Path)

Policy changes bypass `en_group_ecmp_route` entirely:

```
 NB Transaction: lr-policy-add lr0 100 "ip4.src == 10.0.0.0/24" allow
 ┌────────────────────────────────────────────────────────────────────┐
 │                                                                    │
 │  en_northd handler                                                 │
 │  └─ Sets NORTHD_TRACKED_LR_POLICIES                               │
 │           │                                                        │
 │           │  (en_group_ecmp_route handler: no route/creation       │
 │           │   flags in tracked data → returns true, no state       │
 │           │   change)                                              │
 │           │                                                        │
 │           ▼                                                        │
 │  en_lflow: lflow_northd_handler                                    │
 │  ├─ Unlink od->policy_lflow_ref                                    │
 │  ├─ build_ingress_policy_flows_for_lrouter → od->policy_lflow_ref  │
 │  └─ Sync od->policy_lflow_ref to SB                               │
 │                                                                    │
 │  Result: Only policy flows rewritten, routes untouched             │
 └────────────────────────────────────────────────────────────────────┘
```

### 5. Full Recompute (Fallback)

Certain conditions force full recompute (all engine nodes run from scratch):

```
 Triggers (northd handler returns false → engine runs _run() instead):
  - Router deletion (NORTHD_TRACKED_LR_DELETED)
  - BFD route change (any route references a BFD entry)
  - DGW port changes (LRP modification/deletion falls back)
  - Unhandled column changes on LR (lr_changes_can_be_handled fails)
  - COPP meter changes on LR
  - Mutation limit exceeded (n_mutations > ODS_MUTATION_LIMIT)
  - Disabled router creation (!lrouter_is_enabled)
  - Static MAC bindings exist in NB
  - Multi-router group deletion (lr_group->n_router_dps > 1)

 Additionally, en_lflow recomputes when NB_bfd or SB_bfd tables change
 (NULL handlers in DAG wiring → automatic recompute).

 ┌────────────────────────────────────────────────────────────────────┐
 │  en_northd_run()     → rebuild ALL datapaths, ports, routes       │
 │         │                                                          │
 │         ▼                                                          │
 │  en_group_ecmp_route_run() → rebuild ALL ECMP groups, route_nodes │
 │         │                     (each with fresh lflow_ref)          │
 │         ▼                                                          │
 │  en_lflow_run()      → clear lflow_table                          │
 │    build_bfd_table() → BFD connections (only here)                │
 │    build_lflows()    → ALL flows for ALL datapaths                 │
 │      per router:                                                   │
 │        build_lswitch_and_lrouter_iterate_by_lr(od):                │
 │          general flows    → od->lflow_ref                          │
 │          skeleton routes  → od->route_lflow_ref                    │
 │          per-route flows  → rn->lflow_ref (from group_ecmp_data)  │
 │          policy flows     → od->policy_lflow_ref                   │
 │    sync ALL lflow_refs to SB                                       │
 └────────────────────────────────────────────────────────────────────┘
```

---

## Engine DAG Wiring (inc-proc-northd.c)

```
                    ┌───────────┐
           ┌───────│  en_northd │───────┬──────────────┐
           │       └─────┬─────┘       │              │
           │             │              │              │
           ▼             │              ▼              ▼
  ┌─────────────────┐   │      ┌──────────────┐  ┌──────────┐
  │ en_group_ecmp   │   │      │ en_lr_nat    │  │ en_port  │
  │     _route      │   │      └──────┬───────┘  │ _group   │
  └────────┬────────┘   │             │          └──┬───┬───┘
           │            │             ▼              │   │
           │            │      ┌──────────────┐      │   │
           │            │      │en_lr_stateful│      │   │
           │            │      └──────┬───────┘      │   │
           │            │             │              │   │
           │            │      ┌──────────────┐      │   │
           │            ├─────▶│en_ls_stateful│◀─────┘   │
           │            │      └──────┬───────┘          │
           │            │             │                   │
           │            ▼             ▼                   ▼
           │       ┌──────────────────────────────────────────┐
           └──────▶│              en_lflow                    │
                   │                                          │
                   │  Handlers:                                │
                   │  - lflow_northd_handler                   │
                   │  - lflow_group_ecmp_route_handler         │
                   │  - lflow_port_group_handler               │
                   │  - lflow_lr_stateful_handler              │
                   │  - lflow_ls_stateful_handler              │
                   └──────────────────┬───────────────────────┘
                                      │
                                      ▼
                             SB Logical_Flow table
```

## lflow_ref Ownership Map

```
┌──────────────────────────────────────────────────────────────────────┐
│                        ovn_datapath (od)                             │
│                                                                      │
│  od->lflow_ref           ← General router flows                      │
│    admission control, neighbor learning, ND/RA, mcast,               │
│    ARP resolve, packet length check, gateway redirect,               │
│    ARP request (*), network ID, misc drops, NAT/LB defaults         │
│                                                                      │
│    (*) build_arp_request_flows_for_lrouter is called here during     │
│        full recompute and router creation (od->lflow_ref), but also  │
│        called per crupdated route in the group_ecmp_route handler    │
│        with rn->lflow_ref. See DUAL OWNERSHIP note below.           │
│                                                                      │
│  od->route_lflow_ref     ← Route skeleton flows                      │
│    default drops (IP_ROUTING, IP_ROUTING_ECMP stages),               │
│    ECMP bypass rule, per-LRP route_table pre-flows                   │
│                                                                      │
│  od->policy_lflow_ref    ← Policy flows                              │
│    lr-policy-add rules (ingress policy stage)                        │
│                                                                      │
├──────────────────────────────────────────────────────────────────────┤
│                 ecmp_route_node (rn) — one per route/group           │
│                                                                      │
│  rn->lflow_ref           ← Per-route flows                           │
│    ECMP: select group, ct_next, routing decision                     │
│    Non-ECMP: ip.dst match → set nexthop/outport                     │
│    ARP/ND request flows for ALL route nexthops on this router (*)    │
│                                                                      │
│    (*) DUAL OWNERSHIP: build_arp_request_flows_for_lrouter iterates  │
│        ALL od->nbr->n_static_routes and emits ARP/ND flows for      │
│        every nexthop, not just the current route's. Each crupdated   │
│        rn->lflow_ref gets the full set. The dp_refcnt mechanism in   │
│        lflow-mgr.c correctly handles multiple lflow_refs referencing │
│        the same flow for the same datapath (refcount-based bitmap).  │
│                                                                      │
│  Lifecycle: created by en_group_ecmp_route, consumed by en_lflow     │
│  Reuse: unchanged unique routes keep their lflow_ref across updates  │
│         ECMP groups always get new lflow_refs (membership may change)│
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                         ovn_port (op)                                │
│                                                                      │
│  op->lflow_ref           ← Per-port flows                            │
│    egress delivery, connected routes, ARP/ND for port networks       │
│                                                                      │
│  op->stateful_lflow_ref  ← Stateful per-port flows                  │
│    NAT, LB, ACL flows specific to this port                          │
└──────────────────────────────────────────────────────────────────────┘
```

## Performance Impact

| Scenario | Before (full recompute) | After (incremental) |
|----------|------------------------|---------------------|
| Add 1 route (2000 existing) | ~62ms | ~21ms (projected target, based on upstream measurements) |
| Delete 1 route | Rebuild ALL flows | Unlink 1 lflow_ref |
| Policy change | Rebuild ALL flows | Rebuild policy_lflow_ref only |
| Route unchanged | Rebuild ALL flows | No SB write (lflow_ref reused) |

The key insight is that `lflow_ref` reuse for unchanged routes means the
common case (add/delete one route among thousands) touches only O(1) SB rows
instead of O(N) where N is the total number of routes across all routers.
