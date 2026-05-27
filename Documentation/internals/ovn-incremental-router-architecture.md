# Architecture: Incremental Router Processing & Binary Transport Optimization

## Overview

This document describes the architectural changes made to OVS and OVN to optimize
database synchronization and incremental flow processing for large-scale deployments.

Two complementary optimizations:
1. **Binary→Datum Direct Path** (OVS): Eliminates JSON intermediate in binary transport
2. **Incremental Router Creation** (OVN): Avoids full northd recompute on router add

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

  NB: lr-add lr_new (standalone)
       |
       v
  northd_handle_lr_changes()
       |
       +-- nbrec_logical_router_is_new() == true
       +-- n_ports == 0 && n_nat == 0 && ...
       |
       +-- INCREMENTAL:
           |
           +-- ovn_datapath_create()
           +-- allocate tunnel key
           +-- insert SB datapath_binding
           +-- init mcast info
           +-- add to lr_list
           +-- rebuild array index
           +-- track as NORTHD_TRACKED_LR_CREATED
           |
           v
       en_lflow detects LR_CREATED
           |
           +-- return false (lflow recompute)
               |
               +-- rebuild ALL logical flows
                   (but northd data structures
                    are NOT rebuilt — only flows)
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

| Operation | northd | lflow | SB sync |
|-----------|--------|-------|---------|
| Standalone router add | **norecompute** | recompute | recompute |
| Router + ports add | recompute | recompute | recompute |
| Router + NAT add | recompute | recompute | recompute |
| Router delete | recompute | recompute | recompute |
| NAT modify (existing) | norecompute | recompute | norecompute |
| LB modify (existing) | norecompute | norecompute | norecompute |
| LRP change | recompute | recompute | recompute |
| Static route change | recompute | recompute | recompute |

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

Complex router creation falls back to full recompute:

```c
if (changed_lr->n_ports > 0       // Has ports in same transaction
    || changed_lr->n_nat > 0      // Has NAT rules
    || changed_lr->n_load_balancer > 0  // Has load balancers
    || changed_lr->n_policies > 0      // Has routing policies
    || changed_lr->n_static_routes > 0 // Has static routes
    || !lrouter_is_enabled(changed_lr)) { // Disabled router
    goto fail;  // Full recompute — safe, correct
}
```

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
| `northd/northd.h` | `NORTHD_TRACKED_LR_CREATED/DELETED` enums, `trk_created_lrs/trk_deleted_lrs` hmapx |
| `northd/northd.c` | Actual datapath materialization in `northd_handle_lr_changes()`, `northd_handle_lrp_changes()` |
| `northd/en-northd.h` | `northd_nb_logical_router_port_handler` declaration |
| `northd/en-northd.c` | LR handler passes txn, LRP handler implementation |
| `northd/en-lflow.c` | Detect `NORTHD_TRACKED_LR_CREATED` → fall back to lflow recompute |
| `northd/inc-proc-northd.c` | Four new NB_NODE entries, DAG wiring |
| `tests/ovn-northd.at` | 7 new tests, 1 updated existing test |
| `tests/perf-northd.at` | Binary transport performance test |
| `Documentation/automake.mk` | New doc files in distribution |

## 5. Future Work

### Per-Datapath lflow_ref (avoids lflow recompute)

Add `struct lflow_ref *lflow_ref` to `struct ovn_datapath`. When a router
is created incrementally, generate only that router's base flows and sync
via `lflow_ref_sync_lflows()`. This would change the lflow column from
"recompute" to "norecompute compute" for standalone router creation.

### Incremental Router Deletion

Requires cleaning up:
- All logical flows referencing the router's datapath
- SB datapath_binding record
- lr_group membership (may affect connected routers)
- Port bindings (if any remain)

### Per-LRP Incremental Flow Generation

The LRP handler currently returns false. Full implementation needs:
- Look up port via `ovn_port_find(&nd->lr_ports, lrp->name)`
- Find parent router via `op->od`
- Generate per-LRP flows via `build_lswitch_and_lrouter_iterate_by_lrp()`
- Sync via `lflow_ref_sync_lflows(op->lflow_ref, ...)`
- Handle peer port bidirectional linking

### Binary UPDATE_BATCH (Phase D)

Extend server to send incremental updates as binary frames (not just initial
snapshot). Add XOR support to `ovsdb_idl_binary_row_change()`.

### Batch Accumulation (Phase E)

Accumulate multiple binary frames before triggering `engine_run()` to reduce
engine evaluation frequency during bulk operations.
