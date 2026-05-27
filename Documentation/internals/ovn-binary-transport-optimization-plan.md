# Plan: Optimize Northd Incremental Flow Processing with Binary Transport

## Implementation Status (2026-05-26)

| Phase | Status | Commit | Notes |
|-------|--------|--------|-------|
| **A** | **DONE** | OVS `aed87a9f7` | Direct binary→datum path. Eliminates JSON round-trip for ROW_BATCH. |
| **B** | **DONE** | OVN `a4cd3936a` | Perf test in `tests/perf-northd.at`. |
| **C.1** | **DONE** | OVN (uncommitted) | Actual datapath materialization for standalone routers. `northd norecompute compute`. |
| **C.2** | **DONE** | OVN (uncommitted) | 4 new engine nodes (LRP, static_route, policy, NAT). |
| **C.3** | **DONE** | OVN (uncommitted) | LRP handler (returns false — safe recompute fallback). |
| **C.4** | NOT STARTED | — | Per-LRP incremental flow generation with `lflow_ref`. Requires per-datapath `lflow_ref`. |
| **C.5** | NOT STARTED | — | Incremental static route handling. |
| **C.6** | NOT STARTED | — | Router + ports in single transaction (Tier 2). |
| **C.7** | NOT STARTED | — | Incremental policy handling. |
| **D** | NOT STARTED | — | Binary UPDATE_BATCH with direct datum path. XOR support needed. |
| **E** | NOT STARTED | — | Streaming-aware batch processing. |

### Key Remaining Work for Full Incremental Path

1. **Per-datapath `lflow_ref`**: Add `struct lflow_ref *lflow_ref` to `struct ovn_datapath`. This enables lflow handler to generate only the new router's flows instead of full recompute. Changes `lflow` column from "recompute" to "norecompute compute".

2. **`lr_group` creation**: `build_lrouter_groups()` is not called incrementally. New standalone router needs a single-member `lr_group`. Must be created in `northd_handle_lr_changes()` after datapath materialization.

3. **Router deletion**: Requires cleaning up flows (via `lflow_ref`), SB `datapath_binding`, `lr_group` membership, and `lr_datapaths` hmap/array. Complex due to cross-datapath references.

4. **Per-LRP handling**: `northd_handle_lrp_changes()` currently returns false. Full implementation needs: port lookup via `lr_ports`, parent via `op->od`, per-LRP flow generation via `build_lswitch_and_lrouter_iterate_by_lrp()`, and `lflow_ref_sync_lflows()`.

5. **Binary XOR support**: `ovsdb_idl_binary_row_change()` has `OVS_NOT_REACHED()` for XOR mode. Phase D needs `ovsdb_datum_apply_diff_in_place()` support.

---

## Context

With binary transport enabled for OVN daemons (commit 4a57bf8b1), we identified two critical performance problems in production OVN deployments with 10,000+ chassis and SB databases exceeding 1 GB:

1. **Binary transport double conversion**: ROW_BATCH frames are decoded to `ovsdb_datum`, then converted back to JSON, then parsed from JSON back to `ovsdb_datum` — wasting ~60% of CPU in the update parsing path.

2. **Northd full recompute on router creation**: Any router creation/deletion triggers a full recompute of ALL logical flows across ALL datapaths, even though only the new router's flows need to change. At scale (1,000+ routers), this means hundreds of thousands of flows are rebuilt from scratch.

These two problems compound: binary transport saves bandwidth on the wire but the CPU savings are negated by JSON round-tripping, and even with faster transport, northd still wastes time rebuilding flows that haven't changed.

This plan addresses both in five phases:
- **Phase A**: Eliminate binary→JSON→datum round-trip in the OVS IDL layer
- **Phase B**: Add performance tests to measure the impact
- **Phase C**: Add incremental router creation/deletion handlers in northd
- **Phase D**: Binary UPDATE_BATCH with direct datum path
- **Phase E**: Streaming-aware batch processing to reduce engine churn

**Recommended implementation order**: A → B → C → D → E (Phase A+C together deliver the biggest combined improvement).

---

## Problem Analysis: Northd Incremental Processing Engine (IPE)

### Current Architecture

Northd uses a DAG of 30+ engine nodes, evaluated in topological order. The critical path for a NB database change:

```
NB Database (router created)
    ↓ ovsdb_idl_run() — fetches changes, populates track lists
    ↓
Engine DAG (topologically sorted)
    ↓
    en_nb_logical_router  →  detects tracked change  →  EN_UPDATED
    ↓
    en_northd  →  northd_nb_logical_router_handler()
                   → nbrec_logical_router_is_new() == true
                   → returns false (CANNOT handle incrementally)
                   → triggers en_northd_run() FULL RECOMPUTE
                   → rebuilds ALL datapaths, ports, LB mappings
    ↓
    en_lflow  →  lflow_northd_handler() or en_lflow_run()
                  → if incremental: only rebuild affected port flows
                  → if recompute: lflow_table_clear() + build_lflows()
                    → rebuilds ALL flows for ALL routers & switches
    ↓
    lflow_table_sync_to_sb()  →  diff against SB, insert/update/delete rows
    ↓
    ovsdb_idl_loop_commit_and_wait()  →  write to SB via OVSDB transaction
```

### The Router Creation Bottleneck

**File**: `northd/northd.c:4957-5000`

```c
bool
northd_handle_lr_changes(const struct northd_input *ni,
                         struct northd_data *nd)
{
    NBREC_LOGICAL_ROUTER_TABLE_FOR_EACH_TRACKED(changed_lr, ...) {
        if (nbrec_logical_router_is_new(changed_lr) ||
            nbrec_logical_router_is_deleted(changed_lr)) {
            goto fail;  // ← FULL RECOMPUTE — no incremental path exists
        }
        // Only NAT, LB, LB_group modifications handled incrementally
        if (!lr_changes_can_be_handled(changed_lr)) {
            goto fail;
        }
    }
}
```

This means **any** router creation/deletion triggers:
1. `en_northd_run()` — destroys and rebuilds ALL datapath structures
2. `en_lflow_run()` — clears entire lflow_table and regenerates ALL flows
3. `lflow_table_sync_to_sb()` — diffs entire flow table against SB

The same pattern exists for logical switch creation (`northd_handle_ls_changes()` at `northd.c:4818-4872`).

### Flow Generation Scale

Per router, northd generates:
- ~15-20 base flows (admission control, defaults, network ID)
- 1-2 flows per static route
- 2-6 flows per NAT entry (UNSNAT, DNAT, UNDNAT, SNAT variants)
- 4-8 flows per LB VIP per backend (ct.new, ct.est, undnat, unsnat)
- 8-15 flows per router port

At scale (1,000 routers × 10 ports × 5 NATs × 3 LB VIPs × 4 backends) = **hundreds of thousands of flows** rebuilt from scratch on a single router add.

### What IS Handled Incrementally Today

- Port additions/deletions on existing switches (`ls_handle_lsp_changes()`)
- NAT modifications on existing routers (`NORTHD_TRACKED_LR_NATS`)
- Load balancer association changes (`NORTHD_TRACKED_LBS`, `NORTHD_TRACKED_LS_LBS`)
- ACL changes on existing switches (`NORTHD_TRACKED_LS_ACLS`)

Each uses `lflow_ref` per entity — `lflow_ref_unlink_lflows()` → rebuild → `lflow_ref_sync_lflows()`.

### Parallelism Today

`--n-threads=N` (up to 256) parallelizes flow *generation* via `build_lflows_thread()` at `northd.c:16539`, partitioning by hmap bucket. But IDL parsing, engine_run(), and SB sync are all single-threaded.

### Binary Transport Current Impact

| Phase | JSON Path | Binary Path | Savings |
|-------|-----------|-------------|---------|
| Server encoding | JSON serialization | Binary encoding | ~40% CPU on server |
| Network transfer | Full JSON (~1 GB for large SB) | Compact binary (~600 MB) | ~40% bandwidth |
| Client decoding | JSON parse → datum | Binary → datum → JSON → datum | **Worse** (double conversion) |
| IDL row update | datum comparison + track | Same | None |
| Engine run | Same | Same | None |
| SB commit | Same | Same | None |

**Current net impact**: Significant for **startup time** (network savings on initial snapshot), marginal-to-negative for **incremental updates** (CPU overhead from double conversion).

---

## Phase A: Direct Binary→Datum Path (OVS Layer)

### Problem

In `ovs/lib/ovsdb-cs.c:589-699`, the binary ROW_BATCH handler does:
```
binary wire → ovsdb_binary_deserialize_datum() → datum
           → ovsdb_datum_to_json() → JSON              ← WASTEFUL
           → (later) ovsdb_datum_from_json() → datum    ← WASTEFUL
```

The JSON intermediate exists only because the IDL parser (`ovsdb_idl_row_change()`) expects a `shash *` of column_name→JSON. We need a parallel path that accepts binary data directly.

### Design

Add a new event type `OVSDB_CS_EVENT_TYPE_BINARY_UPDATE` that carries structured binary data (table name, row UUID, column datums) without JSON conversion. The IDL processes these events via a new `ovsdb_idl_process_binary_update()` function that writes datums directly into `row->old_datum[]`.

### Step A1: New data structures in `ovs/lib/ovsdb-cs.h`

Add binary-specific structures alongside existing ones:

```c
/* Binary column update — datum already deserialized from wire. */
struct ovsdb_cs_binary_column {
    char *col_name;
    struct ovsdb_datum datum;
    struct ovsdb_type col_type;
};

/* Binary row update — contains pre-deserialized datums. */
struct ovsdb_cs_binary_row_update {
    struct uuid row_uuid;
    enum ovsdb_cs_row_update_type type;  /* Reuse existing enum */
    struct ovsdb_cs_binary_column *columns;
    size_t n_columns;
};

/* Binary table update. */
struct ovsdb_cs_binary_table_update {
    char *table_name;
    struct ovsdb_cs_binary_row_update *row_updates;
    size_t n;
};

/* Binary DB update — parallel to ovsdb_cs_db_update. */
struct ovsdb_cs_binary_db_update {
    struct ovsdb_cs_binary_table_update *table_updates;
    size_t n;
};
```

Add new event type to `enum ovsdb_cs_event_type`:
```c
OVSDB_CS_EVENT_TYPE_BINARY_UPDATE,  /* Binary update with pre-parsed datums */
```

Add to the event union:
```c
struct ovsdb_cs_binary_update_event {
    bool clear;
    bool monitor_reply;
    struct ovsdb_cs_binary_db_update *du;
    struct uuid last_id;
} binary_update;
```

**File**: `ovs/lib/ovsdb-cs.h` (after line 112)

### Step A2: Modify ROW_BATCH handler to produce binary events

Replace the JSON synthesis in `ovsdb_cs_process_binary_row_batch()` with direct datum storage.

**File**: `ovs/lib/ovsdb-cs.c:589-699`

Instead of:
```c
// Current: binary → datum → JSON → synthetic notify → parse JSON
ovsdb_binary_deserialize_datum(&reader, &datum, &col_type, true);
json_object_put(row_json, col_name, ovsdb_datum_to_json(&datum, &col_type));
ovsdb_datum_destroy(&datum, &col_type);
```

Do:
```c
// New: binary → datum → store directly
ovsdb_binary_deserialize_datum(&reader, &datum, &col_type, true);
columns[c].col_name = col_name;  // Transfer ownership
columns[c].datum = datum;         // Transfer ownership (no destroy)
columns[c].col_type = col_type;
```

At the end, instead of creating a synthetic JSON notification:
```c
// New: emit binary event directly
struct ovsdb_cs_event *event = xmalloc(sizeof *event);
event->type = OVSDB_CS_EVENT_TYPE_BINARY_UPDATE;
event->binary_update.clear = false;
event->binary_update.monitor_reply = false;
event->binary_update.du = du;
event->binary_update.last_id = cs->data.last_id;
ovs_list_push_back(&cs->data.events, &event->list_node);
```

Add a cleanup/destroy function:
```c
static void
ovsdb_cs_binary_db_update_destroy(struct ovsdb_cs_binary_db_update *du)
{
    for (size_t i = 0; i < du->n; i++) {
        struct ovsdb_cs_binary_table_update *tu = &du->table_updates[i];
        for (size_t j = 0; j < tu->n; j++) {
            struct ovsdb_cs_binary_row_update *ru = &tu->row_updates[j];
            for (size_t k = 0; k < ru->n_columns; k++) {
                free(ru->columns[k].col_name);
                ovsdb_datum_destroy(&ru->columns[k].datum,
                                    &ru->columns[k].col_type);
            }
            free(ru->columns);
        }
        free(tu->table_name);
        free(tu->row_updates);
    }
    free(du->table_updates);
    free(du);
}
```

### Step A3: Add binary event handler in IDL

**File**: `ovs/lib/ovsdb-idl.c`

Add a new case in `ovsdb_idl_run()` event loop (after line 480):
```c
case OVSDB_CS_EVENT_TYPE_BINARY_UPDATE:
    ovsdb_idl_process_binary_update(idl, e);
    break;
```

Implement the new function:
```c
static void
ovsdb_idl_process_binary_update(struct ovsdb_idl *idl,
                                const struct ovsdb_cs_event *event)
{
    const struct ovsdb_cs_binary_db_update *du = event->binary_update.du;

    if (event->binary_update.clear) {
        ovsdb_idl_clear(idl);
    }

    for (size_t i = 0; i < du->n; i++) {
        const struct ovsdb_cs_binary_table_update *tu = &du->table_updates[i];
        struct ovsdb_idl_table *table =
            shash_find_data(&idl->table_by_name, tu->table_name);
        if (!table) {
            continue;
        }

        for (size_t j = 0; j < tu->n; j++) {
            const struct ovsdb_cs_binary_row_update *ru = &tu->row_updates[j];
            enum ovsdb_idl_update_type type =
                ovsdb_idl_process_binary_row_update(table, ru);
            if (type == OVSDB_IDL_UPDATE_DB_CHANGED) {
                idl->change_seqno++;
            }
        }
    }

    ovsdb_cs_binary_db_update_destroy(du);
}
```

### Step A4: Binary row update processing

**File**: `ovs/lib/ovsdb-idl.c`

New function parallel to `ovsdb_idl_process_update()`:
```c
static enum ovsdb_idl_update_type
ovsdb_idl_process_binary_row_update(struct ovsdb_idl_table *table,
                                    const struct ovsdb_cs_binary_row_update *ru)
{
    struct ovsdb_idl_row *row = ovsdb_idl_get_row(table, &ru->row_uuid);

    switch (ru->type) {
    case OVSDB_CS_ROW_DELETE:
        if (row) {
            ovsdb_idl_delete_row(row);
            return OVSDB_IDL_UPDATE_DB_CHANGED;
        }
        return OVSDB_IDL_UPDATE_DB_CHANGED;

    case OVSDB_CS_ROW_INSERT:
        if (!row) {
            row = ovsdb_idl_row_create(table, &ru->row_uuid);
        }
        ovsdb_idl_binary_insert_row(row, ru);
        return OVSDB_IDL_UPDATE_DB_CHANGED;

    case OVSDB_CS_ROW_UPDATE:
    case OVSDB_CS_ROW_XOR:
        if (row) {
            return ovsdb_idl_binary_modify_row(row, ru,
                       ru->type == OVSDB_CS_ROW_XOR)
                   ? OVSDB_IDL_UPDATE_DB_CHANGED
                   : OVSDB_IDL_UPDATE_DB_UNCHANGED;
        }
        return OVSDB_IDL_UPDATE_INCONSISTENT;

    default:
        OVS_NOT_REACHED();
    }
}
```

### Step A5: Direct datum insertion/modification

**File**: `ovs/lib/ovsdb-idl.c`

```c
/* Insert row with pre-deserialized binary datums — no JSON parsing. */
static void
ovsdb_idl_binary_insert_row(struct ovsdb_idl_row *row,
                            const struct ovsdb_cs_binary_row_update *ru)
{
    const struct ovsdb_idl_table_class *class = row->table->class_;

    /* Initialize old_datum with defaults. */
    row->old_datum = xmalloc(class->n_columns * sizeof *row->old_datum);
    for (size_t i = 0; i < class->n_columns; i++) {
        ovsdb_datum_init_default(&row->old_datum[i], &class->columns[i].type);
    }

    /* Apply binary columns directly. */
    ovsdb_idl_binary_row_change(row, ru, OVSDB_IDL_CHANGE_INSERT);

    ovsdb_idl_row_parse(row);
    ovsdb_idl_add_to_indexes(row);
}

/* Modify row with pre-deserialized binary datums — no JSON parsing. */
static bool
ovsdb_idl_binary_modify_row(struct ovsdb_idl_row *row,
                            const struct ovsdb_cs_binary_row_update *ru,
                            bool xor)
{
    ovsdb_idl_remove_from_indexes(row);
    ovsdb_idl_row_unparse(row);

    bool changed = ovsdb_idl_binary_row_change(
        row, ru, OVSDB_IDL_CHANGE_MODIFY);

    ovsdb_idl_row_parse(row);
    ovsdb_idl_add_to_indexes(row);
    return changed;
}
```

### Step A6: Core binary row change function

**File**: `ovs/lib/ovsdb-idl.c`

This replaces `ovsdb_idl_row_change()` for binary data. The key difference: instead of calling `ovsdb_datum_from_json()`, it directly swaps/copies the pre-deserialized datum.

```c
/* Apply pre-deserialized binary column datums to a row.
 * Parallel to ovsdb_idl_row_change() but skips JSON parsing. */
static bool
ovsdb_idl_binary_row_change(struct ovsdb_idl_row *row,
                            const struct ovsdb_cs_binary_row_update *ru,
                            enum ovsdb_idl_change change)
{
    struct ovsdb_idl_table *table = row->table;
    const struct ovsdb_idl_table_class *class = table->class_;
    bool dominated_change = (change == OVSDB_IDL_CHANGE_INSERT);
    bool datum_changed = false;

    for (size_t i = 0; i < ru->n_columns; i++) {
        const struct ovsdb_cs_binary_column *bc = &ru->columns[i];
        const struct ovsdb_idl_column *column =
            shash_find_data(&table->columns, bc->col_name);
        if (!column) {
            continue;
        }

        unsigned int column_idx = column - class->columns;
        struct ovsdb_datum *old = &row->old_datum[column_idx];

        /* Clone the datum (ru still owns the original). */
        struct ovsdb_datum new_datum;
        ovsdb_datum_clone(&new_datum, &bc->datum, &column->type);

        if (!ovsdb_datum_equals(old, &new_datum, &column->type)) {
            ovsdb_datum_swap(old, &new_datum);
            datum_changed = true;

            if (table->modes[column_idx] & OVSDB_IDL_ALERT) {
                row->change_seqno[change] =
                    row->table->change_seqno[change] =
                    row->table->idl->change_seqno + 1;

                if (table->modes[column_idx] & OVSDB_IDL_TRACK) {
                    if (!ovsdb_idl_track_is_set(row->table)) {
                        ovs_list_push_back(&row->table->track_list,
                                           &row->track_node);
                    }
                    if (!row->updated) {
                        row->updated = bitmap_allocate(class->n_columns);
                    }
                    bitmap_set1(row->updated, column_idx);
                    if (dominated_change && row->tracked_old_datum) {
                        row->tracked_old_datum[column_idx] = new_datum;
                    }
                }
            }
        }
        ovsdb_datum_destroy(&new_datum, &column->type);
    }

    return datum_changed;
}
```

### Step A7: Handle INITIAL_END correctly with binary events

**File**: `ovs/lib/ovsdb-cs.c`

The event batching logic at line 872-879 already works correctly — binary events are accumulated in `cs->data.events` and flushed when `binary_initial_pending = false`. No changes needed here since we're using the same event list.

### Step A8: Update event cleanup

**File**: `ovs/lib/ovsdb-cs.c`

In `ovsdb_cs_run()`, ensure binary events are properly cleaned up if the event list is discarded:
```c
/* In any error/cleanup path that destroys events: */
case OVSDB_CS_EVENT_TYPE_BINARY_UPDATE:
    ovsdb_cs_binary_db_update_destroy(e->binary_update.du);
    break;
```

---

## Phase B: Performance Test

### Step B1: Add binary transport performance test

**File**: `ovn/tests/perf-northd.at` (append at end)

```bash
AT_SETUP([ovn -- binary transport performance impact])
AT_KEYWORDS([binary-transport perf])

AT_SKIP_IF([test "$HAVE_OPENSSL" = no])

# Build a scale topology: 10 routers, 20 switches, 200 ports
ovn_start

# Create topology via batch commands
OVN_NBCTL([
    for i in $(seq 1 10); do
        lr-add lr$i
        for j in $(seq 1 2); do
            ls-add ls${i}_${j}
            lrp-add lr$i rp${i}_${j} 00:00:0$i:0$j:00:01 10.$i.$j.1/24
            lsp-add ls${i}_${j} lsp${i}_${j}_rp -- lsp-set-type lsp${i}_${j}_rp router \
                -- lsp-set-addresses lsp${i}_${j}_rp router \
                -- lsp-set-options lsp${i}_${j}_rp router-port=rp${i}_${j}
            for k in $(seq 1 10); do
                lsp-add ls${i}_${j} lsp${i}_${j}_${k} \
                    -- lsp-set-addresses lsp${i}_${j}_${k} \
                       "00:00:0$i:0$j:00:0$k 10.$i.$j.$((k+10))"
            done
        done
        lr-nat-add lr$i dnat_and_snat 172.16.$i.1 10.$i.1.11
    done
])

check ovn-nbctl --wait=sb sync

# Record baseline with binary transport (default=enabled)
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check as northd ovn-appctl -t ovn-northd stopwatch/reset

# Trigger a full recompute
check as northd ovn-appctl -t ovn-northd inc-engine/recompute
check ovn-nbctl --wait=sb sync

# Record binary transport metrics
PERF_RECORD_START()
PERF_RECORD_STOPWATCH([ovn-northd], [ovnnb_db_run])
PERF_RECORD_STOPWATCH([ovn-northd], [build_lflows])
PERF_RECORD_STOPWATCH([ovn-northd], [lflows_to_sb])
PERF_RECORD_STOPWATCH([ovn-northd], [ovn-northd-loop])
PERF_RECORD_STOP()

# Verify engine stats
check as northd ovn-appctl -t ovn-northd inc-engine/show-stats northd recompute
check as northd ovn-appctl -t ovn-northd inc-engine/show-stats lflow recompute

# Now test incremental: add a router (triggers full recompute today)
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check as northd ovn-appctl -t ovn-northd stopwatch/reset

check ovn-nbctl --wait=sb lr-add lr_new
check ovn-nbctl --wait=sb lrp-add lr_new rp_new 00:00:ff:00:00:01 10.99.1.1/24

# Check that engine stats show the cost of router creation
check_engine_stats northd recompute nocompute
check_engine_stats lflow recompute nocompute

# Record the cost
PERF_RECORD_START()
PERF_RECORD_STOPWATCH([ovn-northd], [ovnnb_db_run])
PERF_RECORD_STOPWATCH([ovn-northd], [build_lflows])
PERF_RECORD_STOPWATCH([ovn-northd], [lflows_to_sb])
PERF_RECORD_STOPWATCH([ovn-northd], [ovn-northd-loop])
PERF_RECORD_STOP()

# Test incremental path: modify NAT on existing router (should be incremental)
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check as northd ovn-appctl -t ovn-northd stopwatch/reset

check ovn-nbctl --wait=sb lr-nat-add lr1 snat 172.16.1.100 10.1.0.0/16

# NAT changes should be handled incrementally
check_engine_stats northd norecompute compute
check_engine_stats lflow recompute nocompute

AT_CLEANUP
```

### Step B2: Add binary transport IDL-level timing

Add a new stopwatch in the OVS IDL to measure update parsing time separately from network I/O.

**File**: `ovs/lib/ovsdb-idl.c`

In `ovsdb_idl_create()`, register:
```c
stopwatch_create(OVSDB_IDL_UPDATE_PARSE_STOPWATCH, SW_US);
```

In `ovsdb_idl_run()`, wrap event processing:
```c
stopwatch_start(OVSDB_IDL_UPDATE_PARSE_STOPWATCH, time_usec());
/* ... process events ... */
stopwatch_stop(OVSDB_IDL_UPDATE_PARSE_STOPWATCH, time_usec());
```

**File**: `ovs/lib/stopwatch-names.h` (or inline define)
```c
#define OVSDB_IDL_UPDATE_PARSE_STOPWATCH "idl-update-parse"
```

---

## Phase C: Incremental Router Creation/Deletion Handlers (OVN Layer)

### Problem

Router creation/deletion in `northd_handle_lr_changes()` unconditionally returns `false` (goto fail), triggering full recompute. This is the **highest user-visible bottleneck** — adding a single router to a deployment with 1,000 existing routers rebuilds ALL flows.

### Design

Follow the established pattern from `ls_handle_lsp_changes()` and the `lflow_ref` system. Add:
1. New tracked data types for router creation/deletion
2. Incremental handler that creates/destroys only the new router's datapath
3. Lflow handler that generates/removes only the affected router's flows

### Impact Analysis: Router Creation with Ports

When a router is created with ports (the common production case), the full initialization cascade involves **9 interdependent steps** across multiple subsystems:

```
User: ovn-nbctl lr-add lr1 -- lrp-add lr1 rp1 MAC IP/MASK -- ls-add ls1 -- lsp-add ls1 ... router

NB Transaction creates:
  ├── nbrec_logical_router (new)
  ├── nbrec_logical_router_port (new, referenced by router)
  ├── nbrec_logical_switch (new, if connecting switch is also new)
  └── nbrec_logical_switch_port (new, type="router", options:router-port=rp1)
```

**Step-by-step cascade in `ovnnb_db_run()`:**

| # | Function | File:Line | What it does | Dependencies |
|---|----------|-----------|-------------|--------------|
| 1 | `join_datapaths()` | northd.c:804 | Creates `ovn_datapath` for new router, adds to `nb_only` list | None |
| 2 | `build_datapaths()` | northd.c:1000 | Allocates tunnel key, inserts `sbrec_datapath_binding` | Step 1 |
| 3 | `join_logical_ports()` | northd.c:2114 | Creates `ovn_port` for LRP, calls `extract_lrp_networks()` to parse MAC/IPs, allocates `lflow_ref` + `stateful_lflow_ref` | Step 1 |
| 4 | Port peering | northd.c:2306 | Sets `op->peer` bidirectionally between LSP (type=router) and LRP | Step 3 |
| 5 | DGW port handling | northd.c:2390 | If LRP has `ha_chassis_group`: creates `cr-{port}` chassis-redirect port, populates `od->l3dgw_ports[]` | Step 3, 4 |
| 6 | `build_ports()` | northd.c:4155 | Inserts `sbrec_port_binding` for LRP, calls `ovn_port_update_sbrec()` | Steps 2-5 |
| 7 | `build_lb_datapaths()` | northd.c:3635 | Associates load balancers from `nbr->load_balancer[]` with new router datapath | Step 1 |
| 8 | `build_lrouter_groups()` | northd.c:8334 | Creates `lrouter_group`, recursively connects routers via LRP peers | Steps 1, 4 |
| 9 | `build_lflows()` | northd.c:16749 | Generates logical flows for router + ports | Steps 1-8 |

**Flow generation per LRP** (10 builder functions at northd.c:16508-16536):
1. `build_adm_ctrl_flows_for_lrouter_port()` — admission control (~2-4 flows)
2. `build_neigh_learning_flows_for_lrouter_port()` — neighbor learning (~4-8 flows)
3. `build_ip_routing_flows_for_lrp()` — IPv4/IPv6 routing (~2-6 flows per subnet)
4. `build_ND_RA_flows_for_lrouter_port()` — ND/RA (~2-4 flows if IPv6)
5. `build_arp_resolve_flows_for_lrp()` — ARP resolution (~2-6 flows)
6. `build_egress_delivery_flows_for_lrouter_port()` — egress delivery (~2-4 flows)
7. `build_dhcpv6_reply_flows_for_lrouter_port()` — DHCPv6 (~1-2 flows)
8. `build_ipv6_input_flows_for_lrouter_port()` — IPv6 input (~4-8 flows)
9. `build_lrouter_ipv4_ip_input()` — IPv4 input (~4-8 flows)
10. `build_lrouter_icmp_packet_toobig_admin_flows()` — ICMP too-big (~1-2 flows)

**Total per LRP: ~25-55 flows**, plus the switch-side LSP generates ~20-50 flows via `build_lswitch_and_lrouter_iterate_by_lsp()`.

**Critical complications for incremental handling:**

1. **No separate LRP engine node** — LRP changes are tracked via `nb_logical_router` node (inc-proc-northd.c:59). There is NO `nb_logical_router_port` input node. This means LRP creation is invisible to the engine unless the parent router is also tracked.

2. **Router-type LSPs cannot be incrementally processed** — `lsp_can_be_inc_processed()` (northd.c:4366) returns `false` for any LSP with a non-empty `type` field, including `type="router"`. This means `ls_handle_lsp_changes()` falls back to recompute when a router-type port is added.

3. **Port peering requires both sides** — The LRP and its peer LSP must both exist before peering can be established. In a single transaction, both are created atomically, but the incremental handler must process them in the right order.

4. **Distributed gateway ports** — If the LRP has `ha_chassis_group` or `gateway_chassis`, a derived `cr-{port}` must be created (`create_cr_port()` at northd.c:2064), which adds to `od->l3dgw_ports[]` and requires additional flow generation.

5. **Router groups** — `build_lrouter_groups()` recursively connects routers via peer LRPs. A new router joining an existing group changes the group membership, potentially affecting gateway routing decisions for ALL routers in the group.

6. **Load balancer association** — If the router inherits LBs from LB groups, the `ovn_lb_datapaths` structures must be updated.

### Incremental Strategy for Router-with-Ports

Given the complexity above, we adopt a **tiered approach**:

**Tier 1 (Phase C, initial)**: Handle standalone router creation (no ports, no NATs, no LBs in same transaction)
- Just create `ovn_datapath`, assign tunnel key, insert SB `Datapath_Binding`
- Generate base router flows (admission control, defaults)
- Fall back to recompute if ports/NATs/LBs are in same transaction

**Tier 2 (Phase C, extended)**: Handle router + ports in same transaction
- Create `ovn_datapath` AND `ovn_port` structures
- Establish peering with new/existing switch ports
- Generate per-LRP flows using the 10 builder functions
- Generate per-LSP flows for the switch side
- Sync port bindings to SB
- Fall back to recompute for DGW ports or router group changes

**Tier 3 (future)**: Handle router + ports + NATs + LBs + policies
- Full incremental support for all router sub-objects
- Requires incremental `build_lrouter_groups()` (complex cross-datapath)

**Detection logic** in `northd_handle_lr_changes()`:
```c
if (nbrec_logical_router_is_new(changed_lr)) {
    /* Tier 1: Standalone router (no ports in same txn) */
    if (changed_lr->n_ports == 0 && changed_lr->n_nat == 0
        && changed_lr->n_load_balancer == 0
        && changed_lr->n_policies == 0
        && changed_lr->n_static_routes == 0) {
        /* Simple case: create datapath + base flows only */
        od = northd_lr_datapath_create(nd, changed_lr);
        if (od) {
            hmapx_add(&nd->trk_data.trk_created_lrs, od);
            continue;
        }
    }
    /* Complex case: fall back to full recompute */
    goto fail;
}
```

---

### Sub-Phase Architecture

Phase C is broken into sub-phases that build on each other:

```
C.1  Standalone router creation/deletion (Tier 1)
      │ Adds: NORTHD_TRACKED_LR_CREATED/DELETED, base flow generation
      │
C.2  Engine nodes for router sub-tables
      │ Adds: nb_logical_router_port, nb_static_route, nb_lr_policy engine nodes
      │ Enables: Independent change detection for sub-objects
      │
C.3  Parent lookup via IDL arc traversal (NO custom index needed)
      │ Uses: Existing IDL dst_arcs (backward reference arcs)
      │ Enables: Mapping any LRP/NAT/route change back to its parent router
      │
C.4  Incremental LRP handling on existing routers
      │ Adds: Per-LRP create/update/delete with lflow_ref tracking
      │ Extends: lr_changes_can_be_handled() to accept COL_PORTS changes
      │
C.5  Incremental static route handling
      │ Adds: Per-route create/update/delete with targeted flow rebuild
      │ Extends: lr_changes_can_be_handled() to accept COL_STATIC_ROUTES
      │
C.6  Router creation with ports (Tier 2)
      │ Combines: C.1 datapath creation + C.4 port initialization
      │ Handles: Router + LRP + LSP in single transaction
      │
C.7  Incremental policy handling (future)
      │ Adds: Per-policy create/update/delete
      │ Extends: lr_changes_can_be_handled() to accept COL_POLICIES
```

### Current Tracking Infrastructure (What We Build On)

The IDL already provides table-level tracking for all sub-tables:

| Sub-table | Track Macro | Column Tracking | Engine Node |
|-----------|-------------|-----------------|-------------|
| `Logical_Router_Port` | `NBREC_LOGICAL_ROUTER_PORT_TABLE_FOR_EACH_TRACKED` | `nbrec_logical_router_port_is_updated(lrp, col)` | **Missing** |
| `NAT` | `NBREC_NAT_TABLE_FOR_EACH_TRACKED` | `nbrec_nat_is_updated(nat, col)` | **Missing** |
| `Logical_Router_Static_Route` | `NBREC_LOGICAL_ROUTER_STATIC_ROUTE_TABLE_FOR_EACH_TRACKED` | `nbrec_logical_router_static_route_is_updated(rt, col)` | **Missing** |
| `Logical_Router_Policy` | `NBREC_LOGICAL_ROUTER_POLICY_FOR_EACH_TRACKED` | `nbrec_logical_router_policy_is_updated(pol, col)` | **Missing** |
| `ACL` | `NBREC_ACL_TABLE_FOR_EACH_TRACKED` | `nbrec_acl_is_updated(acl, col)` | **Exists** (`nb_acl`) |
| `Load_Balancer` | `NBREC_LOAD_BALANCER_TABLE_FOR_EACH_TRACKED` | `nbrec_load_balancer_is_updated(lb, col)` | **Exists** (`nb_load_balancer`) |

**Parent lookup — solved by IDL arcs**: Sub-objects are stored as strong UUID references in the parent row (`lr->ports[]`, `lr->nat[]`, etc.) with no explicit back-reference column in the schema. However, the **OVS IDL maintains backward arcs automatically** (`ovsdb_idl_row.dst_arcs` at `lib/ovsdb-idl-provider.h:74`). When a `Logical_Router` row references a `Logical_Router_Port`, the IDL creates an arc from router→port. Traversing `lrp_row->dst_arcs` finds the parent router in O(1) — no custom reverse index needed.

Additionally, the custom OVS storage layer provides complementary lookup infrastructure:
- **Bloom filter** (`ovsdb/bloom-filter.h`): Fast negative UUID existence check (~0.82% FPR, 10 bits/key, 7 hash functions, thread-safe via atomics)
- **Clustered index** (`ovsdb/disk-store.h`): UUID→(offset, length) hmap for O(1) disk row lookup
- **HASH indexes** (`ovsdb/index-engine.h`): Column value→row mapping for server-side queries
- **Server-side dst_refs** (`ovsdb/row.h`): Weak reference tracking with `src_table`, `src_uuid`, `column_idx`

**Existing pattern to follow**: ACLs use a separate engine node (`nb_acl`) but the northd handler detects ACL changes by checking `nbrec_acl_row_get_seqno()` on each referenced ACL and records affected datapaths in `ls_with_changed_acls`. We extend this pattern with per-object granularity and IDL arc-based parent lookup.

---

### Sub-Phase C.1: Standalone Router Creation/Deletion (Tier 1)

#### Step C1.1: New tracked data types

**File**: `northd/northd.h` (around line 120)

Add to `enum northd_tracked_data_type`:
```c
NORTHD_TRACKED_LR_CREATED  = (1 << 5),  /* New routers created */
NORTHD_TRACKED_LR_DELETED  = (1 << 6),  /* Routers deleted */
```

Add to `struct northd_tracked_data`:
```c
struct hmapx trk_created_lrs;   /* hmapx node data is 'struct ovn_datapath *' */
struct hmapx trk_deleted_lrs;   /* hmapx node data is 'struct ovn_datapath *' */
```

### Step C1.2: Incremental router creation handler

**File**: `northd/northd.c` (modify `northd_handle_lr_changes()` at line 4957)

Instead of `goto fail` on `is_new()`, handle creation incrementally:

```c
if (nbrec_logical_router_is_new(changed_lr)) {
    /* Create new datapath for this router. */
    struct ovn_datapath *od = northd_lr_datapath_create(nd, changed_lr);
    if (!od) {
        goto fail;
    }
    hmapx_add(&nd->trk_data.trk_created_lrs, od);
    continue;
}

if (nbrec_logical_router_is_deleted(changed_lr)) {
    struct ovn_datapath *od = ovn_datapath_find_(
        &nd->lr_datapaths.datapaths, &changed_lr->header_.uuid);
    if (od) {
        hmapx_add(&nd->trk_data.trk_deleted_lrs, od);
    }
    continue;
}
```

New helper function `northd_lr_datapath_create()`:
```c
static struct ovn_datapath *
northd_lr_datapath_create(struct northd_data *nd,
                          const struct nbrec_logical_router *nbr)
{
    struct ovn_datapath *od = ovn_datapath_create(
        &nd->lr_datapaths.datapaths, &nbr->header_.uuid,
        NULL /* nbs */, nbr, NULL /* sb */);

    /* Initialize router datapath fields. */
    init_mcast_info_for_router_datapath(od);

    /* Assign tunnel key. */
    uint32_t key = ovn_allocate_tnlid(&nd->lr_datapaths.tnlids,
                                       "router datapath",
                                       OVN_MIN_DP_TNLID,
                                       OVN_MAX_DP_TNLID,
                                       &nd->lr_datapaths.hint);
    if (!key) {
        return NULL;
    }
    od->tunnel_key = key;

    /* Create SB datapath binding. */
    /* (Will be synced in sync_to_sb_pb handler) */

    return od;
}
```

### Step C1.3: Lflow handler for new routers

**File**: `northd/en-lflow.c` (modify `lflow_northd_handler()` at line 123)

Add handling for `NORTHD_TRACKED_LR_CREATED`:
```c
if (nd_changes->type & NORTHD_TRACKED_LR_CREATED) {
    struct hmapx_node *hmapx_node;
    HMAPX_FOR_EACH (hmapx_node, &nd_changes->trk_created_lrs) {
        struct ovn_datapath *od = hmapx_node->data;
        /* Generate flows only for this new router. */
        build_lswitch_and_lrouter_iterate_by_lr(od, lflow_input, lflows);
        /* Generate per-port flows. */
        struct ovn_port *op;
        HMAP_FOR_EACH (op, dp_node, &od->ports) {
            build_lswitch_and_lrouter_iterate_by_lrp(op, lflow_input, lflows);
        }
        lflow_ref_sync_lflows(od->lflow_ref, ...);
    }
}
```

### Step C1.4: Sync handler for deleted routers

For deletions, the handler needs to:
1. Remove all lflows referencing the deleted datapath via `lflow_ref_unlink_lflows()`
2. Remove SB datapath binding
3. Free the `ovn_datapath` structure

### Step C1.5: Tests for Tier 1 (standalone router creation/deletion)

**File**: `tests/ovn-northd.at` (append near existing incremental tests around line 12240)

Uses established test patterns: `check_engine_stats` (lines 40-84), `CHECK_NO_CHANGE_AFTER_RECOMPUTE` (lines 18-38), `check_row_count` / `wait_row_count` from `ovn-macros.at`.

#### Test 1: Basic router creation — incremental processing

```bash
AT_SETUP([ovn -- incremental processing for LR creation])
AT_KEYWORDS([incremental lr-create])

ovn_start

# Create initial topology to warm up engine
check ovn-nbctl --wait=sb lr-add lr0
check ovn-nbctl --wait=sb ls-add ls0
check ovn-nbctl --wait=sb \
    lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24
check ovn-nbctl --wait=sb \
    lsp-add ls0 lsp0_rp -- lsp-set-type lsp0_rp router \
    -- lsp-set-addresses lsp0_rp router \
    -- lsp-set-options lsp0_rp router-port=rp0

# Baseline: 1 router datapath + 1 switch datapath in SB
check_row_count Datapath_Binding 2

# ---- Test: Add a standalone router ----
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats

check ovn-nbctl --wait=sb lr-add lr1

# Verify northd used incremental handler (norecompute)
check_engine_stats northd norecompute compute
check_engine_stats lflow norecompute compute
check_engine_stats lr_nat norecompute compute
check_engine_stats lr_stateful norecompute compute
check_engine_stats sync_to_sb_pb norecompute compute

# Verify SB state: new datapath binding exists
check_row_count Datapath_Binding 3

# Verify the router's default flows were generated
AT_CHECK([ovn-sbctl dump-flows lr1 | wc -l | tr -d ' '], [0], [dnl
$(ovn-sbctl dump-flows lr1 | wc -l | tr -d ' ')
])
# Ensure at least the admission control + drop flows exist
AT_CHECK([ovn-sbctl dump-flows lr1 | grep -c lr_in_admission], [0], [dnl
$(ovn-sbctl dump-flows lr1 | grep -c lr_in_admission)
])

# Verify full recompute produces identical SB state
CHECK_NO_CHANGE_AFTER_RECOMPUTE

OVN_CLEANUP([])
AT_CLEANUP
```

#### Test 2: Router deletion — incremental cleanup

```bash
AT_SETUP([ovn -- incremental processing for LR deletion])
AT_KEYWORDS([incremental lr-delete])

ovn_start

# Create two routers
check ovn-nbctl --wait=sb lr-add lr0
check ovn-nbctl --wait=sb lr-add lr1
check ovn-nbctl --wait=sb ls-add ls0
check ovn-nbctl --wait=sb \
    lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24
check ovn-nbctl --wait=sb \
    lsp-add ls0 lsp0_rp -- lsp-set-type lsp0_rp router \
    -- lsp-set-addresses lsp0_rp router \
    -- lsp-set-options lsp0_rp router-port=rp0

check_row_count Datapath_Binding 3

# ---- Test: Delete lr1 ----
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats

check ovn-nbctl --wait=sb lr-del lr1

# Verify northd used incremental handler
check_engine_stats northd norecompute compute
check_engine_stats lflow norecompute compute

# Verify SB: datapath removed, only 2 remain
check_row_count Datapath_Binding 2

# Verify lr1's flows are gone
AT_CHECK([ovn-sbctl dump-flows lr1 2>&1], [0], [
])

# Verify lr0's flows are intact
AT_CHECK([ovn-sbctl dump-flows lr0 | grep -c lr_in_admission | tr -d ' '], [0], [dnl
$(ovn-sbctl dump-flows lr0 | grep -c lr_in_admission | tr -d ' ')
])

# Verify full recompute produces identical SB state
CHECK_NO_CHANGE_AFTER_RECOMPUTE

OVN_CLEANUP([])
AT_CLEANUP
```

#### Test 3: Router + ports in same transaction — Tier 1 fallback to recompute

Tier 1 only handles standalone routers. When ports are created in the same
transaction, the handler detects `n_ports > 0` and falls back to full
recompute. This test verifies the fallback produces correct SB state.

```bash
AT_SETUP([ovn -- LR creation with ports falls back to recompute (Tier 1)])
AT_KEYWORDS([incremental lr-create-ports])

ovn_start

# Create initial topology
check ovn-nbctl --wait=sb lr-add lr0
check ovn-nbctl --wait=sb ls-add ls0
check ovn-nbctl --wait=sb \
    lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24
check ovn-nbctl --wait=sb \
    lsp-add ls0 lsp0_rp -- lsp-set-type lsp0_rp router \
    -- lsp-set-addresses lsp0_rp router \
    -- lsp-set-options lsp0_rp router-port=rp0

# ---- Test: Add router + port + switch in single transaction ----
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats

check ovn-nbctl --wait=sb \
    lr-add lr1 \
    -- ls-add ls1 \
    -- lrp-add lr1 rp1 00:00:00:00:00:02 10.1.0.1/24 \
    -- lsp-add ls1 lsp1_rp -- lsp-set-type lsp1_rp router \
    -- lsp-set-addresses lsp1_rp router \
    -- lsp-set-options lsp1_rp router-port=rp1

# Tier 1: router+ports in same txn falls back to recompute
check_engine_stats northd recompute nocompute
check_engine_stats lflow recompute nocompute

# But SB state is still correct:
check_row_count Datapath_Binding 4

# Verify port binding for the router port exists
wait_row_count Port_Binding 1 logical_port=rp1

# Verify router port flows exist
AT_CHECK([ovn-sbctl dump-flows lr1 | grep -c lr_in_ip_input], [0], [dnl
$(ovn-sbctl dump-flows lr1 | grep -c lr_in_ip_input)
])

# Verify full recompute produces identical SB state
CHECK_NO_CHANGE_AFTER_RECOMPUTE

OVN_CLEANUP([])
AT_CLEANUP
```

#### Test 3b: Router created first, then ports added separately — incremental

This tests the two-step pattern: create router (incremental), then add ports
(currently triggers recompute via `lr_changes_can_be_handled()`, but port
addition to existing switch can be incremental on the switch side).

```bash
AT_SETUP([ovn -- LR created then ports added separately])
AT_KEYWORDS([incremental lr-then-ports])

ovn_start

# Create initial topology
check ovn-nbctl --wait=sb lr-add lr0
check ovn-nbctl --wait=sb ls-add ls0
check ovn-nbctl --wait=sb \
    lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24
check ovn-nbctl --wait=sb \
    lsp-add ls0 lsp0_rp -- lsp-set-type lsp0_rp router \
    -- lsp-set-addresses lsp0_rp router \
    -- lsp-set-options lsp0_rp router-port=rp0

# ---- Step 1: Create standalone router (Tier 1 incremental) ----
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check ovn-nbctl --wait=sb lr-add lr1

check_engine_stats northd norecompute compute
check_row_count Datapath_Binding 3

# ---- Step 2: Add port to existing router ----
# This modifies lr1's ports column, which is NOT in lr_changes_can_be_handled()
# so it triggers recompute
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check ovn-nbctl --wait=sb \
    lrp-add lr1 rp1 00:00:00:00:00:02 10.1.0.1/24

check_engine_stats northd recompute nocompute

# ---- Step 3: Create switch and connect to router port ----
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check ovn-nbctl --wait=sb ls-add ls1
check ovn-nbctl --wait=sb \
    lsp-add ls1 lsp1_rp -- lsp-set-type lsp1_rp router \
    -- lsp-set-addresses lsp1_rp router \
    -- lsp-set-options lsp1_rp router-port=rp1

# Verify final SB state
check_row_count Datapath_Binding 4
wait_row_count Port_Binding 1 logical_port=rp1

# Verify all flows are correct
AT_CHECK([ovn-sbctl dump-flows lr1 | grep -c lr_in_ip_input], [0], [dnl
$(ovn-sbctl dump-flows lr1 | grep -c lr_in_ip_input)
])

CHECK_NO_CHANGE_AFTER_RECOMPUTE

OVN_CLEANUP([])
AT_CLEANUP
```

#### Test 3c: Distributed gateway port — recompute fallback

DGW ports (with `ha_chassis_group`) create derived `cr-{port}` chassis-redirect
ports and modify `od->l3dgw_ports[]`. This is too complex for Tier 1/2 and
must fall back to recompute.

```bash
AT_SETUP([ovn -- LR with distributed gateway port falls back to recompute])
AT_KEYWORDS([incremental lr-dgw-fallback])

ovn_start

# Create HA chassis group
check ovn-nbctl ha-chassis-group-add hagrp1
check ovn-nbctl --wait=sb ha-chassis-group-add-chassis hagrp1 ch1 10

# Create initial router
check ovn-nbctl --wait=sb lr-add lr0
check ovn-nbctl --wait=sb ls-add ls0

# ---- Test: Create router with distributed gateway port ----
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats

check ovn-nbctl --wait=sb lr-add lr1
check ovn-nbctl --wait=sb \
    lrp-add lr1 rp1 00:00:00:00:00:02 10.1.0.1/24 \
    -- lrp-set-gateway-chassis rp1 ch1

# DGW port forces recompute
check_engine_stats northd recompute nocompute

# Verify cr- port exists
wait_row_count Port_Binding 1 logical_port=cr-rp1

CHECK_NO_CHANGE_AFTER_RECOMPUTE

OVN_CLEANUP([])
AT_CLEANUP
```

#### Test 4: Router creation with NAT — flow correctness

```bash
AT_SETUP([ovn -- incremental processing for LR creation with NAT])
AT_KEYWORDS([incremental lr-create-nat])

ovn_start

# Create initial router with NAT for baseline comparison
check ovn-nbctl --wait=sb lr-add lr0
check ovn-nbctl --wait=sb \
    lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24
check ovn-nbctl --wait=sb \
    lr-nat-add lr0 dnat_and_snat 172.16.0.1 10.0.0.11

# Record baseline NAT flow count for lr0
lr0_nat_flows=$(ovn-sbctl dump-flows lr0 | grep -c "lr_in_dnat\|lr_out_snat\|lr_in_unsnat\|lr_out_undnat")

# ---- Test: Add new router with NAT ----
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats

check ovn-nbctl --wait=sb lr-add lr1
check ovn-nbctl --wait=sb \
    lrp-add lr1 rp1 00:00:00:00:00:02 10.1.0.1/24
check ovn-nbctl --wait=sb \
    lr-nat-add lr1 dnat_and_snat 172.16.1.1 10.1.0.11

# Verify lr1 has NAT flows
lr1_nat_flows=$(ovn-sbctl dump-flows lr1 | grep -c "lr_in_dnat\|lr_out_snat\|lr_in_unsnat\|lr_out_undnat")
AT_CHECK([test $lr1_nat_flows -ge 2], [0])

# Verify lr0's NAT flows are unchanged
lr0_nat_flows_after=$(ovn-sbctl dump-flows lr0 | grep -c "lr_in_dnat\|lr_out_snat\|lr_in_unsnat\|lr_out_undnat")
AT_CHECK([test $lr0_nat_flows_after -eq $lr0_nat_flows], [0])

# Verify full recompute produces identical SB state
CHECK_NO_CHANGE_AFTER_RECOMPUTE

OVN_CLEANUP([])
AT_CLEANUP
```

#### Test 5: Multiple router operations — incremental consistency

```bash
AT_SETUP([ovn -- incremental processing for multiple LR operations])
AT_KEYWORDS([incremental lr-multi])

ovn_start

# Create initial topology with 3 routers
for i in $(seq 0 2); do
    check ovn-nbctl --wait=sb lr-add lr$i
    check ovn-nbctl --wait=sb ls-add ls$i
    check ovn-nbctl --wait=sb \
        lrp-add lr$i rp$i 00:00:00:00:0$i:01 10.$i.0.1/24
    check ovn-nbctl --wait=sb \
        lsp-add ls$i lsp${i}_rp -- lsp-set-type lsp${i}_rp router \
        -- lsp-set-addresses lsp${i}_rp router \
        -- lsp-set-options lsp${i}_rp router-port=rp$i
done

check_row_count Datapath_Binding 6

# ---- Test: Add 2 routers, delete 1 in separate transactions ----

# Add lr3
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check ovn-nbctl --wait=sb lr-add lr3
check_engine_stats northd norecompute compute
check_row_count Datapath_Binding 7

# Add lr4
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check ovn-nbctl --wait=sb lr-add lr4
check_engine_stats northd norecompute compute
check_row_count Datapath_Binding 8

# Delete lr2
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check ovn-nbctl --wait=sb lr-del lr2
check_engine_stats northd norecompute compute
check_row_count Datapath_Binding 7

# Verify remaining routers have correct flows
for r in lr0 lr1 lr3 lr4; do
    AT_CHECK([ovn-sbctl dump-flows $r | grep -c lr_in_admission], [0], [dnl
$(ovn-sbctl dump-flows $r | grep -c lr_in_admission)
])
done

# Verify lr2 flows are gone
AT_CHECK([ovn-sbctl dump-flows lr2 2>&1], [0], [
])

# Verify full recompute produces identical SB state
CHECK_NO_CHANGE_AFTER_RECOMPUTE

OVN_CLEANUP([])
AT_CLEANUP
```

#### Test 6: Fallback to full recompute — complex router changes

```bash
AT_SETUP([ovn -- incremental LR fallback to recompute])
AT_KEYWORDS([incremental lr-fallback])

ovn_start

check ovn-nbctl --wait=sb lr-add lr0
check ovn-nbctl --wait=sb ls-add ls0
check ovn-nbctl --wait=sb \
    lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24
check ovn-nbctl --wait=sb \
    lsp-add ls0 lsp0_rp -- lsp-set-type lsp0_rp router \
    -- lsp-set-addresses lsp0_rp router \
    -- lsp-set-options lsp0_rp router-port=rp0

# ---- Test: Changes that should still trigger recompute ----

# Modifying static routes on existing router (not yet handled incrementally)
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check ovn-nbctl --wait=sb lr-route-add lr0 192.168.0.0/16 10.0.0.254
check_engine_stats northd recompute nocompute

# Modifying router policies (not yet handled incrementally)
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats
check ovn-nbctl --wait=sb lr-policy-add lr0 100 "ip4.src == 10.0.0.0/24" allow
check_engine_stats northd recompute nocompute

# Verify SB state is still correct after recomputes
CHECK_NO_CHANGE_AFTER_RECOMPUTE

OVN_CLEANUP([])
AT_CLEANUP
```

---

### Sub-Phase C.2: Engine Nodes for Router Sub-Tables

Add independent engine nodes for router sub-tables so the IPE can detect their changes without relying solely on the parent `nb_logical_router` node.

#### Step C2.1: Register new engine nodes

**File**: `northd/inc-proc-northd.c` (line 51-64, NB_NODES macro)

```c
#define NB_NODES \
    ...
    NB_NODE(logical_router, "logical_router") \
    NB_NODE(logical_router_port, "logical_router_port") \
    NB_NODE(logical_router_static_route, "logical_router_static_route") \
    NB_NODE(logical_router_policy, "logical_router_policy") \
    NB_NODE(nat, "nat") \
    ...
```

This auto-generates via `ENGINE_FUNC_NB()`:
- `en_nb_logical_router_port_run()` — calls `nbrec_logical_router_port_table_track_get_first()`
- `en_nb_nat_run()` — calls `nbrec_nat_table_track_get_first()`
- etc.

#### Step C2.2: Wire into the engine DAG

**File**: `northd/inc-proc-northd.c` (after line 215)

```c
/* Router sub-object change detection.
 * These nodes detect changes to LRP/NAT/route/policy tables independently.
 * The northd handler consumes them to route updates to specific routers. */
engine_add_input(&en_northd, &en_nb_logical_router_port,
                 northd_nb_logical_router_port_handler);
engine_add_input(&en_northd, &en_nb_logical_router_static_route,
                 northd_nb_static_route_handler);
engine_add_input(&en_northd, &en_nb_logical_router_policy,
                 northd_nb_lr_policy_handler);
engine_add_input(&en_northd, &en_nb_nat,
                 northd_nb_nat_handler);
```

**Why separate from `en_nb_logical_router`?** When only a LRP changes (e.g., MAC address update), the `en_nb_logical_router` node may or may not fire (depends on whether the parent row's `ports` column changed). With a dedicated node, the change is always detected.

---

### Sub-Phase C.3: Parent Lookup via IDL Arc Traversal (No Custom Index Needed)

**Original assumption**: Sub-objects have no back-reference to their parent router, requiring a custom reverse index.

**Discovery**: The OVS IDL already maintains **backward reference arcs** (`dst_arcs`) on every row, automatically tracking which rows reference a given row. Combined with the custom OVS storage enhancements (Bloom filter, UUID→offset clustered index), we have a complete lookup chain without building anything new.

#### IDL Arc Architecture

Every `ovsdb_idl_row` (`lib/ovsdb-idl-provider.h:73-74`) has:
```c
struct ovsdb_idl_row {
    struct ovs_list src_arcs;   /* Forward arcs: rows I reference */
    struct ovs_list dst_arcs;   /* Backward arcs: rows that reference ME */
};

struct ovsdb_idl_arc {
    struct ovs_list src_node;   /* In src->src_arcs list */
    struct ovs_list dst_node;   /* In dst->dst_arcs list */
    struct ovsdb_idl_row *src;  /* Row that holds the reference */
    struct ovsdb_idl_row *dst;  /* Row being referenced */
};
```

When a `Logical_Router` row references a `Logical_Router_Port` via its `ports` column, the IDL automatically creates an arc: `arc->src = lr_row`, `arc->dst = lrp_row`. This arc is maintained by `ovsdb_idl_get_row_arc()` at `lib/ovsdb-idl.c:2633`.

#### Parent Lookup: Zero-Cost, Zero-Maintenance

To find the parent router of an LRP:
```c
static const struct nbrec_logical_router *
lrp_find_parent_lr(const struct nbrec_logical_router_port *lrp)
{
    const struct ovsdb_idl_row *lrp_row = &lrp->header_;
    const struct ovsdb_idl_arc *arc;

    LIST_FOR_EACH (arc, dst_node, &lrp_row->dst_arcs) {
        if (arc->src->table->class_ == &nbrec_table_logical_router) {
            return nbrec_logical_router_cast(arc->src);
        }
    }
    return NULL;  /* Orphaned LRP (shouldn't happen with strong refs) */
}
```

**Properties**:
- **Zero allocation**: No custom index to build or maintain
- **Always consistent**: Arcs are maintained by the IDL on every update cycle
- **O(1) for single-parent**: LRPs have exactly one parent router (strong ref), so the loop exits on first match
- **Works for all sub-objects**: NATs, static routes, and policies all have the same pattern

#### Lookup Chain with Storage Enhancements

For server-side operations (not typically needed by northd, but available):

```
UUID of sub-object
    ↓ Bloom filter check (ovsdb/bloom-filter.h)
    │ Fast negative: definitely not in table → skip
    │ Maybe positive: proceed to...
    ↓
    UUID→offset clustered index (ovsdb/disk-store.h)
    │ struct disk_store_index_entry: uuid, offset, length, table_name
    │ O(1) hmap lookup → byte offset on disk
    ↓
    Disk read at offset → row data
    ↓
    Server-side dst_refs (ovsdb/row.h:64-77)
    │ struct ovsdb_row.dst_refs → hmap of ovsdb_weak_ref
    │ Each ref contains: src_table, src_uuid, column_idx
    ↓
    Parent row identified
```

#### Why This Eliminates Sub-Phase C.3's Custom Index

| Approach | Build Cost | Maintenance | Memory | Lookup |
|----------|-----------|-------------|--------|--------|
| Custom `lr_sub_object_index` | O(N) full scan | Manual on every add/delete | 4 hmaps × N entries | O(1) hmap |
| IDL `dst_arcs` traversal | **Zero** (automatic) | **Zero** (IDL maintains) | **Zero** (arcs already exist) | O(1) single-parent |

The IDL arcs are strictly superior: no code to write, no bugs to introduce, no memory to manage.

#### Updated Handler Using IDL Arcs

```c
bool
northd_nb_logical_router_port_handler(struct engine_node *node, void *data)
{
    struct northd_data *nd = data;
    const struct nbrec_logical_router_port *changed_lrp;

    NBREC_LOGICAL_ROUTER_PORT_TABLE_FOR_EACH_TRACKED(changed_lrp,
                                                      lrp_table) {
        /* Find parent router via IDL backward arc — no custom index. */
        const struct nbrec_logical_router *nbr =
            lrp_find_parent_lr(changed_lrp);

        if (!nbr) {
            /* Orphaned LRP or parent is also new — handled by C.1. */
            continue;
        }

        struct ovn_datapath *od = ovn_datapath_find_(
            &nd->lr_datapaths.datapaths, &nbr->header_.uuid);
        if (!od) {
            /* Parent router not yet processed — fall back. */
            return false;
        }

        if (nbrec_logical_router_port_is_new(changed_lrp)) {
            if (!northd_handle_lrp_create(nd, od, changed_lrp)) {
                return false;
            }
        } else if (nbrec_logical_router_port_is_deleted(changed_lrp)) {
            if (!northd_handle_lrp_delete(nd, od, changed_lrp)) {
                return false;
            }
        } else {
            if (!northd_handle_lrp_update(nd, od, changed_lrp)) {
                return false;
            }
        }
    }

    if (northd_has_tracked_data(&nd->trk_data)) {
        engine_set_node_state(node, EN_UPDATED);
    }
    return true;
}
```

---

### Sub-Phase C.4: Incremental LRP Handling on Existing Routers

#### Step C4.1: Extend `lr_changes_can_be_handled()`

**File**: `northd/northd.c` (line 4880)

Add `NBREC_LOGICAL_ROUTER_COL_PORTS` to the allowed columns:
```c
if (col == NBREC_LOGICAL_ROUTER_COL_LOAD_BALANCER
    || col == NBREC_LOGICAL_ROUTER_COL_LOAD_BALANCER_GROUP
    || col == NBREC_LOGICAL_ROUTER_COL_NAT
    || col == NBREC_LOGICAL_ROUTER_COL_PORTS) {  /* NEW */
    continue;
}
```

And remove the blanket LRP seqno check at lines 4903-4908 (replaced by per-LRP handling in C.3).

#### Step C4.2: New tracked data for LRP changes

**File**: `northd/northd.h`

```c
/* Tracked router port changes — mirrors tracked_ovn_ports for switch ports. */
struct tracked_lr_ports {
    struct hmapx created;       /* ovn_port* for new LRPs */
    struct hmapx updated;       /* ovn_port* for modified LRPs */
    struct hmapx deleted;       /* ovn_port* for removed LRPs */
};
```

Add to `enum northd_tracked_data_type`:
```c
NORTHD_TRACKED_LR_PORTS     = (1 << 7),  /* Router port changes */
NORTHD_TRACKED_LR_ROUTES    = (1 << 8),  /* Static route changes */
NORTHD_TRACKED_LR_POLICIES  = (1 << 9),  /* Policy changes */
```

Add to `struct northd_tracked_data`:
```c
struct tracked_lr_ports trk_lrps;       /* Router port changes */
struct hmapx lr_with_changed_routes;    /* Routers with route changes */
struct hmapx lr_with_changed_policies;  /* Routers with policy changes */
```

#### Step C4.3: LRP create handler

**File**: `northd/northd.c`

```c
static bool
northd_handle_lrp_create(struct northd_data *nd,
                         struct ovn_datapath *od,
                         const struct nbrec_logical_router_port *nbrp)
{
    /* Reject DGW ports — too complex for now. */
    if (nbrp->ha_chassis_group || nbrp->n_gateway_chassis) {
        return false;
    }

    /* Parse networks. */
    struct lport_addresses lrp_networks;
    if (!extract_lrp_networks(nbrp, &lrp_networks)) {
        return false;
    }

    /* Create ovn_port. */
    struct ovn_port *op = ovn_port_create(
        &nd->lr_ports, nbrp->name, NULL, nbrp, NULL);
    op->od = od;
    op->lrp_networks = lrp_networks;
    hmap_insert(&od->ports, &op->dp_node, hash_string(op->key, 0));

    /* Allocate tunnel key. */
    if (!ovn_port_allocate_key(od, op)) {
        return false;
    }

    /* Insert SB port_binding. */
    op->sb = sbrec_port_binding_insert(nd->ovnsb_txn);
    sbrec_port_binding_set_logical_port(op->sb, op->key);
    ovn_port_update_sbrec(..., op, ...);

    /* Update reverse index. */
    lr_sub_object_index_add(&nd->sub_index.lrp_to_lr,
                            &nbrp->header_.uuid, od);

    /* Track for downstream lflow generation. */
    hmapx_add(&nd->trk_data.trk_lrps.created, op);
    nd->trk_data.type |= NORTHD_TRACKED_LR_PORTS;

    return true;
}
```

#### Step C4.4: Lflow handler for LRP changes

**File**: `northd/en-lflow.c` (extend `lflow_northd_handler()`)

```c
if (nd_changes->type & NORTHD_TRACKED_LR_PORTS) {
    if (!lflow_handle_northd_lr_port_changes(
            ovnsb_txn, &nd_changes->trk_lrps,
            lflow_input, lflows)) {
        return false;
    }
}
```

**File**: `northd/northd.c` (new function)

```c
bool
lflow_handle_northd_lr_port_changes(struct ovsdb_idl_txn *ovnsb_txn,
                                    struct tracked_lr_ports *trk_lrps,
                                    struct lflow_input *lflow_input,
                                    struct lflow_table *lflows)
{
    /* Process deleted LRPs. */
    HMAPX_FOR_EACH(node, &trk_lrps->deleted) {
        struct ovn_port *op = node->data;
        if (!lflow_ref_resync_flows(op->lflow_ref, ...)) {
            return false;
        }
    }

    /* Process updated LRPs. */
    HMAPX_FOR_EACH(node, &trk_lrps->updated) {
        struct ovn_port *op = node->data;
        lflow_ref_unlink_lflows(op->lflow_ref);
        build_lswitch_and_lrouter_iterate_by_lrp(op, ...);
        if (!lflow_ref_sync_lflows(op->lflow_ref, ...)) {
            return false;
        }
    }

    /* Process created LRPs. */
    HMAPX_FOR_EACH(node, &trk_lrps->created) {
        struct ovn_port *op = node->data;
        build_lswitch_and_lrouter_iterate_by_lrp(op, ...);
        if (!lflow_ref_sync_lflows(op->lflow_ref, ...)) {
            return false;
        }
    }

    return true;
}
```

#### Step C4.5: Peer port handling

When an LRP is created, the switch-side peer LSP (type="router") may already exist or may be created in the same transaction. The handler needs to:

1. Look up the peer LSP: `ovn_port_get_peer()` using `lsp-options:router-port`
2. Set `op->peer` bidirectionally
3. Regenerate peer LSP flows (since they reference the LRP's networks)

```c
/* In northd_handle_lrp_create(), after port creation: */
struct ovn_port *peer = ovn_port_find(&nd->ls_ports, nbrp->peer);
if (peer) {
    op->peer = peer;
    peer->peer = op;
    /* Mark peer as updated so its lflows are regenerated. */
    hmapx_add(&nd->trk_data.trk_lsps.updated, peer);
    nd->trk_data.type |= NORTHD_TRACKED_PORTS;
}
```

---

### Sub-Phase C.5: Incremental Static Route Handling

#### Step C5.1: Extend `lr_changes_can_be_handled()`

Add `NBREC_LOGICAL_ROUTER_COL_STATIC_ROUTES` to allowed columns.

Remove the blanket seqno check at lines 4919-4924.

#### Step C5.2: Static route handler

**File**: `northd/northd.c`

```c
static bool
northd_handle_static_route_changes(struct northd_data *nd,
                                   struct ovn_datapath *od,
                                   const struct nbrec_logical_router *nbr)
{
    /* Check if static routes changed. */
    if (!nbrec_logical_router_is_updated(nbr,
            NBREC_LOGICAL_ROUTER_COL_STATIC_ROUTES)) {
        return true;  /* No route changes. */
    }

    /* For now, mark the router for route flow regeneration.
     * Per-route granularity can be added later. */
    hmapx_add(&nd->trk_data.lr_with_changed_routes, od);
    nd->trk_data.type |= NORTHD_TRACKED_LR_ROUTES;

    return true;
}
```

#### Step C5.3: Lflow handler for route changes

In the lflow handler, when `NORTHD_TRACKED_LR_ROUTES` is set:
```c
/* Regenerate routing flows for affected routers. */
HMAPX_FOR_EACH(node, &nd_changes->lr_with_changed_routes) {
    struct ovn_datapath *od = node->data;
    /* Unlink old routing flows for this router. */
    lflow_ref_unlink_lflows(od->route_lflow_ref);
    /* Rebuild: */
    build_static_route_flows_for_lrouter(od, ...);
    build_ip_routing_pre_flows_for_lrouter(od, ...);
    lflow_ref_sync_lflows(od->route_lflow_ref, ...);
}
```

**Note**: This requires adding a `route_lflow_ref` to `struct ovn_datapath` to track which flows belong to routing specifically (separate from the per-port `lflow_ref`).

---

### Sub-Phase C.6: Router Creation with Ports (Tier 2)

This sub-phase combines C.1 (datapath creation) with C.4 (port handling) to handle the common case of router + ports in a single transaction.

#### Step C6.1: Extend detection in `northd_handle_lr_changes()`

```c
if (nbrec_logical_router_is_new(changed_lr)) {
    /* Create datapath (from C.1). */
    struct ovn_datapath *od = northd_lr_datapath_create(nd, changed_lr);
    if (!od) {
        goto fail;
    }
    hmapx_add(&nd->trk_data.trk_created_lrs, od);

    /* Process ports on the new router (from C.4). */
    for (size_t i = 0; i < changed_lr->n_ports; i++) {
        if (!northd_handle_lrp_create(nd, od, changed_lr->ports[i])) {
            goto fail;  /* Fall back for complex ports (DGW, etc.) */
        }
    }

    /* Process NATs on the new router. */
    for (size_t i = 0; i < changed_lr->n_nat; i++) {
        /* Use existing NAT tracking pattern. */
        hmapx_add(&nd->trk_data.trk_nat_lrs, od);
    }

    /* Reject if policies or static routes present (not yet handled). */
    if (changed_lr->n_policies > 0 || changed_lr->n_static_routes > 0) {
        goto fail;
    }

    continue;
}
```

#### Step C6.2: Tests for Tier 2

Tests 3b and 3c from the existing test plan validate Tier 2 behavior. Additional tests:

```bash
AT_SETUP([ovn -- incremental LR creation with ports (Tier 2)])
AT_KEYWORDS([incremental lr-create-tier2])

ovn_start

# Warm up with initial topology
check ovn-nbctl --wait=sb lr-add lr0 -- ls-add ls0
check ovn-nbctl --wait=sb lrp-add lr0 rp0 00:00:00:00:00:01 10.0.0.1/24
check ovn-nbctl --wait=sb \
    lsp-add ls0 lsp0_rp -- lsp-set-type lsp0_rp router \
    -- lsp-set-addresses lsp0_rp router \
    -- lsp-set-options lsp0_rp router-port=rp0

# ---- Test: Create router + port in single transaction ----
check as northd ovn-appctl -t ovn-northd inc-engine/clear-stats

check ovn-nbctl --wait=sb \
    lr-add lr1 \
    -- lrp-add lr1 rp1 00:00:00:00:00:02 10.1.0.1/24

# Tier 2: should be incremental (no DGW, no policies, no static routes)
check_engine_stats northd norecompute compute
check_engine_stats lflow norecompute compute

# Verify SB state
check_row_count Datapath_Binding 3
wait_row_count Port_Binding 1 logical_port=rp1

# Verify per-LRP flows generated
AT_CHECK([ovn-sbctl dump-flows lr1 | grep -c lr_in_ip_input], [0], [dnl
$(ovn-sbctl dump-flows lr1 | grep -c lr_in_ip_input)
])

CHECK_NO_CHANGE_AFTER_RECOMPUTE

OVN_CLEANUP([])
AT_CLEANUP
```

---

### Sub-Phase C.7: Incremental Policy Handling (Future)

Similar to static routes but for `Logical_Router_Policy` objects. Adds `NBREC_LOGICAL_ROUTER_COL_POLICIES` to allowed columns and builds policy-specific flows incrementally. Deferred until Phases C.1-C.6 are stable.

---

### Test Coverage Summary

| Test | Scenario | Expected Engine Behavior | Sub-Phase |
|------|----------|-------------------------|-----------|
| 1 | Standalone router (no ports) | `northd norecompute compute` | C.1 |
| 2 | Router deletion | `northd norecompute compute` | C.1 |
| 3 | Router + ports in single txn | Tier 1: `recompute`; Tier 2: `norecompute` | C.1→C.6 |
| 3b | Router first, then ports later | Router: `norecompute`; port add: `norecompute` (C.4) | C.1 + C.4 |
| 3c | Router with DGW port | `northd recompute` (fallback) | Always fallback |
| 4 | Router + NAT (separate txns) | NAT add: `norecompute compute` | C.1 + existing |
| 5 | Multiple add/delete operations | Each: `norecompute compute` | C.1 |
| 6 | Static routes, policies | `northd norecompute compute` (C.5/C.7) | C.5, C.7 |
| C.2 | LRP modify on existing router | `northd norecompute compute` | C.4 |
| C.6 | Router + ports Tier 2 | `northd norecompute compute` | C.6 |

All tests verify SB correctness with `CHECK_NO_CHANGE_AFTER_RECOMPUTE`.

### Files to Modify (Phase C, All Sub-Phases)

| File | Sub-Phase | Change |
|------|-----------|--------|
| `northd/inc-proc-northd.c` | C.2 | Add `nb_logical_router_port`, `nb_nat`, `nb_logical_router_static_route`, `nb_logical_router_policy` to NB_NODES; wire handlers into DAG |
| `northd/northd.h` | C.1, C.4 | `NORTHD_TRACKED_LR_*` enums; `tracked_lr_ports` struct; `lrp_find_parent_lr()` helper |
| `northd/northd.c` | C.1-C.6 | `northd_handle_lr_changes()` extension; `northd_handle_lrp_create/update/delete()`; `northd_handle_static_route_changes()`; `lflow_handle_northd_lr_port_changes()` — all using IDL `dst_arcs` for parent lookup |
| `northd/en-northd.c` | C.2, C.4 | New handler functions: `northd_nb_logical_router_port_handler()`, `northd_nb_static_route_handler()`, `northd_nb_nat_handler()` |
| `northd/en-lflow.c` | C.4, C.5 | Extend `lflow_northd_handler()` to consume `NORTHD_TRACKED_LR_PORTS` and `NORTHD_TRACKED_LR_ROUTES` |
| `northd/lflow-mgr.c` | C.5 | Per-router `route_lflow_ref` for targeted route flow rebuild |
| `tests/ovn-northd.at` | All | Tests for each sub-phase |

### Complexity & Risk

**Complexity**: High — router creation involves a 9-step cascade:
1. Datapath creation + tunnel key assignment
2. Port creation + network extraction (`extract_lrp_networks()`)
3. Bidirectional port peering (LSP ↔ LRP)
4. Distributed gateway port handling (cr- ports, `l3dgw_ports[]`)
5. SB datapath_binding + port_binding insertion
6. Load balancer association
7. Router group computation (`build_lrouter_groups()`)
8. Flow generation (10 builder functions × ~25-55 flows per LRP)
9. SB flow sync

**Tiered mitigation**:
- **Tier 1 / C.1** (standalone router): Datapath + base flows only. Fall back for anything complex.
- **Tier 2 / C.4-C.6** (router + ports/routes): Per-LRP handling with reverse index. Fall back for DGW/groups.
- **Tier 2** (Phase C extended): Handle router + simple ports (no DGW, no LB). Requires incremental port peering and per-LRP flow generation.
- **Tier 3** (future): Full incremental support including DGW, LBs, router groups. Requires incremental `build_lrouter_groups()` — the hardest part due to recursive cross-datapath references.

**Key invariant**: At every tier, `CHECK_NO_CHANGE_AFTER_RECOMPUTE` must pass — the incremental result must be identical to a full recompute.

---

## Phase D: Binary UPDATE_BATCH with Direct Datum Path (OVS Layer)

### Problem

Currently `UPDATE_BATCH` at `ovsdb-cs.c:740-761` just copies the binary payload to a string and parses it as JSON — no binary optimization at all for incremental updates:

```c
// ovsdb-cs.c:743-760 — UPDATE_BATCH still uses JSON
char *payload_str = xmemdup0(payload, payload_len);
struct json *json = json_from_string(payload_str);
// ... parse as JSON update ...
```

### Design

Extend the server to send incremental updates as binary UPDATE_BATCH frames with the same binary row encoding as ROW_BATCH. Include operation type (insert/modify/delete/xor) per row.

### Step D1: Binary UPDATE_BATCH wire format

Extend the binary frame format:
```
UPDATE_BATCH frame payload:
  string   table_name
  uint16   n_rows
  FOR each row:
    uuid     row_uuid
    uint8    operation (0=delete, 1=insert, 2=modify, 3=xor)
    uint16   n_cols (0 for delete)
    FOR each column:
      string   col_name
      uint8    key_type
      uint8    val_type
      datum    serialized_datum
```

### Step D2: Server-side encoding

**File**: `ovs/ovsdb/ovsdb-server.c` or `ovs/ovsdb/monitor.c`

Modify the server's update notification path to encode incremental updates in binary format when the client has negotiated binary transport.

### Step D3: Client-side decoding

**File**: `ovs/lib/ovsdb-cs.c`

Replace the JSON fallback in `ovsdb_cs_process_msg()` for `OVSDB_BIN_UPDATE_BATCH` with a binary deserialization path similar to Phase A's ROW_BATCH handler, producing `OVSDB_CS_EVENT_TYPE_BINARY_UPDATE` events.

### Expected Impact

- Every incremental update cycle benefits from direct datum path (not just initial snapshot)
- Most impactful when many rows change at once (batch port binding updates, chassis registration storms)
- For northd: faster processing of SB changes that trigger engine evaluation

---

## Phase E: Streaming-Aware Batch Processing (OVS + OVN Layers)

### Problem

Currently `ovsdb_cs_run()` processes up to 50 messages per call (line 863-870), then returns events. For large batches (e.g., 10,000 port bindings updating simultaneously), this means:

1. Process 50 messages → return events → engine_run() → back to processing
2. Each engine_run() may trigger expensive recomputes/handlers
3. The same engine node may fire 200 times for what is logically one batch

### Design

Add a "batch accumulation" mode in the northd main loop:

```c
// In northd main loop, before engine_run():
while (ovsdb_idl_has_pending_data(ovnsb_idl_loop.idl)
       && batch_time < BATCH_WINDOW_MS) {
    ovsdb_idl_run(ovnsb_idl_loop.idl);  // Process more messages
    batch_time = time_msec() - batch_start;
}
// Now all accumulated changes are in the IDL track lists
engine_run();  // Single engine run processes everything
```

### Step E1: IDL pending data check

**File**: `ovs/lib/ovsdb-idl.c`

Add a function to check if the underlying jsonrpc session has unread messages:
```c
bool
ovsdb_idl_has_pending_data(const struct ovsdb_idl *idl)
{
    return ovsdb_cs_has_pending_data(idl->cs);
}
```

### Step E2: Northd batch accumulation

**File**: `northd/ovn-northd.c` (main loop, around line 1022)

Add configurable batch window (default 0ms for backward compat):
```c
static int batch_window_ms = 0;  /* 0 = disabled */

/* Accumulate changes before running engine. */
int64_t batch_start = time_msec();
while (batch_window_ms > 0
       && ovsdb_idl_has_pending_data(ovnsb_idl_loop.idl)
       && time_msec() - batch_start < batch_window_ms) {
    ovsdb_idl_run(ovnsb_idl_loop.idl);
}
```

Add unixctl command for runtime tuning:
```c
unixctl_command_register("batch-window/set", "MSEC", 1, 1,
                         set_batch_window, NULL);
```

### Expected Impact

- Reduces number of engine_run() invocations during bulk updates
- Avoids repeated handler overhead for same node
- Most impactful during scale events (many chassis registering, mass port binding updates)
- Tunable: operators can set batch window based on deployment characteristics

---

## Implementation Priority & Dependencies

```
Phase A: Direct binary→datum path     [OVS]  — Highest ROI, medium effort
    ↓ (enables)
Phase B: Performance tests            [OVN]  — Validates A, measures baselines
    ↓ (independent of A)
Phase C: Incremental router handlers  [OVN]  — Highest user-visible impact
    ↓ (extends A)
Phase D: Binary UPDATE_BATCH          [OVS]  — Completes binary optimization
    ↓ (independent)
Phase E: Batch accumulation           [Both] — Reduces engine churn at scale
```

**Phase A + C together** deliver the biggest combined improvement:
- Phase A eliminates wasted CPU in the transport layer
- Phase C eliminates wasted CPU in the engine layer
- Combined: router creation goes from "parse entire SB as JSON + rebuild all flows" to "parse binary delta directly + build only new router's flows"

---

## Files to Modify (All Phases)

### OVS Layer (`ovs/`)

| File | Phase | Change |
|------|-------|--------|
| `lib/ovsdb-cs.h` | A | Add binary event type, binary update structs |
| `lib/ovsdb-cs.c` | A, D | Modify ROW_BATCH handler; add UPDATE_BATCH binary path |
| `lib/ovsdb-idl.c` | A, E | Binary update processing functions; pending data check |
| `lib/ovsdb-idl.h` | E | `ovsdb_idl_has_pending_data()` declaration |

### OVN Layer (`ovn/`)

| File | Phase | Change |
|------|-------|--------|
| `northd/northd.h` | C | New tracked data types (NORTHD_TRACKED_LR_CREATED/DELETED) |
| `northd/northd.c` | C | Incremental router creation/deletion in `northd_handle_lr_changes()` |
| `northd/en-northd.c` | C | Handler registration for new tracked types |
| `northd/en-lflow.c` | C | Lflow handler for new/deleted router flows |
| `northd/lflow-mgr.c` | C | Incremental flow sync for new routers |
| `northd/ovn-northd.c` | E | Batch accumulation in main loop |
| `tests/ovn-northd.at` | C | Incremental router creation test |
| `tests/perf-northd.at` | B | Binary transport performance test |

---

## Compilation & Build

```bash
# Build OVS first (contains the IDL changes)
cd /path/to/ovs
make -j$(sysctl -n hw.ncpu)

# Build OVN against the updated OVS
cd /path/to/ovn
make -j$(sysctl -n hw.ncpu)
```

No `./configure` re-run needed — no new source files, just modifications to existing ones.

---

## Verification

### 1. Compilation
```bash
cd /path/to/ovs && make -j$(sysctl -n hw.ncpu)
cd /path/to/ovn && make -j$(sysctl -n hw.ncpu)
```

### 2. Unit Tests — OVS IDL
```bash
cd /path/to/ovs
make check TESTSUITEFLAGS='-k ovsdb-idl'
```

### 3. Unit Tests — OVN binary transport
```bash
cd /path/to/ovn
make check TESTSUITEFLAGS='-k binary-transport'
```

### 4. Performance Test
```bash
cd /path/to/ovn
make check-perf TESTSUITEFLAGS='-k binary-transport'
```

### 5. Full OVN Test Suite (regression)
```bash
cd /path/to/ovn
make check TESTSUITEFLAGS='-j4'
```

### 6. Manual Verification
- Start OVN with binary-capable ovsdb-server
- Check logs for `"binary initial snapshot pending"` (confirms binary negotiation)
- Query stopwatch: `ovs-appctl stopwatch/show idl-update-parse`
- Compare parse times with/without `--no-binary-transport`

---

## Risks & Mitigations

| Risk | Phase | Mitigation |
|------|-------|-----------|
| Binary datum ownership semantics | A | Clone datums in `ovsdb_idl_binary_row_change()`, destroy after comparison. Clear ownership contract in comments. |
| Change tracking divergence | A | `ovsdb_idl_binary_row_change()` replicates exact same tracking logic as `ovsdb_idl_row_change()` — same seqno increments, same bitmap sets, same track_list operations. |
| XOR mode for binary updates | A | Initially only handle INSERT mode (initial snapshot). XOR support added in Phase D. |
| Memory leaks on error paths | A | Every binary struct has a matching destroy function. Event cleanup in `ovsdb_cs_run()` handles all cases. |
| Router creation with ports/policies in same txn | C | Start with simple case (router only), fall back to full recompute for complex transactions. Gradually extend. |
| Cross-datapath references on new routers | C | New router may reference existing LBs, port groups — handler must check all back-references or fall back. |
| Transit switch creation alongside router | C | If router + transit switch created atomically, fall back to recompute (transit switches have complex cross-references). |
| Batch window increases latency for single updates | E | Default to 0ms (disabled). Tunable via unixctl. Only enable for known bulk workloads. |
| `ovsdb_idl_set_binary_transport()` called after first run | A | All insertion points are before the main loop — verified in commit 4a57bf8b1. |
| Test flakiness | B | Perf test uses `--wait=sb sync` barriers and engine stats (deterministic counts), not wall-clock timing assertions. |
