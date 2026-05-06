<!-- Licensed under the Apache License, Version 2.0 -->

# OVN Binary Transport and Streaming Architecture

This document describes the architecture for enabling binary transport and
streaming data support in OVN components (ovn-controller, ovn-northd, ovn-ic).
Binary transport is an optimization built into the OVS/OVSDB library that
replaces JSON serialization with a compact binary wire format for OVSDB
monitor traffic.

## Motivation

Production OVN deployments with 10,000+ chassis have Southbound databases
exceeding 1 GB. The traditional JSON-based OVSDB monitor protocol introduces
significant serialization overhead:

- JSON parsing and generation is CPU-intensive for large table snapshots
- JSON encoding inflates data size by 2-3x compared to binary
- Initial snapshot delivery is single-threaded and blocks the server main loop

Binary transport addresses all three problems:

- Eliminates JSON serialization overhead on the critical monitor path
- Reduces wire bandwidth by ~10-40%
- Uses worker threads to parallelize initial snapshot encoding
- Is fully backward compatible (old servers ignore the binary format request)

## OVS Library Layer

The binary transport implementation lives entirely in the OVS library. OVN
components consume it through a single API call.

### IDL API

```c
void ovsdb_idl_set_binary_transport(struct ovsdb_idl *idl, bool enable);
```

This function sets a boolean flag that propagates to the OVSDB Client Session
(CS) layer:

```c
/* lib/ovsdb-idl.c */
void
ovsdb_idl_set_binary_transport(struct ovsdb_idl *idl, bool enable)
{
    ovsdb_cs_set_binary_transport(idl->cs, enable);
}

/* lib/ovsdb-cs.c */
void
ovsdb_cs_set_binary_transport(struct ovsdb_cs *cs, bool enable)
{
    cs->binary_transport = enable;
}
```

**Default behavior**: The CS layer sets `binary_transport = true` in
`ovsdb_cs_create()`, so binary transport is enabled by default for all
IDL instances built against this OVS.

**Safety**: The function can be called at any time. It sets a flag that
affects the next `monitor_cond_since` request. It does not force
reconnection or invalidate existing data. The flag is preserved across
reconnections.

### Protocol Negotiation

When binary transport is enabled, the client adds a 5th parameter to its
`monitor_cond_since` request:

```json
["monitor_cond_since", "db", "monitor_id", {"...conds..."},
 "last_txn_id", {"format": "binary"}]
```

The server responds in one of two ways:

**New server (supports binary)**:

1. Returns a 4-element JSON reply: `[found, last_txn_id, {}, {"format": "binary"}]`
2. Streams initial snapshot as binary frames (ROW_BATCH)
3. Sends INITIAL_END frame with txn_id
4. Subsequent updates sent as binary UPDATE_BATCH frames

**Old server (no binary support)**:

1. Ignores the 5th parameter
2. Returns standard 3-element JSON reply: `[found, last_txn_id, {table_updates}]`
3. All communication remains JSON — no errors, fully transparent

### Binary Wire Protocol

Binary frames coexist with JSON-RPC on the same TCP connection. The receiver
distinguishes them by the first byte: `0xDB` (binary magic) vs. JSON
characters (`{`, `[`, `"`, digits, `t`, `f`, `n`).

Frame header (8 bytes):

```
[0]    magic      0xDB
[1]    version    0x01
[2]    msg_type   (see below)
[3]    flags      0x00 (reserved)
[4..7] payload_len  uint32_t, network byte order
```

Message types:

| Name | Value | Description |
|------|-------|-------------|
| INITIAL_BEGIN | 0x01 | Start of initial snapshot |
| ROW_BATCH | 0x02 | Batch of serialized rows |
| INITIAL_END | 0x03 | End of initial snapshot (carries txn_id) |
| UPDATE | 0x04 | Single incremental update |
| UPDATE_BATCH | 0x05 | Batch of incremental updates |

Maximum payload: 64 MB (`OVSDB_BINARY_MAX_PAYLOAD`).

### Binary Codec

The binary codec (`lib/binary-codec.c`) serializes OVSDB data types:

- **Atoms**: INTEGER (8B), REAL (8B IEEE 754), BOOLEAN (1B), STRING (4B len +
  UTF-8), UUID (16B raw)
- **Datums**: uint32 count + N keys + N values (if value_type != VOID)
- **Rows (network)**: uuid[16] | n_columns (uint16) | per column: name_len +
  name + key_type + val_type + datum

All multi-byte integers use network byte order (big-endian) on the wire.

Sanity limits: 16 MB max string, 1M max datum elements.

### Client-Side Processing

The client (`lib/ovsdb-cs.c`) processes binary frames as follows:

1. `jsonrpc_recv()` detects binary frame by magic byte
2. ROW_BATCH frames: `ovsdb_cs_process_binary_row_batch()` deserializes
   rows and converts to `table-updates2` JSON format for the IDL
3. Events accumulate but are NOT flushed while `binary_initial_pending`
4. INITIAL_END: flush all events, IDL processes complete snapshot,
   `has_ever_connected = true`

Transactions are blocked while `binary_initial_pending = true`.

Key log messages for verification:

- `"binary initial snapshot pending — waiting for ROW_BATCH + INITIAL_END"`
  (INFO, confirms binary negotiation succeeded)
- `"received binary initial snapshot complete"` (INFO, snapshot loaded)

## OVN Integration

OVN components enable binary transport by calling
`ovsdb_idl_set_binary_transport()` on their IDL instances at initialization
time. Runtime overrides allow operators to disable the feature for debugging.

### ovn-controller

**File**: `controller/ovn-controller.c`

Two IDL connections:

1. **OVS local DB** — `ovsdb_idl_create(ovs_remote, &ovsrec_idl_class, ...)`
   Binary transport enabled at initialization, before
   `ovsdb_idl_get_initial_snapshot()`.

2. **OVN SB DB** — `ovsdb_idl_create_unconnected(&sbrec_idl_class, ...)`
   Binary transport enabled at initialization. Remote set dynamically from
   `external_ids:ovn-remote`.

**Runtime toggle**: `external_ids:ovn-binary-transport` (boolean, default
`true`). Read in `update_sb_db()` every main-loop iteration, following
the same pattern as `ovn-monitor-all`. Only affects the SB connection.

### ovn-northd

**File**: `northd/ovn-northd.c`

Two IDL connections:

1. **NB DB** — `ovsdb_idl_create(ovnnb_db, &nbrec_idl_class, true, true)`
2. **SB DB** — `ovsdb_idl_create(ovnsb_db, &sbrec_idl_class, true, true)`

Both set binary transport at initialization.

**CLI override**: `--no-binary-transport` flag disables binary transport on
both connections.

### ovn-ic

**File**: `ic/ovn-ic.c`

Four IDL connections: IC-NB, IC-SB, NB, SB. All set binary transport at
initialization.

**CLI override**: `--no-binary-transport` flag disables binary transport on
all four connections.

### Interaction with Conditional Monitoring

Binary transport is orthogonal to conditional monitoring
(`monitor_cond_since`). The OVS library adds the `{"format": "binary"}`
parameter alongside condition parameters. The server applies conditions
regardless of transport format — binary frames contain only rows matching
the client's conditions.

ovn-controller's dynamic condition updates (`update_sb_monitors()`) work
identically whether binary or JSON transport is active.

## Configuration Reference

### ovn-controller

`external_ids:ovn-binary-transport`
: Boolean. Controls binary transport for the SB OVSDB connection. Default:
  `true`. Set to `false` to disable for debugging. Backward compatible:
  falls back to JSON if server does not support binary.

### ovn-northd

`--no-binary-transport`
: Disables binary transport for NB and SB OVSDB connections. Default:
  binary enabled. Falls back to JSON automatically if server does not
  support binary.

### ovn-ic

`--no-binary-transport`
: Disables binary transport for all four OVSDB connections. Default: binary
  enabled. Falls back to JSON automatically if server does not support
  binary.

## Build Requirements

No build system changes are required. The `ovsdb_idl_set_binary_transport()`
function is declared in `ovsdb-idl.h` and implemented in
`libopenvswitch.la`, which all OVN daemons already link against.

Compilation:

```bash
cd ovn/
./configure --with-ovs-source=../ovs
make -j$(nproc)
```

## Testing

Binary transport can be verified through log messages:

1. Start ovn-controller with binary-capable ovsdb-server
2. Grep logs for `"binary initial snapshot pending"` — confirms negotiation
3. Grep logs for `"received binary initial snapshot complete"` — confirms
   data loaded via binary

Runtime toggle test:

```bash
# Disable
ovs-vsctl set open_vswitch . external_ids:ovn-binary-transport=false
# Force reconnect
ovn-appctl -t ovn-controller debug/reconnect
# Re-enable
ovs-vsctl set open_vswitch . external_ids:ovn-binary-transport=true
ovn-appctl -t ovn-controller debug/reconnect
```

## Server-Side Architecture (OVS)

For completeness, here is how the server handles binary transport. This is
implemented in the OVS tree, not in OVN.

### Disk-Backed Storage

The server uses a BINARYV1 file format for on-disk storage:

- Header: magic "BINARYV1" + version + SHA-1 schema hash + schema JSON
- Rows: append-only records (UUID + length + column data)
- Indexes: UUID->offset hmap, bloom filter, name->UUID hmap

Three query paths:

1. **Point lookup**: `_uuid == X` -> hmap -> cache -> pread (O(1))
2. **Name lookup**: `name == "foo"` -> name_index -> UUID -> point lookup (O(1))
3. **Full scan**: complex conditions -> disk cursor + condition filter

### Row Cache

- Clock-sweep LRU bounded by atom count (configurable `--cache-max-atoms`)
- States: UNLOADED -> LOADING -> CACHED (or ERROR)
- Query burst mode temporarily raises budget to 2x for table scans

### Worker Pool

For binary initial snapshots:

1. Main thread submits `disk_cursor_stream_worker_fn` jobs
2. Workers: pread -> deserialize -> serialize(binary) -> batch -> signal
3. Main thread: drain batches -> send ROW_BATCH frames
4. After all tables: send INITIAL_END

Workers bypass cache entirely (pread -> serialize -> destroy), avoiding lock
contention with the main thread.

### Query Engine

Three-layer architecture:

```
CALLERS (monitor.c, jsonrpc-server.c, transaction.c, lazy-load.c)
    |
QUERY ENGINE (plan, execute, lookup_uuid)
    |
+-- INDEX ENGINE (bloom for UUID, hash for column lookups)
+-- STORAGE ENGINE (pread, cursor_open/next, count, contains)
+-- CACHE (clock-sweep LRU, independent lifecycle)
```

Plan types:

- **POINT_LOOKUP**: `_uuid == uuid` -> bloom -> cache -> pread
- **INDEX_LOOKUP**: `column == value` + hash index -> index -> cache -> pread
- **FULL_SCAN**: complex or NULL conditions -> cursor + filter
