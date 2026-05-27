<!-- Licensed under the Apache License, Version 2.0 -->

# OVN Binary Transport — Implementation Plan

This document describes the step-by-step implementation plan for adding binary
transport and streaming support to OVN components.

## Context

Production OVN deployments with 10,000+ chassis have SB databases exceeding
1 GB. The local OVS/OVSDB library has been enhanced with a binary transport
protocol that:

- Eliminates JSON serialization overhead on the monitor path
- Uses worker threads to parallelize initial snapshot encoding
- Reduces wire bandwidth by ~10-40%
- Is fully backward compatible (old servers ignore the binary format request)

The OVS IDL exposes `ovsdb_idl_set_binary_transport(idl, bool)` which sets
a flag in the CS layer. When composing `monitor_cond_since`, it appends
`{"format": "binary"}` as a 5th parameter. The server acknowledges with a
4-element reply and streams binary frames (ROW_BATCH, INITIAL_END,
UPDATE_BATCH).

**Important discovery**: The CS layer already defaults `binary_transport =
true` in `ovsdb_cs_create()` (ovsdb-cs.c:351). So OVN components built
against this OVS will already attempt binary negotiation. Our explicit calls
serve to:

1. Make the intent self-documenting in OVN code
2. Enable runtime toggling (external_ids for ovn-controller, CLI for
   northd/ic)
3. Allow operators to disable for debugging

## Files to Modify

| File | Change |
|------|--------|
| `controller/ovn-controller.c` | Enable binary transport on OVS + SB IDLs, add runtime `external_ids:ovn-binary-transport` toggle |
| `northd/ovn-northd.c` | Enable binary transport on NB + SB IDLs, add `--no-binary-transport` CLI flag |
| `ic/ovn-ic.c` | Enable binary transport on all 4 IDLs, add `--no-binary-transport` CLI flag |
| `controller/ovn-controller.8.xml` | Document `external_ids:ovn-binary-transport` |
| `northd/ovn-northd.8.xml` | Document `--no-binary-transport` |
| `ic/ovn-ic.8.xml` | Document `--no-binary-transport` |
| `tests/ovn-controller.at` | Test the external_ids toggle |

## Step 1: ovn-controller

**File**: `controller/ovn-controller.c`

### 1a. Enable binary transport on OVS IDL (line 5407)

After `ctrl_register_ovs_idl(ovs_idl_loop.idl)` and before index creation:

```c
    ctrl_register_ovs_idl(ovs_idl_loop.idl);
+   ovsdb_idl_set_binary_transport(ovs_idl_loop.idl, true);
```

**Why here**: Before `ovsdb_idl_get_initial_snapshot()` at line 5426 which
triggers the first `ovsdb_idl_run()`. The flag must be set before the first
monitor request.

### 1b. Enable binary transport on SB IDL (line 5431)

After `ovsdb_idl_set_leader_only()`:

```c
    ovsdb_idl_set_leader_only(ovnsb_idl_loop.idl, false);
+   ovsdb_idl_set_binary_transport(ovnsb_idl_loop.idl, true);
```

**Why here**: SB IDL is created unconnected — remote is set later in
`update_sb_db()`. The flag is safe to set before any connection is
established.

### 1c. Runtime toggle in `update_sb_db()` (after line 855)

After the `monitor_all` block and its `monitor_all_p` assignment:

```c
    if (monitor_all_p) {
        *monitor_all_p = monitor_all;
    }
+
+   bool binary_transport =
+       get_chassis_external_id_value_bool(
+           &cfg->external_ids, chassis_id, "ovn-binary-transport", true);
+   ovsdb_idl_set_binary_transport(ovnsb_idl, binary_transport);
```

**Why**: Follows the exact pattern of `ovn-monitor-all` (lines 842-858).
The call is idempotent (just sets a bool flag). Default is `true` (enabled).
The flag takes effect on the next monitor request (e.g., after reconnection
or condition change).

**Note**: This only toggles the SB connection, not the OVS local connection.
The OVS local DB is small, so runtime toggling is unnecessary there.

## Step 2: ovn-northd

**File**: `northd/ovn-northd.c`

### 2a. Add static variable (near line 84)

```c
+static bool use_binary_transport = true;
```

### 2b. Add CLI option in `parse_options()` (line 608)

Add to the enum:

```c
    enum {
        OVN_DAEMON_OPTION_ENUMS,
        VLOG_OPTION_ENUMS,
        SSL_OPTION_ENUMS,
        OPT_DRY_RUN,
        OPT_N_THREADS,
+       OPT_NO_BINARY_TRANSPORT,
    };
```

Add to long_options array (after the `n-threads` entry, line 623):

```c
        {"n-threads", required_argument, NULL, OPT_N_THREADS},
+       {"no-binary-transport", no_argument, NULL, OPT_NO_BINARY_TRANSPORT},
```

Add case in switch (after the `OPT_N_THREADS` handler):

```c
+       case OPT_NO_BINARY_TRANSPORT:
+           use_binary_transport = false;
+           break;
```

### 2c. Enable binary transport on NB IDL (after line 855)

```c
    ovsdb_idl_track_add_all(ovnnb_idl_loop.idl);
+   ovsdb_idl_set_binary_transport(ovnnb_idl_loop.idl, use_binary_transport);
```

### 2d. Enable binary transport on SB IDL (after line 876)

```c
    ovsdb_idl_set_write_changed_only_all(ovnsb_idl_loop.idl, true);
+   ovsdb_idl_set_binary_transport(ovnsb_idl_loop.idl, use_binary_transport);
```

## Step 3: ovn-ic

**File**: `ic/ovn-ic.c`

### 3a. Add static variable (after line 94)

```c
 static const char *ssl_ca_cert_file;
+static bool use_binary_transport = true;
```

### 3b. Add CLI option in `parse_options()` (line 2028)

Add to enum:

```c
    enum {
        OVN_DAEMON_OPTION_ENUMS,
        VLOG_OPTION_ENUMS,
        SSL_OPTION_ENUMS,
+       OPT_NO_BINARY_TRANSPORT,
    };
```

Add to long_options array (before the `{NULL, 0, NULL, 0}` terminator):

```c
+       {"no-binary-transport", no_argument, NULL, OPT_NO_BINARY_TRANSPORT},
        {NULL, 0, NULL, 0},
```

Add case in switch (before the `default: break;`):

```c
+       case OPT_NO_BINARY_TRANSPORT:
+           use_binary_transport = false;
+           break;
+
        default:
            break;
```

### 3c. Enable binary transport on all 4 IDLs

After each IDL creation (lines 2218, 2222, 2226, 2303):

```c
    /* ovn-ic-nb db. */
    struct ovsdb_idl_loop ovninb_idl_loop = OVSDB_IDL_LOOP_INITIALIZER(
        ovsdb_idl_create(ovn_ic_nb_db, &icnbrec_idl_class, true, true));
+   ovsdb_idl_set_binary_transport(ovninb_idl_loop.idl, use_binary_transport);

    /* ovn-ic-sb db. */
    struct ovsdb_idl_loop ovnisb_idl_loop = OVSDB_IDL_LOOP_INITIALIZER(
        ovsdb_idl_create(ovn_ic_sb_db, &icsbrec_idl_class, true, true));
+   ovsdb_idl_set_binary_transport(ovnisb_idl_loop.idl, use_binary_transport);

    /* ovn-nb db. */
    struct ovsdb_idl_loop ovnnb_idl_loop = OVSDB_IDL_LOOP_INITIALIZER(
        ovsdb_idl_create(ovnnb_db, &nbrec_idl_class, false, true));
+   ovsdb_idl_set_binary_transport(ovnnb_idl_loop.idl, use_binary_transport);
```

And after line 2303:

```c
    /* ovn-sb db. */
    struct ovsdb_idl_loop ovnsb_idl_loop = OVSDB_IDL_LOOP_INITIALIZER(
        ovsdb_idl_create(ovnsb_db, &sbrec_idl_class, false, true));
+   ovsdb_idl_set_binary_transport(ovnsb_idl_loop.idl, use_binary_transport);
```

## Step 4: Documentation

### ovn-controller.8.xml

Add after `ovn-monitor-all` block (after line 141):

```xml
      <dt><code>external_ids:ovn-binary-transport</code></dt>
      <dd>
        <p>
          A boolean value that controls whether <code>ovn-controller</code>
          uses binary transport for its connection to the
          <code>OVN_Southbound</code> database.  When enabled,
          <code>ovn-controller</code> negotiates binary format with the
          server, which reduces serialization overhead and network bandwidth
          for large databases.
        </p>
        <p>
          Binary transport is fully backward compatible: if the server does
          not support it, <code>ovn-controller</code> automatically falls
          back to JSON format.
        </p>
        <p>
          Set to <code>false</code> to disable binary transport, for
          example when debugging protocol issues.
        </p>
        <p>
          Default value is <var>true</var>.
        </p>
      </dd>
```

### ovn-northd.8.xml

Add after `--n-threads` option:

```xml
      <dt><code>--no-binary-transport</code></dt>
      <dd>
        <p>
          Disables binary transport for OVSDB connections to the
          <code>OVN_Northbound</code> and <code>OVN_Southbound</code>
          databases.  By default, <code>ovn-northd</code> negotiates binary
          format with the server to reduce serialization overhead.  If the
          server does not support binary transport, JSON format is used
          automatically regardless of this option.
        </p>
      </dd>
```

### ovn-ic.8.xml

Add in the options section:

```xml
      <dt><code>--no-binary-transport</code></dt>
      <dd>
        <p>
          Disables binary transport for all OVSDB connections.  By default,
          <code>ovn-ic</code> negotiates binary format with the server to
          reduce serialization overhead.  If the server does not support
          binary transport, JSON format is used automatically regardless
          of this option.
        </p>
      </dd>
```

## Step 5: Tests

**File**: `tests/ovn-controller.at`

Add a test that validates the `external_ids:ovn-binary-transport` toggle
mechanism:

```
AT_SETUP([ovn-controller - binary transport toggle])
AT_KEYWORDS([binary-transport])
ovn_start

# Start with default (binary enabled)
net_add n1
sim_add hv1
as hv1
ovs-vsctl add-br br-phys
ovn_attach n1 br-phys 192.168.0.1

# Verify binary transport is active (look for negotiation log)
OVS_WAIT_UNTIL([grep -q "binary initial snapshot pending" hv1/ovn-controller.log])

# Disable binary transport
as hv1
ovs-vsctl set open . external_ids:ovn-binary-transport=false

# Force reconnect to renegotiate
as hv1
ovn-appctl -t ovn-controller debug/reconnect

# Re-enable
as hv1
ovs-vsctl set open . external_ids:ovn-binary-transport=true
ovn-appctl -t ovn-controller debug/reconnect

# Should see a new binary negotiation
OVS_WAIT_UNTIL([test $(grep -c "binary initial snapshot pending" hv1/ovn-controller.log) -ge 2])

OVN_CLEANUP([hv1])
AT_CLEANUP
```

**Note**: This test depends on the OVS server in the test environment
supporting binary transport. If the test infrastructure uses stock OVS without
binary support, the server will silently fall back to JSON and the log message
won't appear. In that case, the test should be marked with appropriate
`AT_SKIP` conditions.

## Step 6: Build and Compilation

**No build system changes required.** The `ovsdb_idl_set_binary_transport()`
function is declared in `ovsdb-idl.h` and implemented in
`libopenvswitch.la`, which all three daemons already link against:

- ovn-controller: `lib/libovn.la $(OVS_LIBDIR)/libopenvswitch.la`
- ovn-northd: `lib/libovn.la $(OVSDB_LIBDIR)/libovsdb.la $(OVS_LIBDIR)/libopenvswitch.la`
- ovn-ic: links against `libopenvswitch.la` transitively via `lib/libovn.la`

Compilation commands:

```bash
cd ovn/
./boot.sh                    # only if configure.ac changed (not needed here)
./configure --with-ovs-source=../ovs
make -j$(nproc)
```

## Verification

1. **Build**: `make -j$(nproc)` — confirms compilation with the new API calls
2. **Unit tests**: `make check TESTSUITEFLAGS='-k binary-transport'`
3. **Integration**: Start binary-capable ovsdb-server + ovn-controller, grep
   for `"binary initial snapshot pending"` in logs
4. **Fallback**: Start with stock ovsdb-server (no binary), verify no errors
   and graceful JSON fallback
5. **Runtime toggle**: `ovs-vsctl set open_vswitch . external_ids:ovn-binary-transport=false`
   then reconnect and verify JSON mode
6. **Code review**: Review for correctness, readability, security, and
   performance

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| `ovsdb_idl_set_binary_transport` called after first `ovsdb_idl_run()` | All insertion points are before the main loop — safe |
| Conditional monitoring conflict | Binary transport is orthogonal — OVS library adds format param alongside conditions |
| CI uses stock OVS without binary support | Server silently falls back to JSON; test verifies toggle mechanism, not frame exchange |
| Runtime toggle doesn't take effect immediately | Flag takes effect on next monitor request (reconnect or condition change); documented |
