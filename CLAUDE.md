# OVN Project — Claude Code Context

## Project Overview

OVN (Open Virtual Network) is a system of daemons that translates virtual network
configuration into OpenFlow rules and installs them into Open vSwitch. It provides
logical routers, logical switches, ACLs, DHCP, DNS, load balancers, and NAT.

**Version**: 24.03.x (branch-24.03)
**License**: Apache 2.0
**Language**: C (C99)

## Repository Structure

```
ovn/
├── controller/          # ovn-controller — runs on every hypervisor
├── northd/              # ovn-northd — centralized logical flow generator
├── ic/                  # ovn-ic — inter-datacenter interconnect daemon
├── lib/                 # libovn.la — shared library, IDL definitions, utilities
├── utilities/           # CLI tools: ovn-nbctl, ovn-sbctl, ovn-trace, etc.
├── tests/               # Autotest (.at) test suite
├── include/ovn/         # Public headers (actions.h, expr.h, features.h)
├── Documentation/       # Sphinx docs, internals, tutorials
├── ovn-nb.ovsschema     # Northbound DB schema (31 tables)
├── ovn-sb.ovsschema     # Southbound DB schema (34 tables)
├── ovn-ic-nb.ovsschema  # IC Northbound schema
├── ovn-ic-sb.ovsschema  # IC Southbound schema
└── configure.ac         # Autotools build config
```

## Key Components

### ovn-controller (`controller/`)
- Runs on every hypervisor; translates SB logical flows into OpenFlow rules
- Two OVSDB connections: local OVS DB + remote OVN SB DB
- Uses conditional monitoring (`update_sb_monitors()`) for SB tables
- Key subsystems: lflow, physical, pinctrl, binding, chassis, ofctrl, lflow-cache

### ovn-northd (`northd/`)
- Centralized daemon; translates NB config into SB logical flows
- Two OVSDB connections: NB DB (read) + SB DB (read/write)
- Incremental processing engine (IPE) with DAG of `en-*.c` engine nodes
- Monitors everything via `ovsdb_idl_track_add_all()`

### ovn-ic (`ic/`)
- Handles multi-region OVN connectivity
- Four OVSDB connections: IC-NB, IC-SB, NB, SB

### libovn (`lib/`)
- Shared library used by all components
- Auto-generated IDL files from `.ovsschema` + `.ann` annotations
- IDL generation: `ovsdb-idlc annotate` + `ovsdb-idlc c-idl-source/header`
- Key utilities: inc-proc-eng, actions, expr, lex, chassis-index

## Build System

```bash
# Dependencies: OVS source tree
./boot.sh
./configure --with-ovs-source=../ovs
make -j$(sysctl -n hw.ncpu)

# Run tests
make check
make check TESTSUITEFLAGS='-k <keyword>'   # run specific tests
make check TESTSUITEFLAGS='-j4'            # parallel tests
```

### Linker dependencies
- ovn-controller: `libovn.la` + `libopenvswitch.la`
- ovn-northd: `libovn.la` + `libovsdb.la` + `libopenvswitch.la`
- ovn-ic: `libovn.la` + `libopenvswitch.la` (transitively)

## OVS Dependency

OVN depends on a local OVS source tree at `../ovs` (or `--with-ovs-source`).
The local OVS at `/Users/rsouza/repos/mgc-iaas/foundation/ovs` includes custom
enhancements:
- Binary transport protocol for OVSDB (binary-codec, binary-protocol)
- Disk-backed storage engine with lazy loading
- Multi-threaded worker pool for initial snapshot streaming
- Three-layer query engine (query, index, storage)

## Database Schemas

### Northbound (ovn-nb.ovsschema, v7.3.0)
High-level network config: Logical_Switch, Logical_Router, ACL, NAT,
Load_Balancer, DHCP_Options, DNS, Port_Group, Address_Set, etc. (31 tables)

### Southbound (ovn-sb.ovsschema, v20.33.0)
Computed state: Chassis, Port_Binding, Datapath_Binding, Logical_Flow,
MAC_Binding, Multicast_Group, FDB, IGMP_Group, etc. (34 tables)

## Testing

- Framework: GNU Autotest (`.at` files in `tests/`)
- Key test files: `ovn.at` (1.6MB), `ovn-northd.at`, `ovn-controller.at`, `ovn-ic.at`
- Macros: `ovn-macros.at`, `ovsdb-macros.at`
- Pattern: `sim_add hv1` → `ovs-vsctl add-br` → `ovn_attach` → assertions

## Coding Conventions

- C99, follows OVS coding style
- 4-space indentation (no tabs in C code)
- `VLOG_*` macros for logging (VLOG_INFO, VLOG_WARN, VLOG_DBG, VLOG_ERR)
- CLI options: enum + `static struct option long_options[]` + `getopt_long` switch
- External IDs pattern: `get_chassis_external_id_value_bool()` for runtime config
- Manpages in XML format (`.8.xml` files)
