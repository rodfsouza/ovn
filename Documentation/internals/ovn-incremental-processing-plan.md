# OVN Incremental Processing Plan

Optimizing northd incremental flow processing for router operations.
Binary transport optimization (Phase A) is documented in the OVS repo at
`Documentation/internals/ovsdb-binary-transport-architecture.md`.

## Implementation Status (2026-05-27)

| Phase | Status | Commit | Notes |
|-------|--------|--------|-------|
| **A** | **DONE** | OVS `aed87a9f7` | Direct binary→datum path. Eliminates JSON round-trip for ROW_BATCH. |
| **B** | **DONE** | OVN `a4cd3936a` | Perf test in `tests/perf-northd.at`. |
| **C.1** | **DONE** | OVN `b77f6cf93` | Datapath materialization for new routers. |
| **C.2** | **DONE** | OVN `12c9051f5` | 4 new engine nodes (LRP, static_route, policy, NAT). |
| **C.3** | **DONE** | OVN `12c9051f5` | LRP handler wired into DAG. |
| **C.4** | **DONE** | OVN `7d413942f` | Per-datapath `lflow_ref` (3 refs × 16 builders). Incremental lflow gen for new routers. |
| **C.5** | **DONE** | OVN `7d413942f` | Incremental static route handling via `od->route_lflow_ref`. |
| **C.6** | **DONE** | OVN `4987eb03c` | Router + ports in same txn. Inline LRP creation. |
| **C.7** | **DONE** | OVN `7d413942f` | Incremental policy handling via `od->policy_lflow_ref`. |
| **C.8** | **DONE** | OVN `69da4c94b`, `2c1ca7860` | northd creates/deletes port+SB incrementally. Per-LRP lflow tracking via `tracked_lr_ports`. |
| **C.10** | **DONE** | OVN `69da4c94b` | Router deletion with ports. Iterates `od->ports`, cleans cr_port/peer/SB. Single-member `lr_group` cleanup. Lflow returns false (recompute). |
| **C.11** | **DONE** | OVN `69da4c94b`, `2c1ca7860` | LB association restored. Bitmap resize ensures OOB safety. |
| **C.11.1** | **DONE** | OVN `2c1ca7860` | `nb_lr_map_n_bits`/`nb_ls_map_n_bits` fields. `ensure_lr/ls_bitmap_size()` helpers. Bulk resize after new router creation. Doubling strategy for `ovn_lb_group_datapaths` arrays. |
| **C.8.1** | **DONE** | OVN `2c1ca7860` | `tracked_lr_ports` struct, `NORTHD_TRACKED_LR_PORTS` enum. Peer resolution via `ls_ports` scan. Incremental flow generation via `build_lswitch_and_lrouter_iterate_by_lrp()` + `build_lbnat_lflows_iterate_by_lrp()`. Deferred port destruction in `destroy_northd_data_tracked_changes()`. |
| **C.9** | NOT STARTED | — | DGW Tier 1 — cr- port on new routers. |
| **D** | NOT STARTED | — | Binary UPDATE_BATCH with direct datum path. XOR support needed. |
| **E** | NOT STARTED | — | Streaming-aware batch processing. |

### Current Incremental Behavior

| Operation | northd | lflow | Phase |
|-----------|--------|-------|-------|
| Router add (any config, no DGW) | **norecompute** | **norecompute** | C.1+C.4+C.6+C.11.1 |
| Router + ports + NAT + routes + policies + LB | **norecompute** | **norecompute** | C.6+C.5+C.7+C.11.1 |
| LRP add on existing router (no DGW) | **norecompute** | **norecompute** | C.8+C.8.1 |
| LRP delete on existing router (no DGW) | **norecompute** | **norecompute** | C.8+C.8.1 |
| Router delete (with/without ports) | **norecompute** | recompute | C.10 |
| Static route change | **norecompute** | **norecompute** | C.5 |
| Policy change | **norecompute** | **norecompute** | C.7 |
| NAT change | **norecompute** | recompute | Existing |
| LB change | **norecompute** | **norecompute** | Existing |
| LRP modify (MAC/IP change) | recompute | recompute | Fallback |
| DGW port changes | recompute | recompute | Fallback |
| Multi-router group deletion | recompute | recompute | Fallback |
| Disabled router | recompute | recompute | Fallback |

### Remaining Future Work

1. **C.9 — DGW Tier 1**: Create cr- port on new routers via `ovn_chassis_redirect_name()` + `ovn_port_create()`. Populate `od->l3dgw_ports[]`. DGW on existing routers remains fallback.
2. **LRP modify**: MAC/IP changes affect many flow builders — complex to handle incrementally.
3. **Multi-router group deletion**: `lr_group->n_router_dps > 1` requires recursive group rebuild.
4. **Router deletion lflow**: Currently returns false in lflow handler. Needs datapath kept alive for `lflow_ref_resync_flows()`.
5. **Binary XOR** (Phase D): `ovsdb_datum_apply_diff_in_place()` support.
6. **Batch accumulation** (Phase E): Accumulate binary frames before `engine_run()`.
