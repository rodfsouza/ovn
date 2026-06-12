/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <config.h>

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>

#include "en-global-config.h"
#include "en-group-ecmp-route.h"
#include "en-lflow.h"
#include "en-lr-nat.h"
#include "en-lr-stateful.h"
#include "en-ls-stateful.h"
#include "en-northd.h"
#include "lflow-mgr.h"
#include "en-meters.h"

#include "lib/inc-proc-eng.h"
#include "northd.h"
#include "stopwatch.h"
#include "lib/stopwatch-names.h"
#include "timeval.h"
#include "openvswitch/vlog.h"

VLOG_DEFINE_THIS_MODULE(en_lflow);

static void
lflow_get_input_data(struct engine_node *node,
                     struct lflow_input *lflow_input)
{
    struct northd_data *northd_data = engine_get_input_data("northd", node);
    struct port_group_data *pg_data =
        engine_get_input_data("port_group", node);
    struct sync_meters_data *sync_meters_data =
        engine_get_input_data("sync_meters", node);
    struct ed_type_lr_stateful *lr_stateful_data =
        engine_get_input_data("lr_stateful", node);
    struct ed_type_ls_stateful *ls_stateful_data =
        engine_get_input_data("ls_stateful", node);

    lflow_input->nbrec_bfd_table =
        EN_OVSDB_GET(engine_get_input("NB_bfd", node));
    lflow_input->sbrec_bfd_table =
        EN_OVSDB_GET(engine_get_input("SB_bfd", node));
    lflow_input->sbrec_logical_flow_table =
        EN_OVSDB_GET(engine_get_input("SB_logical_flow", node));
    lflow_input->sbrec_multicast_group_table =
        EN_OVSDB_GET(engine_get_input("SB_multicast_group", node));
    lflow_input->sbrec_igmp_group_table =
        EN_OVSDB_GET(engine_get_input("SB_igmp_group", node));
    lflow_input->sbrec_logical_dp_group_table =
        EN_OVSDB_GET(engine_get_input("SB_logical_dp_group", node));

    lflow_input->sbrec_mcast_group_by_name_dp =
           engine_ovsdb_node_get_index(
                          engine_get_input("SB_multicast_group", node),
                         "sbrec_mcast_group_by_name");

    lflow_input->ls_datapaths = &northd_data->ls_datapaths;
    lflow_input->lr_datapaths = &northd_data->lr_datapaths;
    lflow_input->ls_ports = &northd_data->ls_ports;
    lflow_input->lr_ports = &northd_data->lr_ports;
    lflow_input->ls_port_groups = &pg_data->ls_port_groups;
    lflow_input->lr_stateful_table = &lr_stateful_data->table;
    lflow_input->ls_stateful_table = &ls_stateful_data->table;
    lflow_input->meter_groups = &sync_meters_data->meter_groups;
    lflow_input->lb_datapaths_map = &northd_data->lb_datapaths_map;
    lflow_input->svc_monitor_map = &northd_data->svc_monitor_map;
    lflow_input->bfd_connections = NULL;

    struct group_ecmp_route_data *gerd =
        engine_get_input_data("group_ecmp_route", node);
    lflow_input->group_ecmp_data = gerd;

    struct ed_type_global_config *global_config =
        engine_get_input_data("global_config", node);
    lflow_input->features = &global_config->features;
    lflow_input->ovn_internal_version_changed =
        global_config->ovn_internal_version_changed;
    lflow_input->svc_monitor_mac = global_config->svc_monitor_mac;
}

void en_lflow_run(struct engine_node *node, void *data)
{
    const struct engine_context *eng_ctx = engine_get_context();

    struct lflow_input lflow_input;
    lflow_get_input_data(node, &lflow_input);

    struct hmap bfd_connections = HMAP_INITIALIZER(&bfd_connections);
    lflow_input.bfd_connections = &bfd_connections;

    stopwatch_start(BUILD_LFLOWS_STOPWATCH_NAME, time_msec());

    struct lflow_data *lflow_data = data;
    lflow_table_clear(lflow_data->lflow_table);
    lflow_reset_northd_refs(&lflow_input);

    build_bfd_table(eng_ctx->ovnsb_idl_txn,
                    lflow_input.nbrec_bfd_table,
                    lflow_input.sbrec_bfd_table,
                    lflow_input.lr_ports,
                    &bfd_connections);
    build_lflows(eng_ctx->ovnsb_idl_txn, &lflow_input,
                 lflow_data->lflow_table);
    bfd_cleanup_connections(lflow_input.nbrec_bfd_table,
                            &bfd_connections);
    hmap_destroy(&bfd_connections);
    stopwatch_stop(BUILD_LFLOWS_STOPWATCH_NAME, time_msec());

    engine_set_node_state(node, EN_UPDATED);
}

bool
lflow_northd_handler(struct engine_node *node,
                     void *data)
{
    struct northd_data *northd_data = engine_get_input_data("northd", node);
    if (!northd_has_tracked_data(&northd_data->trk_data)) {
        return false;
    }

    /* Router deletion: lflow_refs were already cleared in the northd
     * handler. Any flows that were only referenced by the deleted
     * datapath will be garbage collected during the next full lflow
     * sync. For now, trigger lflow recompute to ensure proper cleanup
     * of datapath group memberships in shared flows. */
    if (northd_data->trk_data.type & NORTHD_TRACKED_LR_DELETED) {
        return false;
    }

    const struct engine_context *eng_ctx = engine_get_context();
    struct lflow_data *lflow_data = data;

    struct lflow_input lflow_input;
    lflow_get_input_data(node, &lflow_input);

    /* Handle new router datapaths — generate base flows incrementally
     * using per-datapath lflow_ref, then sync to SB. */
    if (northd_data->trk_data.type & NORTHD_TRACKED_LR_CREATED) {
        struct hmapx_node *hmapx_node;
        HMAPX_FOR_EACH (hmapx_node, &northd_data->trk_data.trk_created_lrs) {
            struct ovn_datapath *od = hmapx_node->data;

            build_lr_flows_for_datapath(od, &lflow_input,
                                        lflow_data->lflow_table);

            if (!lflow_ref_sync_lflows(
                    od->lflow_ref, lflow_data->lflow_table,
                    eng_ctx->ovnsb_idl_txn,
                    lflow_input.ls_datapaths,
                    lflow_input.lr_datapaths,
                    false,
                    lflow_input.sbrec_logical_flow_table,
                    lflow_input.sbrec_logical_dp_group_table)
                || !lflow_ref_sync_lflows(
                    od->route_lflow_ref, lflow_data->lflow_table,
                    eng_ctx->ovnsb_idl_txn,
                    lflow_input.ls_datapaths,
                    lflow_input.lr_datapaths,
                    false,
                    lflow_input.sbrec_logical_flow_table,
                    lflow_input.sbrec_logical_dp_group_table)
                || !lflow_ref_sync_lflows(
                    od->policy_lflow_ref, lflow_data->lflow_table,
                    eng_ctx->ovnsb_idl_txn,
                    lflow_input.ls_datapaths,
                    lflow_input.lr_datapaths,
                    false,
                    lflow_input.sbrec_logical_flow_table,
                    lflow_input.sbrec_logical_dp_group_table)) {
                return false;
            }
        }
    }

    /* Handle routers whose static routes changed.
     * Try per-route incremental first (only for non-ECMP add/delete).
     * Fall back to per-router rebuild for ECMP-affected changes,
     * route modifications, or mixed add+delete. */
    if (northd_data->trk_data.type & NORTHD_TRACKED_LR_ROUTES) {
        bool per_route_ok = true;

        /* Check if all deleted routes are non-ECMP (have per-route refs). */
        struct route_del_entry *rde;
        LIST_FOR_EACH (rde, list_node,
                       &northd_data->trk_data.routes_deleted) {
            if (!route_flow_ref_find(&rde->od->route_refs,
                                      &rde->route_uuid)) {
                per_route_ok = false;
                break;
            }
        }

        /* Check if all added routes are non-ECMP. */
        if (per_route_ok) {
            struct hmapx_node *hmapx_node;
            HMAPX_FOR_EACH (hmapx_node,
                            &northd_data->trk_data.routes_added) {
                const struct nbrec_logical_router_static_route *route =
                    hmapx_node->data;
                struct ovn_datapath *od = route_to_lr_map_find(
                    &northd_data->route_to_lr_map, route);
                if (!od || route_affects_ecmp(od, route,
                                              &northd_data->lr_ports)) {
                    per_route_ok = false;
                    break;
                }
            }
        }

        if (per_route_ok
            && (!hmapx_is_empty(&northd_data->trk_data.routes_added)
                || !ovs_list_is_empty(
                    &northd_data->trk_data.routes_deleted))) {
            /* Per-route incremental: handle only changed routes. */

            /* Delete flows for removed routes. */
            LIST_FOR_EACH (rde, list_node,
                           &northd_data->trk_data.routes_deleted) {
                struct route_flow_ref *rfr = route_flow_ref_find(
                    &rde->od->route_refs, &rde->route_uuid);
                if (rfr) {
                    if (!lflow_ref_resync_flows(
                            rfr->lflow_ref, lflow_data->lflow_table,
                            eng_ctx->ovnsb_idl_txn,
                            lflow_input.ls_datapaths,
                            lflow_input.lr_datapaths,
                            false,
                            lflow_input.sbrec_logical_flow_table,
                            lflow_input.sbrec_logical_dp_group_table)) {
                        return false;
                    }
                    route_flow_ref_destroy(&rde->od->route_refs, rfr);
                }
            }

            /* Generate flows for added routes. */
            struct hmapx_node *hmapx_node;
            HMAPX_FOR_EACH (hmapx_node,
                            &northd_data->trk_data.routes_added) {
                const struct nbrec_logical_router_static_route *route =
                    hmapx_node->data;
                struct ovn_datapath *od = route_to_lr_map_find(
                    &northd_data->route_to_lr_map, route);
                if (!od) {
                    return false;
                }

                struct route_flow_ref *rfr = route_flow_ref_create(
                    &od->route_refs, &route->header_.uuid);
                build_single_route_flows(od, route,
                                          lflow_data->lflow_table,
                                          &northd_data->lr_ports,
                                          rfr->lflow_ref);
                if (!lflow_ref_sync_lflows(
                        rfr->lflow_ref, lflow_data->lflow_table,
                        eng_ctx->ovnsb_idl_txn,
                        lflow_input.ls_datapaths,
                        lflow_input.lr_datapaths,
                        false,
                        lflow_input.sbrec_logical_flow_table,
                        lflow_input.sbrec_logical_dp_group_table)) {
                    return false;
                }
            }
        } else {
            /* Fall back to per-router rebuild (ECMP, modifications,
             * or route seqno changes without column change). */
            struct hmapx_node *hmapx_node;
            HMAPX_FOR_EACH (hmapx_node,
                            &northd_data->trk_data.lr_with_changed_routes) {
                struct ovn_datapath *od = hmapx_node->data;

                /* Unlink per-route refs. */
                struct route_flow_ref *rfr;
                HMAP_FOR_EACH (rfr, hmap_node, &od->route_refs) {
                    lflow_ref_unlink_lflows(rfr->lflow_ref);
                }
                lflow_ref_unlink_lflows(od->route_lflow_ref);

                build_lr_route_flows_for_datapath(od, &lflow_input,
                                                  lflow_data->lflow_table);

                /* Sync router-level route ref. */
                if (!lflow_ref_sync_lflows(
                        od->route_lflow_ref, lflow_data->lflow_table,
                        eng_ctx->ovnsb_idl_txn,
                        lflow_input.ls_datapaths,
                        lflow_input.lr_datapaths,
                        false,
                        lflow_input.sbrec_logical_flow_table,
                        lflow_input.sbrec_logical_dp_group_table)) {
                    return false;
                }
                /* Sync per-route refs. */
                HMAP_FOR_EACH (rfr, hmap_node, &od->route_refs) {
                    if (!lflow_ref_sync_lflows(
                            rfr->lflow_ref, lflow_data->lflow_table,
                            eng_ctx->ovnsb_idl_txn,
                            lflow_input.ls_datapaths,
                            lflow_input.lr_datapaths,
                            false,
                            lflow_input.sbrec_logical_flow_table,
                            lflow_input.sbrec_logical_dp_group_table)) {
                        return false;
                    }
                }
            }
        }
    }

    /* Handle routers whose policies changed — rebuild only policy flows. */
    if (northd_data->trk_data.type & NORTHD_TRACKED_LR_POLICIES) {
        struct hmapx_node *hmapx_node;
        HMAPX_FOR_EACH (hmapx_node,
                        &northd_data->trk_data.lr_with_changed_policies) {
            struct ovn_datapath *od = hmapx_node->data;

            lflow_ref_unlink_lflows(od->policy_lflow_ref);
            build_lr_policy_flows_for_datapath(od, &lflow_input,
                                               lflow_data->lflow_table);
            if (!lflow_ref_sync_lflows(
                    od->policy_lflow_ref, lflow_data->lflow_table,
                    eng_ctx->ovnsb_idl_txn,
                    lflow_input.ls_datapaths,
                    lflow_input.lr_datapaths,
                    false,
                    lflow_input.sbrec_logical_flow_table,
                    lflow_input.sbrec_logical_dp_group_table)) {
                return false;
            }
        }
    }

    if (!lflow_handle_northd_port_changes(eng_ctx->ovnsb_idl_txn,
                                          &northd_data->trk_data.trk_lsps,
                                          &lflow_input,
                                          lflow_data->lflow_table)) {
        return false;
    }

    if (!lflow_handle_northd_lr_port_changes(
            eng_ctx->ovnsb_idl_txn,
            &northd_data->trk_data.trk_lrps,
            &lflow_input,
            lflow_data->lflow_table)) {
        return false;
    }

    if (!lflow_handle_northd_lb_changes(
            eng_ctx->ovnsb_idl_txn, &northd_data->trk_data.trk_lbs,
            &lflow_input, lflow_data->lflow_table)) {
        return false;
    }

    engine_set_node_state(node, EN_UPDATED);
    return true;
}

bool
lflow_port_group_handler(struct engine_node *node, void *data OVS_UNUSED)
{
    struct port_group_data *pg_data =
        engine_get_input_data("port_group", node);

    /* If the set of switches per port group didn't change then there's no
     * need to reprocess lflows.  Otherwise, there might be a need to
     * add/delete port-group ACLs to/from switches. */
    if (pg_data->ls_port_groups_sets_changed) {
        return false;
    }

    engine_set_node_state(node, EN_UPDATED);
    return true;
}

bool
lflow_lr_stateful_handler(struct engine_node *node, void *data)
{
    struct ed_type_lr_stateful *lr_sful_data =
        engine_get_input_data("lr_stateful", node);

    if (!lr_stateful_has_tracked_data(&lr_sful_data->trk_data)
        || lr_sful_data->trk_data.vip_nats_changed) {
        return false;
    }

    const struct engine_context *eng_ctx = engine_get_context();
    struct lflow_data *lflow_data = data;
    struct lflow_input lflow_input;

    lflow_get_input_data(node, &lflow_input);
    if (!lflow_handle_lr_stateful_changes(eng_ctx->ovnsb_idl_txn,
                                          &lr_sful_data->trk_data,
                                          &lflow_input,
                                          lflow_data->lflow_table)) {
        return false;
    }

    engine_set_node_state(node, EN_UPDATED);
    return true;
}

bool
lflow_ls_stateful_handler(struct engine_node *node, void *data)
{
    struct ed_type_ls_stateful *ls_sful_data =
        engine_get_input_data("ls_stateful", node);

    if (!ls_stateful_has_tracked_data(&ls_sful_data->trk_data)) {
        return false;
    }

    const struct engine_context *eng_ctx = engine_get_context();
    struct lflow_data *lflow_data = data;
    struct lflow_input lflow_input;

    lflow_get_input_data(node, &lflow_input);
    if (!lflow_handle_ls_stateful_changes(eng_ctx->ovnsb_idl_txn,
                                          &ls_sful_data->trk_data,
                                          &lflow_input,
                                          lflow_data->lflow_table)) {
        return false;
    }

    engine_set_node_state(node, EN_UPDATED);
    return true;
}

bool
lflow_group_ecmp_route_handler(struct engine_node *node, void *data)
{
    struct group_ecmp_route_data *gerd =
        engine_get_input_data("group_ecmp_route", node);

    if (hmapx_is_empty(&gerd->trk_data.deleted_datapath_routes)
        && hmapx_is_empty(&gerd->trk_data.crupdated_datapath_routes)) {
        return true;
    }

    const struct engine_context *eng_ctx = engine_get_context();
    struct lflow_data *lflow_data = data;
    struct lflow_input lflow_input;
    lflow_get_input_data(node, &lflow_input);

    /* Handle deleted route nodes — unlink and sync to remove SB flows. */
    struct hmapx_node *hmapx_node;
    HMAPX_FOR_EACH (hmapx_node, &gerd->trk_data.deleted_datapath_routes) {
        struct ecmp_route_node *rn = hmapx_node->data;
        lflow_ref_unlink_lflows(rn->lflow_ref);

        if (!lflow_ref_sync_lflows(
                rn->lflow_ref, lflow_data->lflow_table,
                eng_ctx->ovnsb_idl_txn,
                lflow_input.ls_datapaths,
                lflow_input.lr_datapaths,
                false,
                lflow_input.sbrec_logical_flow_table,
                lflow_input.sbrec_logical_dp_group_table)) {
            return false;
        }
    }

    /* Handle created/updated route nodes — rebuild and sync flows. */
    HMAPX_FOR_EACH (hmapx_node, &gerd->trk_data.crupdated_datapath_routes) {
        struct ecmp_route_node *rn = hmapx_node->data;
        lflow_ref_unlink_lflows(rn->lflow_ref);

        if (rn->is_ecmp) {
            build_ecmp_route_flow(lflow_data->lflow_table,
                                  (struct ovn_datapath *) rn->od,
                                  lflow_input.features->ct_no_masked_label,
                                  lflow_input.lr_ports,
                                  rn->group, rn->lflow_ref);
        } else {
            build_static_route_flow(lflow_data->lflow_table,
                                    (struct ovn_datapath *) rn->od,
                                    lflow_input.lr_ports,
                                    rn->route, rn->lflow_ref);
        }

        if (!lflow_ref_sync_lflows(
                rn->lflow_ref, lflow_data->lflow_table,
                eng_ctx->ovnsb_idl_txn,
                lflow_input.ls_datapaths,
                lflow_input.lr_datapaths,
                false,
                lflow_input.sbrec_logical_flow_table,
                lflow_input.sbrec_logical_dp_group_table)) {
            return false;
        }
    }

    engine_set_node_state(node, EN_UPDATED);
    return true;
}

void *en_lflow_init(struct engine_node *node OVS_UNUSED,
                     struct engine_arg *arg OVS_UNUSED)
{
    struct lflow_data *data = xmalloc(sizeof *data);
    data->lflow_table = lflow_table_alloc();
    lflow_table_init(data->lflow_table);
    return data;
}

void en_lflow_cleanup(void *data_)
{
    struct lflow_data *data = data_;
    lflow_table_destroy(data->lflow_table);
}
