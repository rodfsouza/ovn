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

#include "en-group-ecmp-route.h"
#include "lflow-mgr.h"
#include "lib/inc-proc-eng.h"
#include "northd.h"
#include "simap.h"
#include "openvswitch/vlog.h"
#include "stopwatch.h"
#include "lib/stopwatch-names.h"
#include "timeval.h"
#include "lib/ovn-util.h"

VLOG_DEFINE_THIS_MODULE(en_group_ecmp_route);

/* ECMP grouping helpers — moved from northd.c */

void
ecmp_groups_add_route(struct ecmp_groups_node *group,
                      const struct parsed_route *route)
{
    static struct vlog_rate_limit rl = VLOG_RATE_LIMIT_INIT(5, 1);
    if (group->route_count == UINT16_MAX) {
        VLOG_WARN_RL(&rl, "too many routes in a single ecmp group.");
        return;
    }

    if (route->is_discard_route) {
        group->has_discard_route = true;

        char *prefix = normalize_v46_prefix(&route->prefix, route->plen);
        VLOG_WARN_RL(&rl, "The ECMP route \"%s\" contains \"discard\" "
                     "route, the whole group will drop traffic.", prefix);
        free(prefix);
    }

    struct ecmp_route_list_node *er = xmalloc(sizeof *er);
    er->route = route;
    er->id = ++group->route_count;
    ovs_list_insert(&group->route_list, &er->list_node);
}

struct ecmp_groups_node *
ecmp_groups_add(struct group_ecmp_datapath *ged,
                const struct parsed_route *route)
{
    if (hmap_count(&ged->ecmp_groups) == UINT16_MAX
        || ged->next_ecmp_id == UINT16_MAX) {
        /* The id is embedded in REG_ECMP_GROUP_ID (16-bit), so exhausting
         * next_ecmp_id forces the caller to fall back to a full re-walk,
         * which resets the counter to 0. */
        static struct vlog_rate_limit rl = VLOG_RATE_LIMIT_INIT(5, 1);
        VLOG_WARN_RL(&rl,
                     "too many ecmp groups or group id space exhausted.");
        return NULL;
    }

    struct ecmp_groups_node *eg = xzalloc(sizeof *eg);
    hmap_insert(&ged->ecmp_groups, &eg->hmap_node, route->hash);

    eg->id = ++ged->next_ecmp_id;
    eg->prefix = route->prefix;
    eg->plen = route->plen;
    eg->is_src_route = route->is_src_route;
    eg->origin = smap_get_def(&route->route->options, "origin", "");
    eg->route_table_id = route->route_table_id;
    ovs_list_init(&eg->route_list);
    ecmp_groups_add_route(eg, route);

    return eg;
}

struct ecmp_groups_node *
ecmp_groups_find(struct hmap *ecmp_groups, struct parsed_route *route)
{
    struct ecmp_groups_node *eg;
    HMAP_FOR_EACH_WITH_HASH (eg, hmap_node, route->hash, ecmp_groups) {
        if (ipv6_addr_equals(&eg->prefix, &route->prefix) &&
            eg->plen == route->plen &&
            eg->is_src_route == route->is_src_route &&
            eg->route_table_id == route->route_table_id) {
            return eg;
        }
    }
    return NULL;
}

void
ecmp_groups_destroy(struct hmap *ecmp_groups)
{
    struct ecmp_groups_node *eg;
    HMAP_FOR_EACH_SAFE (eg, hmap_node, ecmp_groups) {
        struct ecmp_route_list_node *er;
        LIST_FOR_EACH_SAFE (er, list_node, &eg->route_list) {
            ovs_list_remove(&er->list_node);
            free(er);
        }
        hmap_remove(ecmp_groups, &eg->hmap_node);
        free(eg);
    }
    hmap_destroy(ecmp_groups);
}

void
unique_routes_add(struct hmap *unique_routes,
                  const struct parsed_route *route)
{
    struct unique_routes_node *ur = xmalloc(sizeof *ur);
    ur->route = route;
    hmap_insert(unique_routes, &ur->hmap_node, route->hash);
}

const struct parsed_route *
unique_routes_remove(struct hmap *unique_routes,
                     const struct parsed_route *route)
{
    struct unique_routes_node *ur;
    HMAP_FOR_EACH_WITH_HASH (ur, hmap_node, route->hash, unique_routes) {
        if (ipv6_addr_equals(&route->prefix, &ur->route->prefix) &&
            route->plen == ur->route->plen &&
            route->is_src_route == ur->route->is_src_route &&
            route->route_table_id == ur->route->route_table_id) {
            hmap_remove(unique_routes, &ur->hmap_node);
            const struct parsed_route *existed_route = ur->route;
            free(ur);
            return existed_route;
        }
    }
    return NULL;
}

void
unique_routes_destroy(struct hmap *unique_routes)
{
    struct unique_routes_node *ur;
    HMAP_FOR_EACH_SAFE (ur, hmap_node, unique_routes) {
        hmap_remove(unique_routes, &ur->hmap_node);
        free(ur);
    }
    hmap_destroy(unique_routes);
}

/* Per-datapath ECMP grouping container. */

static struct group_ecmp_datapath *
group_ecmp_datapath_add(struct group_ecmp_route_data *data,
                        const struct ovn_datapath *od)
{
    struct group_ecmp_datapath *ged = xzalloc(sizeof *ged);
    ged->od = od;
    hmap_init(&ged->ecmp_groups);
    hmap_init(&ged->unique_routes);
    ovs_list_init(&ged->parsed_routes);
    hmap_init(&ged->parsed_routes_by_uuid);
    hmap_init(&ged->route_nodes);
    hmap_insert(&data->datapaths, &ged->hmap_node,
                uuid_hash(&od->key));
    return ged;
}

struct group_ecmp_datapath *
group_ecmp_datapath_lookup(const struct group_ecmp_route_data *data,
                           const struct ovn_datapath *od)
{
    struct group_ecmp_datapath *ged;
    HMAP_FOR_EACH_WITH_HASH (ged, hmap_node, uuid_hash(&od->key),
                             &data->datapaths) {
        if (ged->od == od) {
            return ged;
        }
    }
    return NULL;
}

struct parsed_route *
parsed_route_lookup_by_uuid(const struct group_ecmp_datapath *ged,
                            const struct uuid *route_uuid)
{
    struct parsed_route *pr;
    HMAP_FOR_EACH_WITH_HASH (pr, key_node, uuid_hash(route_uuid),
                             &ged->parsed_routes_by_uuid) {
        if (uuid_equals(&pr->route->header_.uuid, route_uuid)) {
            return pr;
        }
    }
    return NULL;
}

/* Helpers used by both the full re-walk and the per-route delta path. */

static void
populate_route_tables_from_lrp_options(const struct ovn_datapath *od,
                                       struct simap *route_tables)
{
    for (int i = 0; i < od->nbr->n_ports; i++) {
        const char *rt = smap_get(&od->nbr->ports[i]->options,
                                  "route_table");
        if (rt && rt[0]) {
            get_route_table_id(route_tables, rt);
        }
    }
}

/* Allocate a route_node owning a fresh lflow_ref, insert into ged->route_nodes
 * keyed by the right hash, and return it. For ECMP groups, payload is the
 * ecmp_groups_node *; for unique routes, payload is the parsed_route *.
 * Does NOT set parsed_route back-pointers — caller is responsible. */
static struct ecmp_route_node *
allocate_route_node(struct group_ecmp_datapath *ged, bool is_ecmp,
                    const void *payload)
{
    struct ecmp_route_node *rn = xzalloc(sizeof *rn);
    rn->od = ged->od;
    rn->lflow_ref = lflow_ref_create();
    rn->is_ecmp = is_ecmp;
    uint32_t hash;
    if (is_ecmp) {
        rn->group = CONST_CAST(struct ecmp_groups_node *, payload);
        hash = uuid_hash(&ged->od->key) ^ rn->group->id;
    } else {
        rn->route = (const struct parsed_route *) payload;
        hash = uuid_hash(&rn->route->route->header_.uuid);
    }
    hmap_insert(&ged->route_nodes, &rn->hmap_node, hash);
    return rn;
}

/* Allocate a route_node and remember it in freshly_allocated. Used by the
 * per-route delta path so a later delta deleting the same route_node can
 * recognize it as fresh (no live SB flows yet) and free it directly,
 * rather than putting it in deleted_datapath_routes where en-lflow would
 * also iterate it as crupdated → use-after-free. */
static struct ecmp_route_node *
allocate_route_node_tracked(struct group_ecmp_datapath *ged,
                            struct group_ecmp_route_data *data,
                            struct hmapx *freshly_allocated,
                            bool is_ecmp, const void *payload)
{
    struct ecmp_route_node *rn = allocate_route_node(ged, is_ecmp, payload);
    hmapx_add(freshly_allocated, rn);
    hmapx_add(&data->trk_data.crupdated_datapath_routes, rn);
    return rn;
}

/* Transition rn out of "live" state.
 *
 * If rn was freshly allocated this handler invocation, its lflow_ref has
 * never been visited by en-lflow → no SB flows registered → safe to free
 * directly. Otherwise hand off to deleted_datapath_routes for en-lflow to
 * unlink the SB flows it owns.
 *
 * In both cases, ensure rn is no longer in crupdated_datapath_routes so the
 * invariant "no rn in both sets" holds when en-lflow iterates them. */
static void
route_node_retire(struct group_ecmp_route_data *data,
                  struct hmapx *freshly_allocated,
                  struct ecmp_route_node *rn)
{
    hmapx_find_and_delete(&data->trk_data.crupdated_datapath_routes, rn);
    if (hmapx_find_and_delete(freshly_allocated, rn)) {
        lflow_ref_destroy(rn->lflow_ref);
        free(rn);
        return;
    }
    hmapx_add(&data->trk_data.deleted_datapath_routes, rn);
}

/* Find the route_node that backs an ECMP group within a ged. */
static struct ecmp_route_node *
group_route_node_of(const struct group_ecmp_datapath *ged,
                    const struct ecmp_groups_node *eg)
{
    uint32_t hash = uuid_hash(&ged->od->key) ^ eg->id;
    struct ecmp_route_node *rn;
    HMAP_FOR_EACH_WITH_HASH (rn, hmap_node, hash, &ged->route_nodes) {
        if (rn->is_ecmp && rn->group == eg) {
            return rn;
        }
    }
    return NULL;
}

/* Remove pr from an ECMP group's route_list. Updates route_count and
 * recomputes has_discard_route. Returns the count after removal (>= 0). */
static uint16_t
ecmp_group_remove_route(struct ecmp_groups_node *eg,
                        const struct parsed_route *pr)
{
    struct ecmp_route_list_node *er;
    LIST_FOR_EACH_SAFE (er, list_node, &eg->route_list) {
        if (er->route == pr) {
            ovs_list_remove(&er->list_node);
            free(er);
            eg->route_count--;
            break;
        }
    }
    /* Recompute has_discard_route flag from remaining members. */
    eg->has_discard_route = false;
    LIST_FOR_EACH (er, list_node, &eg->route_list) {
        if (er->route->is_discard_route) {
            eg->has_discard_route = true;
            break;
        }
    }
    return eg->route_count;
}

/* Per-route delta handlers. Each takes the engine data and the ged it
 * operates on; they mutate ged in place and push tracked-data entries for
 * en-lflow to consume. Returns false on unrecoverable inconsistency, in
 * which case the caller MUST NOT claim the LR for the delta path — letting
 * the LR re-walk fix the bookkeeping. */

static bool
handle_route_delete_delta(struct group_ecmp_route_data *data,
                          struct group_ecmp_datapath *ged,
                          struct hmapx *freshly_allocated,
                          struct parsed_route *pr)
{
    struct ecmp_route_node *rn = pr->route_node;
    if (!rn) {
        return false;
    }

    if (rn->is_ecmp) {
        struct ecmp_groups_node *eg = rn->group;
        uint16_t remaining = ecmp_group_remove_route(eg, pr);

        if (remaining >= 2) {
            /* Group still ECMP; flow content (bundle members) changed.
             * Keep the same route_node and lflow_ref pointer; mark
             * crupdated so en-lflow re-emits. */
            hmapx_add(&data->trk_data.crupdated_datapath_routes, rn);
        } else if (remaining == 1) {
            /* Demote group → unique route. */
            struct ecmp_route_list_node *survivor_er = CONTAINER_OF(
                ovs_list_front(&eg->route_list),
                struct ecmp_route_list_node, list_node);
            const struct parsed_route *surviving = survivor_er->route;
            ovs_list_remove(&survivor_er->list_node);
            free(survivor_er);
            hmap_remove(&ged->ecmp_groups, &eg->hmap_node);
            free(eg);

            /* Old ECMP route_node dies. */
            hmap_remove(&ged->route_nodes, &rn->hmap_node);
            route_node_retire(data, freshly_allocated, rn);

            /* Promote survivor to unique_routes with a new route_node. */
            unique_routes_add(&ged->unique_routes, surviving);
            struct ecmp_route_node *new_rn = allocate_route_node_tracked(
                ged, data, freshly_allocated,
                /*is_ecmp=*/false, surviving);
            CONST_CAST(struct parsed_route *, surviving)->route_node = new_rn;
        } else {
            /* Group empty; destroy it and the route_node. */
            hmap_remove(&ged->ecmp_groups, &eg->hmap_node);
            free(eg);
            hmap_remove(&ged->route_nodes, &rn->hmap_node);
            route_node_retire(data, freshly_allocated, rn);
        }
    } else {
        /* Unique route — straight delete. */
        unique_routes_remove(&ged->unique_routes, pr);
        hmap_remove(&ged->route_nodes, &rn->hmap_node);
        route_node_retire(data, freshly_allocated, rn);
    }

    /* Tear down the parsed_route itself. */
    hmap_remove(&ged->parsed_routes_by_uuid, &pr->key_node);
    ovs_list_remove(&pr->list_node);
    pr->route_node = NULL;
    free(pr);
    return true;
}

static bool
handle_route_add_delta(struct group_ecmp_route_data *data,
                       struct group_ecmp_datapath *ged,
                       struct hmapx *freshly_allocated,
                       const struct nbrec_logical_router_static_route *nb_route,
                       const struct hmap *lr_ports)
{
    struct simap route_tables = SIMAP_INITIALIZER(&route_tables);
    populate_route_tables_from_lrp_options(ged->od, &route_tables);

    struct parsed_route *pr = parsed_routes_add(
        CONST_CAST(struct ovn_datapath *, ged->od), lr_ports,
        &ged->parsed_routes, &route_tables, nb_route, NULL);
    simap_destroy(&route_tables);
    if (!pr) {
        /* Parse failed — the LR re-walk will produce the same outcome
         * (route silently omitted). Don't claim. */
        return false;
    }

    hmap_insert(&ged->parsed_routes_by_uuid, &pr->key_node,
                uuid_hash(&nb_route->header_.uuid));

    /* Case A: existing ECMP group at this prefix → join. */
    struct ecmp_groups_node *group =
        ecmp_groups_find(&ged->ecmp_groups, pr);
    if (group) {
        ecmp_groups_add_route(group, pr);
        struct ecmp_route_node *rn = group_route_node_of(ged, group);
        if (!rn) {
            return false;
        }
        pr->route_node = rn;
        hmapx_add(&data->trk_data.crupdated_datapath_routes, rn);
        return true;
    }

    /* Case B: existing unique route at same prefix → promote both to ECMP. */
    const struct parsed_route *existed =
        unique_routes_remove(&ged->unique_routes, pr);
    if (existed) {
        struct ecmp_route_node *old_rn =
            CONST_CAST(struct parsed_route *, existed)->route_node;
        if (old_rn) {
            hmap_remove(&ged->route_nodes, &old_rn->hmap_node);
            route_node_retire(data, freshly_allocated, old_rn);
        }

        group = ecmp_groups_add(ged, existed);
        if (!group) {
            return false;
        }
        ecmp_groups_add_route(group, pr);

        struct ecmp_route_node *new_rn = allocate_route_node_tracked(
            ged, data, freshly_allocated, /*is_ecmp=*/true, group);
        CONST_CAST(struct parsed_route *, existed)->route_node = new_rn;
        pr->route_node = new_rn;
        return true;
    }

    /* Case C: ecmp_symmetric_reply route → solo ECMP group of 1. */
    if (pr->ecmp_symmetric_reply) {
        group = ecmp_groups_add(ged, pr);
        if (!group) {
            return false;
        }
        struct ecmp_route_node *new_rn = allocate_route_node_tracked(
            ged, data, freshly_allocated, /*is_ecmp=*/true, group);
        pr->route_node = new_rn;
        return true;
    }

    /* Case D: plain unique route. */
    unique_routes_add(&ged->unique_routes, pr);
    struct ecmp_route_node *new_rn = allocate_route_node_tracked(
        ged, data, freshly_allocated, /*is_ecmp=*/false, pr);
    pr->route_node = new_rn;
    return true;
}

static bool
handle_route_modify_delta(struct group_ecmp_route_data *data,
                          struct group_ecmp_datapath *ged,
                          struct hmapx *freshly_allocated,
                          const struct nbrec_logical_router_static_route *nb_route,
                          const struct hmap *lr_ports)
{
    struct parsed_route *old_pr =
        parsed_route_lookup_by_uuid(ged, &nb_route->header_.uuid);
    if (!old_pr) {
        return false;
    }
    if (!handle_route_delete_delta(data, ged, freshly_allocated, old_pr)) {
        return false;
    }
    return handle_route_add_delta(data, ged, freshly_allocated,
                                  nb_route, lr_ports);
}

static void
group_ecmp_datapath_destroy(struct group_ecmp_datapath *ged)
{
    ecmp_groups_destroy(&ged->ecmp_groups);
    unique_routes_destroy(&ged->unique_routes);
    parsed_routes_destroy(&ged->parsed_routes);
    hmap_destroy(&ged->parsed_routes_by_uuid);

    struct ecmp_route_node *rn;
    HMAP_FOR_EACH_POP (rn, hmap_node, &ged->route_nodes) {
        lflow_ref_destroy(rn->lflow_ref);
        free(rn);
    }
    hmap_destroy(&ged->route_nodes);

    free(ged);
}

static void
group_ecmp_route_clear(struct group_ecmp_route_data *data)
{
    struct group_ecmp_datapath *ged;
    HMAP_FOR_EACH_POP (ged, hmap_node, &data->datapaths) {
        group_ecmp_datapath_destroy(ged);
    }
}

/* Build ECMP groups for a single router datapath. */
static void
group_ecmp_route(struct group_ecmp_route_data *data,
                 struct ovn_datapath *od,
                 const struct hmap *lr_ports,
                 const struct hmap *bfd_connections)
{
    if (!od->nbr || od->nbr->n_static_routes == 0) {
        return;
    }

    struct group_ecmp_datapath *ged = group_ecmp_datapath_add(data, od);
    struct simap route_tables = SIMAP_INITIALIZER(&route_tables);

    /* Pre-populate route_tables from LRP options so IDs match the
     * lr_in_ip_routing_pre flows built by build_route_table_lflow(). */
    for (int i = 0; i < od->nbr->n_ports; i++) {
        const char *rt = smap_get(&od->nbr->ports[i]->options,
                                  "route_table");
        if (rt && rt[0]) {
            get_route_table_id(&route_tables, rt);
        }
    }

    for (int i = 0; i < od->nbr->n_static_routes; i++) {
        struct parsed_route *route =
            parsed_routes_add(od, lr_ports, &ged->parsed_routes,
                              &route_tables,
                              od->nbr->static_routes[i],
                              bfd_connections);
        if (!route) {
            continue;
        }
        hmap_insert(&ged->parsed_routes_by_uuid, &route->key_node,
                    uuid_hash(&od->nbr->static_routes[i]->header_.uuid));
        struct ecmp_groups_node *group =
            ecmp_groups_find(&ged->ecmp_groups, route);
        if (group) {
            ecmp_groups_add_route(group, route);
        } else {
            const struct parsed_route *existed_route =
                unique_routes_remove(&ged->unique_routes, route);
            if (existed_route) {
                group = ecmp_groups_add(ged, existed_route);
                if (group) {
                    ecmp_groups_add_route(group, route);
                }
            } else if (route->ecmp_symmetric_reply) {
                ecmp_groups_add(ged, route);
            } else {
                unique_routes_add(&ged->unique_routes, route);
            }
        }
    }
    simap_destroy(&route_tables);

    /* Create per-route/group nodes with independent lflow_refs. */
    struct ecmp_groups_node *eg;
    HMAP_FOR_EACH (eg, hmap_node, &ged->ecmp_groups) {
        struct ecmp_route_node *rn = xzalloc(sizeof *rn);
        rn->od = od;
        rn->lflow_ref = lflow_ref_create();
        rn->is_ecmp = true;
        rn->group = eg;
        hmap_insert(&ged->route_nodes, &rn->hmap_node,
                    uuid_hash(&od->key) ^ eg->id);
        /* Set back-pointer on every group member's parsed_route. */
        struct ecmp_route_list_node *er;
        LIST_FOR_EACH (er, list_node, &eg->route_list) {
            CONST_CAST(struct parsed_route *, er->route)->route_node = rn;
        }
    }
    const struct unique_routes_node *ur;
    HMAP_FOR_EACH (ur, hmap_node, &ged->unique_routes) {
        struct ecmp_route_node *rn = xzalloc(sizeof *rn);
        rn->od = od;
        rn->lflow_ref = lflow_ref_create();
        rn->is_ecmp = false;
        rn->route = ur->route;
        hmap_insert(&ged->route_nodes, &rn->hmap_node,
                    uuid_hash(&ur->route->route->header_.uuid));
        CONST_CAST(struct parsed_route *, ur->route)->route_node = rn;
    }
}

/* Engine node functions. */

void *
en_group_ecmp_route_init(struct engine_node *node OVS_UNUSED,
                         struct engine_arg *arg OVS_UNUSED)
{
    struct group_ecmp_route_data *data = xzalloc(sizeof *data);
    hmap_init(&data->datapaths);
    hmapx_init(&data->trk_data.deleted_datapath_routes);
    hmapx_init(&data->trk_data.crupdated_datapath_routes);
    return data;
}

void
en_group_ecmp_route_cleanup(void *data_)
{
    struct group_ecmp_route_data *data = data_;
    group_ecmp_route_clear(data);
    hmap_destroy(&data->datapaths);
    hmapx_destroy(&data->trk_data.deleted_datapath_routes);
    hmapx_destroy(&data->trk_data.crupdated_datapath_routes);
}

void
en_group_ecmp_route_clear_tracked_data(void *data_)
{
    struct group_ecmp_route_data *data = data_;

    struct hmapx_node *node;
    HMAPX_FOR_EACH_SAFE (node, &data->trk_data.deleted_datapath_routes) {
        struct ecmp_route_node *rn = node->data;
        lflow_ref_destroy(rn->lflow_ref);
        free(rn);
    }
    hmapx_clear(&data->trk_data.deleted_datapath_routes);
    hmapx_clear(&data->trk_data.crupdated_datapath_routes);
}

void
en_group_ecmp_route_run(struct engine_node *node, void *data_)
{
    struct group_ecmp_route_data *data = data_;
    group_ecmp_route_clear(data);

    struct northd_data *northd_data = engine_get_input_data("northd", node);

    struct ovn_datapath *od;
    HMAP_FOR_EACH (od, key_node, &northd_data->lr_datapaths.datapaths) {
        if (!od->nbr) {
            continue;
        }
        group_ecmp_route(data, od, &northd_data->lr_ports, NULL);
    }

    engine_set_node_state(node, EN_UPDATED);
}

bool
en_group_ecmp_route_northd_handler(struct engine_node *node, void *data_)
{
    struct northd_data *northd_data = engine_get_input_data("northd", node);
    if (!northd_has_tracked_data(&northd_data->trk_data)) {
        return false;
    }

    if (northd_data->trk_data.type & NORTHD_TRACKED_LR_DELETED) {
        return false;
    }

    struct group_ecmp_route_data *data = data_;

    /* Per-route delta path: O(K) updates touching only the affected
     * route_nodes. LRs claimed here are skipped by the per-LR re-walk
     * below. Any LR for which a delta helper returns false stays
     * unclaimed → re-walk handles it for safety.
     *
     * freshly_allocated tracks route_nodes created during this handler
     * invocation. If a later delta in the same invocation deletes one,
     * route_node_retire() frees it directly instead of moving it to
     * deleted_datapath_routes — preventing en-lflow from iterating a
     * route_node whose parsed_route has already been freed. */
    struct hmapx handled_by_delta = HMAPX_INITIALIZER(&handled_by_delta);
    struct hmapx freshly_allocated = HMAPX_INITIALIZER(&freshly_allocated);
    if (northd_data->trk_data.type & NORTHD_TRACKED_LR_ROUTES_DELTA) {
        /* DELETIONS first — may demote ECMP groups to unique, providing a
         * clean baseline for any subsequent adds at the same prefix. */
        struct deleted_route_node *drn;
        HMAP_FOR_EACH (drn, node,
                       &northd_data->trk_data.trk_routes_deleted) {
            struct group_ecmp_datapath *ged =
                group_ecmp_datapath_lookup(data, drn->od);
            if (!ged) {
                continue;
            }
            struct parsed_route *pr =
                parsed_route_lookup_by_uuid(ged, &drn->route_uuid);
            if (!pr) {
                continue;
            }
            if (handle_route_delete_delta(data, ged, &freshly_allocated,
                                          pr)) {
                hmapx_add(&handled_by_delta, drn->od);
            }
        }

        /* MODIFICATIONS — delete + add of the same UUID. */
        struct modified_route_node *mrn;
        HMAP_FOR_EACH (mrn, node,
                       &northd_data->trk_data.trk_routes_modified) {
            struct group_ecmp_datapath *ged =
                group_ecmp_datapath_lookup(data, mrn->od);
            if (!ged) {
                continue;
            }
            if (handle_route_modify_delta(data, ged, &freshly_allocated,
                                          mrn->nb_route,
                                          &northd_data->lr_ports)) {
                hmapx_add(&handled_by_delta, mrn->od);
            }
        }

        /* ADDITIONS. */
        struct hmapx_node *hn;
        HMAPX_FOR_EACH (hn, &northd_data->trk_data.trk_routes_added) {
            const struct nbrec_logical_router_static_route *nb_route =
                hn->data;
            struct ovn_datapath *od = route_to_lr_map_find(
                &northd_data->route_to_lr_map, nb_route);
            if (!od) {
                continue;
            }
            struct group_ecmp_datapath *ged =
                group_ecmp_datapath_lookup(data, od);
            if (!ged) {
                /* New-LR-with-routes — NORTHD_TRACKED_LR_CREATED branch
                 * below will build the whole ged from scratch. Don't
                 * claim. */
                continue;
            }
            if (handle_route_add_delta(data, ged, &freshly_allocated,
                                       nb_route,
                                       &northd_data->lr_ports)) {
                hmapx_add(&handled_by_delta, od);
            }
        }

        if (!hmapx_is_empty(&data->trk_data.crupdated_datapath_routes) ||
            !hmapx_is_empty(&data->trk_data.deleted_datapath_routes)) {
            engine_set_node_state(node, EN_UPDATED);
        }
    }
    hmapx_destroy(&freshly_allocated);

    if (northd_data->trk_data.type & NORTHD_TRACKED_LR_ROUTES) {
        struct hmapx_node *hmapx_node;
        HMAPX_FOR_EACH (hmapx_node,
                        &northd_data->trk_data.lr_with_changed_routes) {
            struct ovn_datapath *od = hmapx_node->data;
            if (hmapx_contains(&handled_by_delta, od)) {
                /* Already handled by per-route delta path. */
                continue;
            }
            struct group_ecmp_datapath *ged =
                group_ecmp_datapath_lookup(data, od);

            if (ged) {
                /* Save old route_nodes for lflow_ref reuse. Remove from
                 * route_nodes hmap so they don't get iterated by
                 * build_static_route_flows_for_lrouter if en_lflow_run
                 * does a full recompute. */
                struct hmap old_route_nodes = HMAP_INITIALIZER(
                    &old_route_nodes);
                struct ecmp_route_node *rn;
                HMAP_FOR_EACH_SAFE (rn, hmap_node, &ged->route_nodes) {
                    hmap_remove(&ged->route_nodes, &rn->hmap_node);
                    hmap_insert(&old_route_nodes, &rn->hmap_node,
                                rn->hmap_node.hash);
                }

                /* Keep the old parsed_routes alive until the lflow_ref
                 * matching loop below; old_rn->route still points into
                 * them. */
                struct ovs_list old_parsed_routes =
                    OVS_LIST_INITIALIZER(&old_parsed_routes);
                ovs_list_push_back_all(&old_parsed_routes,
                                       &ged->parsed_routes);

                ecmp_groups_destroy(&ged->ecmp_groups);
                hmap_init(&ged->ecmp_groups);
                /* Re-walk rebuilds groups from scratch; reset the id
                 * counter so ids are sequential and don't drift across
                 * recompute cycles. */
                ged->next_ecmp_id = 0;
                unique_routes_destroy(&ged->unique_routes);
                hmap_init(&ged->unique_routes);
                hmap_destroy(&ged->parsed_routes_by_uuid);
                hmap_init(&ged->parsed_routes_by_uuid);

                /* Rebuild ECMP groups from current routes. */
                struct simap route_tables = SIMAP_INITIALIZER(&route_tables);
                for (int i = 0; i < od->nbr->n_ports; i++) {
                    const char *rt = smap_get(
                        &od->nbr->ports[i]->options, "route_table");
                    if (rt && rt[0]) {
                        get_route_table_id(&route_tables, rt);
                    }
                }
                for (int i = 0; i < od->nbr->n_static_routes; i++) {
                    struct parsed_route *route = parsed_routes_add(
                        od, &northd_data->lr_ports, &ged->parsed_routes,
                        &route_tables, od->nbr->static_routes[i], NULL);
                    if (!route) {
                        continue;
                    }
                    hmap_insert(&ged->parsed_routes_by_uuid,
                                &route->key_node,
                                uuid_hash(&od->nbr->static_routes[i]
                                           ->header_.uuid));
                    struct ecmp_groups_node *group =
                        ecmp_groups_find(&ged->ecmp_groups, route);
                    if (group) {
                        ecmp_groups_add_route(group, route);
                    } else {
                        const struct parsed_route *existed =
                            unique_routes_remove(&ged->unique_routes, route);
                        if (existed) {
                            group = ecmp_groups_add(ged, existed);
                            if (group) {
                                ecmp_groups_add_route(group, route);
                            }
                        } else if (route->ecmp_symmetric_reply) {
                            ecmp_groups_add(ged, route);
                        } else {
                            unique_routes_add(&ged->unique_routes, route);
                        }
                    }
                }
                simap_destroy(&route_tables);

                /* Create new route_nodes. Reuse lflow_refs from old
                 * route_nodes when the route is unchanged (same UUID
                 * for unique routes). ECMP groups always get new
                 * lflow_refs since membership may have changed. */
                struct ecmp_groups_node *eg;
                HMAP_FOR_EACH (eg, hmap_node, &ged->ecmp_groups) {
                    uint32_t hash = uuid_hash(&od->key) ^ eg->id;
                    rn = xzalloc(sizeof *rn);
                    rn->od = od;
                    rn->is_ecmp = true;
                    rn->group = eg;
                    rn->lflow_ref = lflow_ref_create();
                    hmap_insert(&ged->route_nodes, &rn->hmap_node, hash);
                    hmapx_add(&data->trk_data.crupdated_datapath_routes, rn);
                    /* Set back-pointer on every group member. */
                    struct ecmp_route_list_node *er;
                    LIST_FOR_EACH (er, list_node, &eg->route_list) {
                        CONST_CAST(struct parsed_route *,
                                   er->route)->route_node = rn;
                    }
                }
                const struct unique_routes_node *ur;
                HMAP_FOR_EACH (ur, hmap_node, &ged->unique_routes) {
                    uint32_t hash = uuid_hash(
                        &ur->route->route->header_.uuid);
                    rn = xzalloc(sizeof *rn);
                    rn->od = od;
                    rn->is_ecmp = false;
                    rn->route = ur->route;

                    /* Try to reuse lflow_ref from matching old node. */
                    struct ecmp_route_node *old_rn;
                    HMAP_FOR_EACH_WITH_HASH (old_rn, hmap_node, hash,
                                             &old_route_nodes) {
                        if (!old_rn->is_ecmp && uuid_equals(
                                &old_rn->route->route->header_.uuid,
                                &ur->route->route->header_.uuid)) {
                            break;
                        }
                    }
                    if (old_rn) {
                        rn->lflow_ref = old_rn->lflow_ref;
                        old_rn->lflow_ref = NULL;
                        hmap_remove(&old_route_nodes, &old_rn->hmap_node);
                        free(old_rn);
                    } else {
                        rn->lflow_ref = lflow_ref_create();
                        hmapx_add(
                            &data->trk_data.crupdated_datapath_routes, rn);
                    }

                    hmap_insert(&ged->route_nodes, &rn->hmap_node, hash);
                    CONST_CAST(struct parsed_route *,
                               ur->route)->route_node = rn;
                }

                /* Remaining old route_nodes are deleted routes. */
                HMAP_FOR_EACH_SAFE (rn, hmap_node, &old_route_nodes) {
                    hmap_remove(&old_route_nodes, &rn->hmap_node);
                    hmapx_add(&data->trk_data.deleted_datapath_routes, rn);
                }
                hmap_destroy(&old_route_nodes);
                /* Old parsed_routes' back-pointers may point to old_rn's
                 * being freed at clear_tracked_data time. NULL them here
                 * for hygiene before destroying the snapshot list — though
                 * they are not dereferenced (not in parsed_routes_by_uuid)
                 * during the window. */
                struct parsed_route *opr;
                LIST_FOR_EACH (opr, list_node, &old_parsed_routes) {
                    opr->route_node = NULL;
                }
                parsed_routes_destroy(&old_parsed_routes);
            } else {
                /* Router had no routes before, build from scratch. */
                group_ecmp_route(data, od, &northd_data->lr_ports, NULL);
                ged = group_ecmp_datapath_lookup(data, od);
                if (ged) {
                    struct ecmp_route_node *rn;
                    HMAP_FOR_EACH (rn, hmap_node, &ged->route_nodes) {
                        hmapx_add(
                            &data->trk_data.crupdated_datapath_routes, rn);
                    }
                }
            }
        }
        engine_set_node_state(node, EN_UPDATED);
    }

    if (northd_data->trk_data.type & NORTHD_TRACKED_LR_CREATED) {
        struct hmapx_node *hmapx_node;
        HMAPX_FOR_EACH (hmapx_node,
                        &northd_data->trk_data.trk_created_lrs) {
            struct ovn_datapath *od = hmapx_node->data;
            if (od->nbr && od->nbr->n_static_routes > 0
                && !group_ecmp_datapath_lookup(data, od)) {
                group_ecmp_route(data, od, &northd_data->lr_ports, NULL);
                struct group_ecmp_datapath *ged =
                    group_ecmp_datapath_lookup(data, od);
                if (ged) {
                    struct ecmp_route_node *rn;
                    HMAP_FOR_EACH (rn, hmap_node, &ged->route_nodes) {
                        hmapx_add(
                            &data->trk_data.crupdated_datapath_routes, rn);
                    }
                }
            }
        }
        engine_set_node_state(node, EN_UPDATED);
    }

    hmapx_destroy(&handled_by_delta);
    return true;
}
