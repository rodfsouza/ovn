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
ecmp_groups_add(struct hmap *ecmp_groups,
                const struct parsed_route *route)
{
    if (hmap_count(ecmp_groups) == UINT16_MAX) {
        static struct vlog_rate_limit rl = VLOG_RATE_LIMIT_INIT(5, 1);
        VLOG_WARN_RL(&rl, "too many ecmp groups.");
        return NULL;
    }

    struct ecmp_groups_node *eg = xzalloc(sizeof *eg);
    hmap_insert(ecmp_groups, &eg->hmap_node, route->hash);

    eg->id = hmap_count(ecmp_groups);
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

static void
group_ecmp_datapath_destroy(struct group_ecmp_datapath *ged)
{
    ecmp_groups_destroy(&ged->ecmp_groups);
    unique_routes_destroy(&ged->unique_routes);
    parsed_routes_destroy(&ged->parsed_routes);
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

    for (int i = 0; i < od->nbr->n_static_routes; i++) {
        struct parsed_route *route =
            parsed_routes_add(od, lr_ports, &ged->parsed_routes,
                              &route_tables,
                              od->nbr->static_routes[i],
                              bfd_connections);
        if (!route) {
            continue;
        }
        struct ecmp_groups_node *group =
            ecmp_groups_find(&ged->ecmp_groups, route);
        if (group) {
            ecmp_groups_add_route(group, route);
        } else {
            const struct parsed_route *existed_route =
                unique_routes_remove(&ged->unique_routes, route);
            if (existed_route) {
                group = ecmp_groups_add(&ged->ecmp_groups, existed_route);
                if (group) {
                    ecmp_groups_add_route(group, route);
                }
            } else if (route->ecmp_symmetric_reply) {
                ecmp_groups_add(&ged->ecmp_groups, route);
            } else {
                unique_routes_add(&ged->unique_routes, route);
            }
        }
    }
    simap_destroy(&route_tables);
}

/* Engine node functions. */

void *
en_group_ecmp_route_init(struct engine_node *node OVS_UNUSED,
                         struct engine_arg *arg OVS_UNUSED)
{
    struct group_ecmp_route_data *data = xzalloc(sizeof *data);
    hmap_init(&data->datapaths);
    return data;
}

void
en_group_ecmp_route_cleanup(void *data_)
{
    struct group_ecmp_route_data *data = data_;
    group_ecmp_route_clear(data);
    hmap_destroy(&data->datapaths);
}

void
en_group_ecmp_route_clear_tracked_data(void *data OVS_UNUSED)
{
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
