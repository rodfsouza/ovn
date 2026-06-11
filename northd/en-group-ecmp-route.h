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

#ifndef EN_GROUP_ECMP_ROUTE_H
#define EN_GROUP_ECMP_ROUTE_H 1

#include <config.h>

#include "lib/inc-proc-eng.h"
#include "openvswitch/hmap.h"
#include "openvswitch/list.h"
#include "include/ovn/expr.h"

struct nbrec_logical_router_static_route;
struct ovn_datapath;
struct parsed_route;

struct ecmp_route_list_node {
    struct ovs_list list_node;
    uint16_t id;
    const struct parsed_route *route;
};

struct ecmp_groups_node {
    struct hmap_node hmap_node;
    uint16_t id;
    struct in6_addr prefix;
    unsigned int plen;
    bool is_src_route;
    bool has_discard_route;
    const char *origin;
    uint32_t route_table_id;
    uint16_t route_count;
    struct ovs_list route_list;
};

struct unique_routes_node {
    struct hmap_node hmap_node;
    const struct parsed_route *route;
};

struct group_ecmp_datapath {
    struct hmap_node hmap_node;
    const struct ovn_datapath *od;
    struct hmap ecmp_groups;
    struct hmap unique_routes;
    struct ovs_list parsed_routes;
};

struct group_ecmp_route_data {
    struct hmap datapaths;
};

void *en_group_ecmp_route_init(struct engine_node *, struct engine_arg *);
void en_group_ecmp_route_cleanup(void *data);
void en_group_ecmp_route_clear_tracked_data(void *data);
void en_group_ecmp_route_run(struct engine_node *, void *data);

struct group_ecmp_datapath *group_ecmp_datapath_lookup(
    const struct group_ecmp_route_data *data,
    const struct ovn_datapath *od);

void ecmp_groups_add_route(struct ecmp_groups_node *group,
                           const struct parsed_route *route);
struct ecmp_groups_node *ecmp_groups_add(struct hmap *ecmp_groups,
                                        const struct parsed_route *route);
struct ecmp_groups_node *ecmp_groups_find(struct hmap *ecmp_groups,
                                         struct parsed_route *route);
void ecmp_groups_destroy(struct hmap *ecmp_groups);
void unique_routes_add(struct hmap *unique_routes,
                       const struct parsed_route *route);
const struct parsed_route *unique_routes_remove(
    struct hmap *unique_routes, const struct parsed_route *route);
void unique_routes_destroy(struct hmap *unique_routes);

#endif /* EN_GROUP_ECMP_ROUTE_H */
