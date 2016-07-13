import numpy as np


def merge_two(route_a, route_b, all_orders, o2o_orders, last_a=-1, last_b=-1, max_load=140, time_lim=720):
    # route=[noteIDs, arrive minutes, leave minutes, package numbers, order IDs, order positions]
    # last_a and last_b denote the last pickup nodes in the routes
    # first found the last site to obtain packages if last=-1, because the route always end with dispatching packages
    if last_a == -1:
        last_a = find_last(route_a)
    if last_b == -1:
        last_b = find_last(route_b)
    print(last_a)
    return (merge_order(route_a, route_b, all_orders, o2o_orders, last_a, max_load, time_lim) +
            merge_order(route_b, route_a, all_orders, o2o_orders, last_b, max_load, time_lim))


def merge_order(ra, rb, all_orders, o2o_orders, last_a, max_load=140, time_lim=720):
    merge_r = []
    return merge_r


def find_last(r):
    # find the position of the last pickup node
    # return -1 if no pickup
    # 0 package number takes as pickup
    pn = r[3]
    last = len(pn)-1
    while last >= 0 > pn[last]:
        last -= 1
    return last


def stay_time(pack_num):
    return np.round(3.0*np.sqrt(pack_num)+5.0)
