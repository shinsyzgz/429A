import numpy as np


def merge_two(route_a, route_b, all_orders, last_a=-1, last_b=-1, max_load=140, time_lim=720):
    # route=[noteIDs, arrive minutes, leave minutes, package numbers, order IDs]
    # last_a and last_b denote the last pickup nodes in the routes
    # all_orders is pandas object
    # first found the last site to obtain packages if last=-1, because the route always end with dispatching packages
    if last_a == -1:
        last_a = find_last(route_a)
    if last_b == -1:
        last_b = find_last(route_b)
    print(last_a)
    return (merge_order(route_a, route_b, all_orders, last_a, max_load, time_lim) +
            merge_order(route_b, route_a, all_orders, last_b, max_load, time_lim))


def merge_order(ra, rb, all_orders, last_a, max_load=140, time_lim=720):
    if len(ra[0] <= 0):
        # no new route generated
        return []
    # first renew rb to delete repeated node
    nrb = del_rep(ra, rb, all_orders)
    if len(nrb[0] <= 0):
        # no new route generated
        return []
    merge_r = []
    # TBA combine ra and nrb
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


def del_rep(ra, rb, allo):
    # delete repeat order in rb respect to ra without changing rb
    nids = []
    arr = []
    lea = []
    pacn = []
    orid = []
    ra_order = ra[4]
    rb_order = rb[4]
    lenrb = len(rb_order)
    for i in range(lenrb):
        temp_o = rb_order[i]
        if not (temp_o in ra_order):
            nids.append(rb[0][i])
            pacn.append(rb[3][i])
            orid.append(temp_o)
    nrb = [nids, arr, lea, pacn, orid]
    nrb = recal_time(nrb, allo)
    return nrb


def recal_time(r, allo):
    # recalculate time in route r, and changing r itself
    # not counting the case that start with an O2O
    rl = len(r[0])
    if rl <= 0:
        return r
    if r[3][0] < 0:
        raise Exception('Wrong route, not start with a pickup')
    arr = [0]
    lea = [0]
    for i in range(1, rl):
        last_leave = lea[i-1]
        pck_num = r[3][i]
        ord_id = r[4][i]
        if pck_num < 0:
            # TBA delivery, arr_time = last_leave + travel, leave_time = arr_time + handling remember round
            pass
        elif allo.at[ord_id, 'order_type'] == 0:
            # TBA pickup at site, arr_time = last_leave + travel, leave_time = arr_time remember round
            pass
        else:
            # TBA pickup at O2O, arr_time = (last + travel), leave_time = max(arr_time, pickup time) remember round
            pass
        r[1] = arr
        r[2] = lea
    return r

