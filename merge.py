import numpy as np

# Constant: speed is the car speed. MERGE_MAX_DISTANCE is the max distance to merge
SPEED = 250
MERGE_MAX_DISTANCE = 45000


def merge_two(route_a, route_b, all_orders, last_a=-1, last_b=-1, undelivered_a=[], undelivered_b=[], max_load=140, time_lim=720):
    # route=[noteIDs, arrive minutes, leave minutes, package numbers, order IDs, [last_a, undelivered_a](optional)]
    # last_a and last_b denote the last pickup nodes in the routes
    # undelivered = [package number left, [list of order IDs left]]
    # all_orders is pandas object
    # first found the last site to obtain packages if last=-1, because the route always end with dispatching packages
    if last_a == -1:
        last_a, undelivered_a = find_last(route_a)
    if last_b == -1:
        last_b, undelivered_b = find_last(route_b)
    print(last_a)
    return (merge_order(route_a, route_b, all_orders, last_a, undelivered_a, last_b, max_load, time_lim) +
            merge_order(route_b, route_a, all_orders, last_b, undelivered_b, last_a, max_load, time_lim))


def merge_order(ra, rb, all_orders, last_a, und_a, last_b, max_load=140, time_lim=720):
    # und_a = [pck n left, [list of order IDs left]]
    if len(ra[0] <= 0):
        # no new route generated
        return []
    # first renew rb to delete repeated node
    nrb = del_rep(ra, rb, all_orders, False)
    if len(nrb[0] <= 0):
        # no new route generated
        return []
    merge_r = []
    temp_ra = [ra[0][:(last_a+1)], ra[1][:(last_a+1)], ra[2][:(last_a+1)], ra[3][:(last_a+1)], ra[4][:(last_a+1)]]
    try_next(merge_r, temp_ra, rb, all_orders, und_a, last_b, max_load, time_lim)
    return merge_r


def try_next(res_routes, ra, rb, allo, und_a, last_b, max_load=140, time_lim=720):
    # First judge if it's the end of a merge
    if last_b < 0:
        ra = bb_tsp(ra, allo, und_a, time_lim)
        res_routes.append(ra)
        return
    # TBA try combine the following node in rb
    next_id, next_pck, next_oid = rb[0][0], rb[3][0], rb[4][0]
    is_suc, r_ab, und_ab = route_node_merge(ra, und_a, [next_id, next_pck, next_oid], allo)
    if is_suc:
        pass
    # TBA try combine the node in und_a
    pass


def bb_tsp(r, allo, und_a, time_lim=720):
    # TBA find the minimum delivery; remember to append [last_a, und_a] to r
    return r


def route_node_merge(r, und_a, node, allo):
    # TBA merge the node; remember deepcopy r and und_a
    # node=[node id, pack number, order id]
    return True, r, und_a


def find_last(r):
    # find the position of the last pickup node
    # return -1 if no pickup
    # 0 package number takes as pickup
    pn = r[3]
    last = len(pn)-1
    pkn = 0
    o_id = []
    while last >= 0 > pn[last]:
        pkn -= pn[last]
        o_id.append(r[4][last])
        last -= 1
    return last, [pkn, o_id]


def stay_time(pack_num):
    return np.round(3.0*np.sqrt(pack_num)+5.0)


def node_dis(ox, oy, dx, dy):
    return np.sqrt((ox-dx)**2+(oy-dy)**2)


def travel_time(dis, speed=SPEED):
    return np.round(dis/speed)


def del_rep(ra, rb, allo, recalculate_time=True):
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
    if recalculate_time:
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
    arr = [0.0]
    lea = [0.0]
    xl, yl = get_cor(r[4][0], allo)
    for i in range(1, rl):
        last_leave = np.round(lea[i-1])
        pck_num = r[3][i]
        ord_id = r[4][i]
        arr_time, lea_time, xl, yl = time_update(ord_id, pck_num, last_leave, xl, yl, allo)
        arr.append(arr_time)
        lea.append(lea_time)
    r[1] = arr
    r[2] = lea
    return r


def time_update(ord_id, pck_num, last_leave_time, last_x, last_y, allo):
    if pck_num < 0:
        # delivery, arr = last + travel, leave=arr+holding
        xd, yd = get_cor(ord_id, allo, 1)
        arr_time = last_leave_time + travel_time(node_dis(last_x, last_y, xd, yd))
        lea_time = arr_time + stay_time(-pck_num)
        return arr_time, lea_time, xd, yd
    elif allo.at[ord_id, 'order_type'] == 0:
        # pickup at site, arr=last+travel, leave=arr
        xd, yd = get_cor(ord_id, allo, 0)
        arr_time = last_leave_time + travel_time(node_dis(last_x, last_y, xd, yd))
        lea_time = arr_time + 0.0
        return arr_time, lea_time, xd, yd
    else:
        # pickup at O2O, arr_time = (last + travel), leave_time = max(arr_time, pickup time) remember round
        xd, yd = get_cor(ord_id, allo, 0)
        arr_time = last_leave_time + travel_time(node_dis(last_x, last_y, xd, yd))
        lea_time = max(arr_time, allo.at[ord_id, 'pickup_time'])
        return arr_time, lea_time, xd, yd


def get_cor(order_id, allo, o_type=0):
    # type=0: pickup; else: delivery
    if o_type == 0:
        return allo.at[order_id, 'ox'], allo.at[order_id, 'oy']
    return allo.at[order_id, 'dx'], allo.at[order_id, 'dy']
