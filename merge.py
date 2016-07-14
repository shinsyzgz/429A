import numpy as np

SPEED = 250


def merge_two(route_a, route_b, all_orders, last_a=-1, last_b=-1, undelivered_a=[], undelivered_b=[], max_load=140, time_lim=720):
    # route=[noteIDs, arrive minutes, leave minutes, package numbers, order IDs]
    # last_a and last_b denote the last pickup nodes in the routes
    # undelivered = [package number left, [list of order IDs left]]
    # all_orders is pandas object
    # first found the last site to obtain packages if last=-1, because the route always end with dispatching packages
    if last_a == -1:
        last_a, undelivered_a = find_last(route_a)
    if last_b == -1:
        last_b, undelivered_b = find_last(route_b)
    print(last_a)
    return (merge_order(route_a, route_b, all_orders, last_a, undelivered_a, max_load, time_lim) +
            merge_order(route_b, route_a, all_orders, last_b, undelivered_b, max_load, time_lim))


def merge_order(ra, rb, all_orders, last_a, und_a, max_load=140, time_lim=720):
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
    arr = [0.0]
    lea = [0.0]
    xl, yl = get_cor(r[4][0], allo)
    for i in range(1, rl):
        last_leave = np.round(lea[i-1])
        pck_num = r[3][i]
        ord_id = r[4][i]
        if pck_num < 0:
            # delivery, arr = last + travel, leave=arr+holding
            xd, yd = get_cor(ord_id, allo, 1)
            arr_time = last_leave + travel_time(node_dis(xl, yl, xd, yd))
            arr.append(arr_time)
            lea.append(arr_time + stay_time(-pck_num))
            xl, yl = xd, yd
        elif allo.at[ord_id, 'order_type'] == 0:
            # pickup at site, arr=last+travel, leave=arr
            xd, yd = get_cor(ord_id, allo, 0)
            arr_time = last_leave + travel_time(node_dis(xl, yl, xd, yd))
            arr.append(arr_time)
            lea.append(arr_time + 0.0)
            xl, yl = xd, yd
        else:
            # pickup at O2O, arr_time = (last + travel), leave_time = max(arr_time, pickup time) remember round
            xd, yd = get_cor(ord_id, allo, 0)
            arr_time = last_leave + travel_time(node_dis(xl, yl, xd, yd))
            arr.append(arr_time)
            lea.append(max(arr_time, allo.at[ord_id, 'pickup_time']))
            xl, yl = xd, yd
    r[1] = arr
    r[2] = lea
    return r


def get_cor(order_id, allo, o_type=0):
    # type=0: pickup; else: delivery
    if o_type == 0:
        return allo.at[order_id, 'ox'], allo.at[order_id, 'oy']
    return allo.at[order_id, 'dx'], allo.at[order_id, 'dy']
