import numpy as np
import copy
import scipy.spatial.distance as sci_dis

# Constant: speed is the car speed. MERGE_MAX_DISTANCE is the max distance to merge
SPEED = 250.0
MERGE_MAX_DISTANCE = 45000.0
SITE_END_TIME = 720.0
MAX_LOADS = 140.0


def merge_two(route_a, route_b, all_orders, last_a=-1, last_b=-1, undelivered_a=None, undelivered_b=None,
              max_load=140, time_lim=720):
    # route=[noteIDs, arrive minutes, leave minutes, package numbers, order IDs, [last_a, undelivered_a](optional)]
    # last_a and last_b denote the last pickup nodes in the routes
    # undelivered = [package number left, [list of order IDs left]]
    # all_orders is pandas object
    # first found the last site to obtain packages if last=-1, because the route always end with dispatching packages
    if last_a == -1:
        last_a, undelivered_a = find_last(route_a)
    if last_b == -1:
        last_b, undelivered_b = find_last(route_b)
    return (merge_order(route_a, route_b, all_orders, last_a, undelivered_a, last_b, max_load, time_lim) +
            merge_order(route_b, route_a, all_orders, last_b, undelivered_b, last_a, max_load, time_lim))


def merge_order(ra, rb, all_orders, last_a, und_a, last_b, max_load=140, time_lim=720):
    # und_a = [pck n left, [list of order IDs left]]
    if len(ra[0]) <= 0:
        # no new route generated
        return []
    # first renew rb to delete repeated node
    nrb = del_rep(ra, rb, all_orders, False)
    if len(nrb[0]) <= 0:
        # no new route generated
        return []
    merge_r = []
    temp_ra = [ra[0][:(last_a+1)], ra[1][:(last_a+1)], ra[2][:(last_a+1)], ra[3][:(last_a+1)], ra[4][:(last_a+1)]]
    try_next(merge_r, temp_ra, rb, all_orders, und_a, last_b, max_load, time_lim)
    return merge_r


def try_next(res_routes, ra, rb, allo, und_a, last_b, max_load=140, time_lim=720):
    # First judge if it's the end of a merge
    if last_b < 0:
        pre_cal_res = generate_distance_time(ra, und_a, allo)
        ra = bb_tsp(ra, allo, und_a, time_lim, pre_cal=pre_cal_res)
        res_routes.append(ra)
        return
    # try combine the following node in rb
    next_id, next_pck, next_oid = rb[0][0], rb[3][0], rb[4][0]
    is_suc, r_ab, und_ab = route_node_merge(ra, und_a, [next_id, next_pck, next_oid], allo, max_load, time_lim)
    if is_suc:
        new_rb = [rb[0][1:], [], [], rb[3][1:], rb[4][1:]]
        try_next(res_routes, r_ab, new_rb, allo, und_ab, last_b-1, max_load, time_lim)
    elif und_a[0] <= 0:
        return
    # try combine the node in und_a
    for de_or_id in und_a[1]:
        de_id, de_pck = allo.at[de_or_id, 'dest_id'], -allo.at[de_or_id, 'num']
        is_suc, r_aa, und_aa = route_node_merge(ra, und_a, [de_id, de_pck, de_or_id], allo, max_load, time_lim)
        if is_suc:
            try_next(res_routes, r_aa, rb, allo, und_aa, last_b, max_load, time_lim)
    return


def bb_tsp(r, allo, und_a, time_lim=720, best_obj=np.inf, append_und=True, now_punish=0.0, out_obj=False, pre_cal=None):
    # TP append [last_a, und_a] to r
    if append_und:
        last_a = len(r[0]) - 1
        if len(r) == 5:
            r.append([last_a, und_a])
        elif len(r) > 5:
            r[5] = [last_a, und_a]
        else:
            raise Exception('Wrong element in the route: ' + str(r))
    if und_a[0] <= 0:
        # no further delivery
        return r, r[2][-1] + now_punish
    final_r = None
    for de_or_id in und_a[1]:
        if pre_cal is None:
            de_id, de_pck = allo.at[de_or_id, 'dest_id'], -allo.at[de_or_id, 'num']
        else:
            temp_ind = pre_cal['index'][de_or_id + 'd']
            de_id, de_pck = pre_cal['node ID'][temp_ind], pre_cal['package num'][temp_ind]
        is_suc, r_aa, und_aa, punish = route_node_merge(r, und_a, [de_id, de_pck, de_or_id], allo,
                                                        MAX_LOADS, time_lim, False, True, pre_cal)
        if not is_suc:
            raise Exception('Exceed load limits')
        next_punish = now_punish + punish
        adjust = 0.0
        if (not (pre_cal is None)) and len(und_aa[1]) > 0:
            left_ind = [pre_cal['index'][ttoid + 'd'] for ttoid in und_aa[1]]
            temp_last_ind = pre_cal['index'][r_aa[4][-1] + 'd']
            min_last_to_left = min([pre_cal['travel time'][temp_last_ind][loop_i] for loop_i in left_ind])
            left_nm = len(und_aa[1])
            min_inter = 0.0
            if left_nm > 1:
                min_inter = min([pre_cal['travel time'][loop_i][loop_j] for loop_i in left_ind for loop_j in left_ind
                                if loop_i > loop_j])
            left_stay = [pre_cal['stay time'][loop_i] for loop_i in left_ind]
            total_left_stay = sum(left_stay)
            min_stay = min(left_stay)
            best_arr_time = np.array([min_last_to_left + min_stay + temp_i * (min_inter + min_stay) for temp_i
                                      in range(left_nm)])
            left_del_time = np.sort([pre_cal['delivery time'][temp_i] for temp_i in left_ind])
            least_pun = np.sum(5.0 * (np.maximum(best_arr_time, left_del_time) - left_del_time))
            adjust += min_last_to_left + (left_nm - 1) * min_inter + total_left_stay + least_pun
        if next_punish + r_aa[2][-1] + adjust < best_obj:
            test_r, best_obj = bb_tsp(r_aa, allo, und_aa, time_lim, best_obj, False, next_punish, True, pre_cal)
            if not (test_r is None):
                final_r = test_r
    if out_obj:
        return final_r, best_obj
    return final_r


def route_node_merge(r, und_a, node, allo, max_load=140.0, time_lim=720, check_dis_time=True, cal_punish=False,
                     pre_cal=None):
    # TP node=[node id, pack number, order id]
    # judge whether suitable to merge: max distance, max time, package capacity
    if und_a[0] + node[1] > max_load:
        if cal_punish:
            return False, [], [], np.inf
        return False, [], []
    if pre_cal is None:
        xa, ya = get_cor(r[4][-1], allo, int(r[3][-1] < 0))
        arr_t, lea_t, info = time_update(node[2], node[1], np.round(r[2][-1]), xa, ya, allo, cal_punish)
    else:
        arr_t, lea_t, info = quick_time_update(node[2], np.round(r[2][-1]), r[4][-1], r[3][-1], pre_cal, cal_punish)
    dis = info[2]
    if check_dis_time:
        if dis > MERGE_MAX_DISTANCE:
            return False, [], []
        if arr_t > time_lim:
            return False, [], []
    m_r = copy.deepcopy(r)
    m_und = copy.deepcopy(und_a)
    # merge the node; remember deepcopy r and und_a; first calculate the time; then append m_r, then edit m_und
    m_r[0].append(node[0])
    m_r[1].append(arr_t)
    m_r[2].append(lea_t)
    m_r[3].append(node[1])
    m_r[4].append(node[2])
    m_und[0] += node[1]
    if node[1] < 0:
        # deliver, delete in m_und
        m_und[1].remove(node[2])
    else:
        # pickup, add in m_und
        m_und[1].append(node[2])
    if cal_punish:
        return True, m_r, m_und, info[3]
    return True, m_r, m_und


def find_last(r):
    # TP find the position of the last pickup node
    # return -1 if no pickup
    # 0 package number takes as pickup
    pn = r[3]
    last = len(pn)-1
    pkn = 0
    o_id = []
    while last >= 0 > pn[last]:
        pkn -= pn[last]
        o_id.insert(0, r[4][last])
        last -= 1
    return last, [pkn, o_id]


def stay_time(pack_num):
    # TP
    return np.round(3.0*np.sqrt(pack_num)+5.0)


def node_dis(ox, oy, dx, dy):
    # TP
    return np.sqrt((ox-dx)**2+(oy-dy)**2)


def travel_time(dis, speed=SPEED):
    # TP
    return np.round(dis/speed)


def del_rep(ra, rb, allo, recalculate_time=True):
    # TP delete repeat order in rb respect to ra without changing rb
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


def recal_time(r, allo, cal_punish=False):
    # TP recalculate time in route r, and changing r itself
    # not counting the case that start with an O2O
    rl = len(r[0])
    if rl <= 0:
        return r
    if r[3][0] < 0:
        raise Exception('Wrong route, not start with a pickup')
    arr = [0.0]
    lea = [0.0]
    xl, yl = get_cor(r[4][0], allo)
    total_punish = 0.0
    punish = [0.0]
    for i in range(1, rl):
        last_leave = np.round(lea[i-1])
        pck_num = r[3][i]
        ord_id = r[4][i]
        arr_time, lea_time, info = time_update(ord_id, pck_num, last_leave, xl, yl, allo, cal_punish)
        xl, yl = info[:2]
        if cal_punish:
            punish.append(info[3])
            total_punish += info[3]
        arr.append(arr_time)
        lea.append(lea_time)
    r[1] = arr
    r[2] = lea
    if cal_punish:
        return r, (total_punish, punish)
    return r


def time_update(ord_id, pck_num, last_leave_time, last_x, last_y, allo, cal_punish=False):
    # TP
    if pck_num < 0:
        # delivery, arr = last + travel, leave=arr+holding
        xd, yd = get_cor(ord_id, allo, 1)
        dist = node_dis(last_x, last_y, xd, yd)
        arr_time = last_leave_time + travel_time(dist)
        lea_time = arr_time + stay_time(-pck_num)
        punish = 0.0
        if cal_punish:
            if allo.at[ord_id, 'order_type'] == 0:
                del_time = SITE_END_TIME
            else:
                del_time = allo.at[ord_id, 'delivery_time']
            if arr_time > del_time:
                punish = 5.0 * (arr_time - del_time)
        return arr_time, lea_time, (xd, yd, dist, punish)
    elif allo.at[ord_id, 'order_type'] == 0:
        # pickup at site, arr=last+travel, leave=arr
        xd, yd = get_cor(ord_id, allo, 0)
        dist = node_dis(last_x, last_y, xd, yd)
        arr_time = last_leave_time + travel_time(dist)
        lea_time = arr_time + 0.0
        return arr_time, lea_time, (xd, yd, dist, 0.0)
    else:
        # pickup at O2O, arr_time = (last + travel), leave_time = max(arr_time, pickup time) remember round
        xd, yd = get_cor(ord_id, allo, 0)
        dist = node_dis(last_x, last_y, xd, yd)
        arr_time = last_leave_time + travel_time(dist)
        pick_time = allo.at[ord_id, 'pickup_time']
        lea_time = max(arr_time, pick_time)
        punish = 0.0
        if cal_punish and arr_time > pick_time:
            punish = 5.0 * (arr_time - pick_time)
        return arr_time, lea_time, (xd, yd, dist, punish)


def quick_time_update(ord_id, last_leave_time, last_oid, last_pck, info, cal_punish=False):
    # TP only for delivery
    n_oid, last_n_oid = ord_id + 'd', last_oid
    if last_pck < 0:
        last_n_oid += 'd'
    else:
        last_n_oid += 'p'
    index = info['index'][n_oid]
    last_index = info['index'][last_n_oid]
    arr_time = last_leave_time + info['travel time'][last_index][index]
    lea_time = arr_time + info['stay time'][index]
    punish = 0.0
    if cal_punish:
        if arr_time > info['delivery time'][index]:
            punish = 5.0 * (arr_time - info['delivery time'][index])
    return arr_time, lea_time, (0, 0, info['distance'][last_index][index], punish)


def get_cor(order_id, allo, o_type=0):
    # TP type=0: pickup; else: delivery
    if o_type == 0:
        return allo.at[order_id, 'ox'], allo.at[order_id, 'oy']
    return allo.at[order_id, 'dx'], allo.at[order_id, 'dy']


def generate_distance_time(r, und, allo):
    # TP
    nodes = [r[0][-1]] + [allo.at[oid, 'dest_id'] for oid in und[1]]
    pcks = [0.0] + [-allo.at[oid, 'num'] for oid in und[1]]
    st_time = [0.0] + [stay_time(-pkn) for pkn in pcks[1:]]
    deli_time = [0.0]
    for oid in und[1]:
        if allo.at[oid, 'order_type'] == 0:
            deli_time.append(SITE_END_TIME)
        else:
            deli_time.append(allo.at[oid, 'delivery_time'])
    n_oid, xy_c = [], []
    if r[3][-1] < 0:
        n_oid.append(r[4][-1] + 'd')
        xy_c.append((allo.at[r[4][-1], 'dx'], allo.at[r[4][-1], 'dy']))
    else:
        n_oid.append(r[4][-1] + 'p')
        xy_c.append((allo.at[r[4][-1], 'ox'], allo.at[r[4][-1], 'oy']))
    for oid in und[1]:
        n_oid.append(oid + 'd')
        xy_c.append((allo.at[oid, 'dx'], allo.at[oid, 'dy']))
    index_dic = {n_oid[i]: i for i in range(len(n_oid))}
    dis_m = sci_dis.cdist(xy_c, xy_c, 'euclidean')
    tra_time_m = travel_time(dis_m)
    return {'distance': dis_m, 'travel time': tra_time_m, 'stay time': st_time, 'node ID': nodes, 'package num': pcks,
            'delivery time': deli_time, 'index': index_dic}
