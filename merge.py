import numpy as np
import copy
import scipy.spatial.distance as sci_dis
import random as random
import pickle

# Constant: speed is the car speed. MERGE_MAX_DISTANCE is the max distance to merge
SPEED = 250.0
MERGE_MAX_DISTANCE_DELIVERY = 45000.0
MERGE_MAX_DISTANCE_PICKUP = 10000.0
SITE_END_TIME = 720.0
MAX_LOADS = 140.0
EXCEED_TIME_LIM = 40.0
EXCEED_TIME_LIM_TSP = 50.0
O2O_MINI_START = None
# O2O_MINI_START = {o2o order id: (min pickup arr time, nearest site id)}


def merge_set(all_orders, routes_set_a, routes_set_b=None, most_merge=np.inf, max_load=MAX_LOADS,
              time_lim=EXCEED_TIME_LIM, find_l=True, recalculate_time=True, check_feasible=True,
              full_iteration=(True, 0.0, 0.0)):
    # TP merge set A and set B. If only one set is input, merge itself
    exchange = True
    if find_l:
        for r in routes_set_a:
            last_t, und_a = find_last(r)
            append_to_route(r, [last_t, und_a])
            if recalculate_time:
                recal_time(r, all_orders)
            if check_feasible:
                if not check_route_feasible(r, max_load):
                    raise Exception('Route load infeasible: ' + str(r))
        if not (routes_set_b is None):
            for r in routes_set_b:
                last_t, und_a = find_last(r)
                append_to_route(r, [last_t, und_a])
                if recalculate_time:
                    recal_time(r, all_orders)
                if check_feasible:
                    if not check_route_feasible(r, max_load):
                        raise Exception('Route load infeasible: ' + str(r))
    if routes_set_b is None:
        exchange = False
        routes_set_b = routes_set_a
    set_am = []
    for ra in routes_set_a:
        for rb in routes_set_b:
            if len(ra) > 5:
                if len(rb) > 5:
                    set_am += merge_two(ra, rb, all_orders, ra[5][0], rb[5][0], ra[5][1], rb[5][1],
                                        max_load, time_lim, most_merge, exchange, full_iteration=full_iteration)
                else:
                    set_am += merge_two(ra, rb, all_orders, ra[5][0], undelivered_a=ra[5][1], max_load=max_load,
                                        time_lim=time_lim, most_merge=most_merge, exchange=exchange,
                                        full_iteration=full_iteration)
            elif len(rb) > 5:
                set_am += merge_two(ra, rb, all_orders, last_b=rb[5][0], undelivered_b=rb[5][1], max_load=max_load,
                                    time_lim=time_lim, most_merge=most_merge, exchange=exchange,
                                    full_iteration=full_iteration)
            else:
                set_am += merge_two(ra, rb, all_orders, max_load=max_load, time_lim=time_lim,
                                    most_merge=most_merge, exchange=exchange,
                                    full_iteration=full_iteration)
    return set_am


def merge_two(route_a, route_b, all_orders, last_a=-1, last_b=-1, undelivered_a=None, undelivered_b=None,
              max_load=MAX_LOADS, time_lim=EXCEED_TIME_LIM, most_merge=np.inf, exchange=True,
              full_iteration=(True, 0.0, 0.0)):
    # TP route=[noteIDs, arrive minutes, leave minutes, package numbers, order IDs, [last_a, undelivered_a](optional)]
    # last_a and last_b denote the last pickup nodes in the routes
    # undelivered = [package number left, [list of order IDs left]]
    # all_orders is pandas object; time_lim is the time limit to exceed pickup/delivery time
    # first found the last site to obtain packages if last=-1, because the route always end with dispatching packages
    if last_a == -1:
        last_a, undelivered_a = find_last(route_a)
    if last_b == -1:
        last_b, undelivered_b = find_last(route_b)
    if exchange:
        return (merge_order(route_a, route_b, all_orders, last_a, undelivered_a, max_load, time_lim, most_merge,
                            full_iteration=full_iteration) +
                merge_order(route_b, route_a, all_orders, last_b, undelivered_b, max_load, time_lim, most_merge,
                            full_iteration=full_iteration))
    return merge_order(route_a, route_b, all_orders, last_a, undelivered_a, max_load, time_lim, most_merge,
                       full_iteration=full_iteration)


def merge_order(ra, rb, all_orders, last_a, und_a, max_load=MAX_LOADS, time_lim=EXCEED_TIME_LIM, most_merge=np.inf,
                full_iteration=(True, 0.0, 0.0)):
    # TP und_a = [pck n left, [list of order IDs left]]
    if len(ra[0]) <= 0:
        # no new route generated
        return []
    # first renew rb to delete repeated node
    nrb = del_rep(ra, rb, all_orders, False)
    last_bb, und_bb = find_last(nrb)
    if len(nrb[0]) <= 0:
        # no new route generated
        return []
    merge_r = []
    temp_ra = [ra[0][:(last_a+1)], ra[1][:(last_a+1)], ra[2][:(last_a+1)], ra[3][:(last_a+1)], ra[4][:(last_a+1)]]
    try_next(merge_r, ra, nrb, all_orders, [0, []], [0, []], len(nrb[0]) - 1, max_load, time_lim, most_merge,
             find_l=True)
    if len(merge_r) > 0:
        hard_m_last, hard_m_und = find_last(merge_r[0])
        merge_r[0][5] = [hard_m_last, hard_m_und]
    try_next(merge_r, temp_ra, nrb, all_orders, und_a, und_a, last_bb, max_load, time_lim, most_merge,
             full_iteration=full_iteration)
    return merge_r


def try_next(res_routes, ra, rb, allo, und_a, und_all, last_b, max_load=MAX_LOADS, time_lim=EXCEED_TIME_LIM,
             most_merge=np.inf, full_iteration=(True, 0.0, 0.0), find_l=False):
    # TP First judge if it's the end of a merge; full_iteration=(Flag, most_n, rand_shuffle_num/n = (n-1)!)
    if len(res_routes) >= most_merge:
        return
    if last_b < 0:
        pre_cal_res = generate_distance_time(ra, und_all, allo)
        append_last = len(ra[0]) - 1
        ra = bb_tsp(ra, allo, und_all, time_lim, pre_cal=pre_cal_res, find_l=find_l, append_und=False)
        if ra is None:
            return
        if find_l:
            append_last, append_und = find_last(ra)
        else:
            append_und = [und_all[0], ra[4][append_last + 1:]]
        append_to_route(ra, [append_last, append_und])
        res_routes.append(ra)
        return
    # try combine the following node in rb
    next_id, next_pck, next_oid = rb[0][0], rb[3][0], rb[4][0]
    is_suc, r_ab, und_ab = route_node_merge(ra, und_all, [next_id, next_pck, next_oid], allo, max_load, time_lim)
    if is_suc:
        new_rb = [rb[0][1:], [], [], rb[3][1:], rb[4][1:]]
        try_next(res_routes, r_ab, new_rb, allo, und_a, und_ab, last_b-1, max_load, time_lim, most_merge,
                 full_iteration, find_l=find_l)
    elif und_a[0] <= 0:
        return
    # try combine the node in und_a
    if full_iteration[0] or full_iteration[1] >= len(und_a[1]):
        for de_or_id in und_a[1]:
            de_id, de_pck = allo.at[de_or_id, 'dest_id'], -allo.at[de_or_id, 'num']
            is_suc, r_aa, und_aa = route_node_merge(ra, und_all, [de_id, de_pck, de_or_id], allo, max_load, time_lim)
            if is_suc:
                und_acopy = copy.deepcopy(und_a[1])
                und_acopy.remove(de_or_id)
                try_next(res_routes, r_aa, rb, allo, [und_a[0] + de_pck, und_acopy],
                         und_aa, last_b, max_load, time_lim, most_merge, full_iteration, find_l=find_l)
    else:
        # first try the default order. then shuffle rand times
        len_und_a = len(und_a[1])
        if len_und_a > 0:
            de_or_id = und_a[1][0]
            de_id, de_pck = allo.at[de_or_id, 'dest_id'], -allo.at[de_or_id, 'num']
            is_suc, r_aa, und_aa = route_node_merge(ra, und_all, [de_id, de_pck, de_or_id], allo, max_load, time_lim)
            if is_suc:
                und_acopy = copy.deepcopy(und_a[1])
                und_acopy.remove(de_or_id)
                try_next(res_routes, r_aa, rb, allo, [und_a[0] + de_pck, und_acopy],
                         und_aa, last_b, max_load, time_lim, most_merge, (False, 0.0, 0.0), find_l=find_l)
            shuffle_time = 1
            shuffle_und_a = copy.deepcopy(und_a[1])
            while shuffle_time <= full_iteration[2] * len_und_a:
                shuffle_time += 1
                random.shuffle(shuffle_und_a)
                de_or_id = shuffle_und_a[0]
                de_id, de_pck = allo.at[de_or_id, 'dest_id'], -allo.at[de_or_id, 'num']
                is_suc, r_aa, und_aa = route_node_merge(ra, und_all, [de_id, de_pck, de_or_id], allo, max_load,
                                                        time_lim)
                if is_suc:
                    und_acopy = copy.deepcopy(shuffle_und_a)
                    und_acopy.remove(de_or_id)
                    try_next(res_routes, r_aa, rb, allo, [und_a[0] + de_pck, und_acopy],
                             und_aa, last_b, max_load, time_lim, most_merge, (False, 0.0, 0.0), find_l=find_l)
    return


def bb_tsp(r, allo, und_a, time_lim=EXCEED_TIME_LIM, best_obj=np.inf, append_und=True,
           now_punish=0.0, out_obj=False, pre_cal=None, find_l=False):
    # TP append [last_a, und_a] to r
    if append_und:
        if find_l:
            last_a, new_und = find_last(r)
            append_to_route(r, [last_a, new_und])
        else:
            last_a = len(r[0]) - 1
            append_to_route(r, [last_a, und_a])
    if und_a[0] <= 0:
        # no further delivery
        if out_obj:
            return r, r[2][-1] + now_punish
        return r
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
        if pre_cal is None:
            judge_delivery = allo.at[r_aa[4][-1], 'delivery_time']
        else:
            judge_delivery = pre_cal['delivery time'][pre_cal['index'][r_aa[4][-1] + 'd']]
        if r_aa[1][-1] > EXCEED_TIME_LIM_TSP + judge_delivery:
            continue
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


def route_node_merge(r, und_a, node, allo, max_load=MAX_LOADS, time_lim=EXCEED_TIME_LIM,
                     check_dis_time=True, cal_punish=False, pre_cal=None):
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
        if node[1] < 0:
            dis_threshold = MERGE_MAX_DISTANCE_DELIVERY
        else:
            dis_threshold = MERGE_MAX_DISTANCE_PICKUP
        if dis > dis_threshold:
            return False, [], []
        if arr_t > time_lim + info[4]:
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
    global O2O_MINI_START
    rl = len(r[0])
    if rl <= 0:
        return r
    if r[3][0] < 0:
        raise Exception('Wrong route, not start with a pickup')
    if allo.at[r[4][0], 'order_type'] == 0:
        arr = [0.0]
        lea = [0.0]
    else:
        if O2O_MINI_START is None:
            O2O_MINI_START = generate_o2o_minimum_start(allo)
        arr = [O2O_MINI_START[r[4][0]][0]]
        lea = [max(arr[0], allo.at[r[4][0], 'pickup_time'])]
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
        if allo.at[ord_id, 'order_type'] == 0:
            del_time = SITE_END_TIME
        else:
            del_time = allo.at[ord_id, 'delivery_time']
        if cal_punish:
            if arr_time > del_time:
                punish = 5.0 * (arr_time - del_time)
        return arr_time, lea_time, (xd, yd, dist, punish, del_time)
    elif allo.at[ord_id, 'order_type'] == 0:
        # pickup at site, arr=last+travel, leave=arr
        xd, yd = get_cor(ord_id, allo, 0)
        dist = node_dis(last_x, last_y, xd, yd)
        arr_time = last_leave_time + travel_time(dist)
        lea_time = arr_time + 0.0
        return arr_time, lea_time, (xd, yd, dist, 0.0, SITE_END_TIME)
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
        return arr_time, lea_time, (xd, yd, dist, punish, pick_time)


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


def append_to_route(r, app):
    # TP
    if len(r) == 5:
        r.append(app)
    elif len(r) > 5:
        r[5] = app
    else:
        raise Exception('Wrong element in the route: ' + str(r))


def check_route_feasible(r, load_max=MAX_LOADS):
    # TP
    all_pck = 0.0
    order_times = 0.0
    for pck in r[3]:
        all_pck += pck
        if pck > 0:
            order_times += 1
        else:
            order_times -= 1
        if all_pck > load_max or all_pck < 0:
            print('Route with infeasible cumulative loads: ')
            print(r)
            return False
    if all_pck != 0.0 or order_times != 0:
        print('Route infeasible with all_pck = ' + str(all_pck) + ' and order_times = ' + str(order_times) + ': ')
        print(r)
        return False
    return True


def generate_o2o_minimum_start(allo):
    # TP
    # print('Generate O2O start time...')
    try:
        input_dic = open('o2o_start', 'rb')
    except IOError:
        print('No file named o2o_start. Start generating...')
        o2o_ord = allo[allo['order_type'] == 1]
        site_ord = allo[allo['order_type'] == 0]
        o2o_dict = {}
        for o2o_order_id in o2o_ord['order_id']:
            o2o_ox, o2o_oy = o2o_ord.at[o2o_order_id, 'ox'], o2o_ord.at[o2o_order_id, 'oy']
            min_dis, near_site = np.inf, ''
            for site_order_id in site_ord['order_id']:
                site_ox, site_oy = site_ord.at[site_order_id, 'ox'], site_ord.at[site_order_id, 'oy']
                dis = node_dis(site_ox, site_oy, o2o_ox, o2o_oy)
                if dis < min_dis:
                    min_dis, near_site = dis, site_ord.at[site_order_id, 'ori_id']
            o2o_dict[o2o_order_id] = travel_time(min_dis), near_site
        output = open('o2o_start', 'wb')
        pickle.dump(o2o_dict, output)
        output.close()
        print('Generation completed')
        return o2o_dict
    else:
        # print('Find file o2o_start, load the file...')
        o2o_dict = pickle.load(input_dic)
        input_dic.close()
        # print('Load complete.')
        return o2o_dict


def generate_o2o_set(allo):
    # TP
    o2o_ord = allo[allo['order_type'] == 1]
    o2o_set = []
    for order_id in o2o_ord['order_id']:
        pck = o2o_ord.at[order_id, 'num']
        r = [[o2o_ord.at[order_id, 'ori_id'], o2o_ord.at[order_id, 'dest_id']], [], [], [pck, -pck],
             [order_id, order_id]]
        recal_time(r, allo)
        last, und = find_last(r)
        append_to_route(r, [last, und])
        o2o_set.append(r)
    return o2o_set


def cal_xc(routes, allo):
    # TP
    x = []
    c = []
    # First generate all the x
    for order_id in allo['order_id']:
        route_ind = 0
        x_row = []
        for r in routes:
            if order_id in r[4]:
                x_row.append(route_ind)
            route_ind += 1
        x.append(x_row)
    # generate all the cost
    for r in routes:
        r, punish_info = recal_time(r, allo, True)
        c.append(r[2][-1] + punish_info[0])
    return x, c


def format_transform(results_nodes, results_times, allo):
    # TP
    routes = []
    for nodes, times in zip(results_nodes, results_times):
        r = [[], [], [], [], []]
        for node_id, time_info in zip(nodes, times):
            pick = True
            if time_info[3] == 'deliver':
                pick = False
            for order_id in time_info[2]:
                r[0].append(node_id)
                r[4].append(order_id)
                pck = allo.at[order_id, 'num']
                if pick:
                    r[3].append(pck)
                else:
                    r[3].append(-pck)
        recal_time(r, allo)
        if check_route_feasible(r):
            routes.append(r)
        else:
            print('route infeasible!')
    return routes
