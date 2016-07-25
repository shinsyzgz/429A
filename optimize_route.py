import pulp as lp
import merge
import os
from main_lp import route_to_str

SITE_END_TIME = 720.0
MAX_LOADS = 140.0
PUNISH_CO = 5.0
CPLEX_TIME_LIMIT = 600


def opt_route(route_str, allo, o2o_mini, initial=None):
    # initial = (given start id, given load, given start time)
    route = route_str.split(',')[:-1]
    node_ind = range(len(route))
    order_dict, stay_t, pick_t, require_t, num, mini_start, ll = {}, [], [], [], [], [], []
    o_id_ind = 0
    for o_id in route:
        if o_id in order_dict:
            # delivery
            if len(order_dict[o_id]) == 1:
                order_dict[o_id].append(o_id_ind)
            else:
                raise Exception('Multiple visiting for the same order: ' + o_id)
            t_num = -allo.at[o_id, 'num']
            stay_t.append(merge.stay_time(-t_num))
            pick_t.append(0)
            if allo.at[o_id, 'order_type'] == 1:
                require_t.append(allo.at[o_id, 'delivery_time'])
            else:
                require_t.append(SITE_END_TIME)
            num.append(t_num)
            mini_start.append(0)
            ll.append((allo.at[o_id, 'dlng'], allo.at[o_id, 'dlat']))
        else:
            # pickup
            order_dict[o_id] = [o_id_ind]
            num.append(allo.at[o_id, 'num'])
            stay_t.append(0)
            ll.append((allo.at[o_id, 'olng'], allo.at[o_id, 'olat']))
            if allo.at[o_id, 'order_type'] == 1:
                t_pick_time = allo.at[o_id, 'pickup_time']
                pick_t.append(t_pick_time)
                require_t.append(t_pick_time)
                mini_start.append(o2o_mini[o_id][0])
            else:
                pick_t.append(0)
                require_t.append(SITE_END_TIME)
                mini_start.append(0)
        o_id_ind += 1
    # Now travel time
    travel_t = {}
    for i in node_ind:
        for j in range(i+1, len(route)):
            t_travel_time = merge.travel_time(merge.node_dis_ll(ll[i][0], ll[i][1], ll[j][0], ll[j][1]))
            travel_t[(i, j)] = t_travel_time
            travel_t[(j, i)] = t_travel_time
    if not (initial is None):
        mini_start[initial[0]] = initial[2]
    obj, res_list, status = opt_with_solver(node_ind, order_dict, travel_t, stay_t, pick_t,
                                            require_t, num, mini_start, initial)
    if res_list is None:
        print(status)
        return None, None
    o_route = route[:]
    for ind, position in zip(node_ind, res_list):
        o_route[position] = route[ind]
    o_r_str = ''
    for o_r_id in o_route:
        o_r_str += o_r_id + ','
    return o_r_str, obj


def opt_tsp(r, und_a, pre_cal):
    if und_a[0] <= 0:
        return None
    node_ind = range(len(und_a[1]) + 1)
    order_dict, stay_t, pick_t, require_t, num, mini_start = {}, [r[2][-1]-r[1][-1]], [0], [0], [0], [r[1][-1]]
    initial = (0, 0)
    for o_idb in und_a[1]:
        # delivery
        o_id = o_idb + 'd'
        temp_ind = pre_cal['index'][o_id]
        stay_t.append(pre_cal['stay time'][temp_ind])
        pick_t.append(0)
        require_t.append(pre_cal['delivery time'][temp_ind])
        num.append(0)
        mini_start.append(0)
    # Now travel time
    travel_t = {}
    for i in node_ind:
        for j in range(i + 1, len(und_a[1]) + 1):
            t_travel_time = pre_cal['travel time'][i][j]
            travel_t[(i, j)] = t_travel_time
            travel_t[(j, i)] = t_travel_time
    obj, res_list, status = opt_with_solver(node_ind, order_dict, travel_t, stay_t, pick_t,
                                            require_t, num, mini_start, initial)
    if res_list is None:
        print(status)
        return None, None
    route = [r[4][-1]] + und_a[1]
    o_route = route[:]
    for ind, position in zip(node_ind, res_list):
        o_route[position] = route[ind]
    o_r_str = route_to_str([[], [], [], [], o_route])
    return o_r_str, obj


def opt_with_solver(node_ind, order_dict, travel_t, stay_t, pick_t, require_t, num,
                    mini_start, initial=None, load_check=True):
    # order_dict = {ord:(ori_id, dest_id)}, ord in order_set, ori_id, dest_id in node_ind
    # node_ind = range(n), travel_t = {(i,j): travel time between i and j}
    # initial = (given start id, given load)
    big_m = 10000
    eps = 1.0/10**7
    n = len(node_ind)
    inter_n = [(i, j) for i in node_ind for j in node_ind if j != i]
    off_time = lp.LpVariable('The route-off time')
    p = lp.LpVariable.dicts('Punish cost', node_ind, lowBound=0)
    x = lp.LpVariable.dicts('Route variable', inter_n, cat='Binary')
    o = lp.LpVariable.dicts('Start-point flag', node_ind, cat='Binary')
    d = lp.LpVariable.dicts('End-point flag', node_ind, cat='Binary')
    a = lp.LpVariable.dicts('Arrival time', node_ind)
    l = lp.LpVariable.dicts('Leave time', node_ind)
    t = lp.LpVariable.dicts('Order count', node_ind, 0, n-1+eps)
    if load_check:
        load = lp.LpVariable.dicts('Arrival load', node_ind, 0, MAX_LOADS+eps)
    else:
        load = lp.LpVariable.dicts('Arrival load', node_ind, 0)
    prob = lp.LpProblem('Optimize a route', lp.LpMinimize)
    # Objective
    prob += off_time + lp.lpSum(p[i] for i in node_ind)
    # Constraints
    for j in node_ind:
        prob += lp.lpSum(x[(i, j)] for i in node_ind if i != j) == 1 - o[j]
    for i in node_ind:
        prob += lp.lpSum(x[(i, j)] for j in node_ind if j != i) == 1 - d[i]
    prob += lp.lpSum(o[i] for i in node_ind) == 1
    prob += lp.lpSum(d[i] for i in node_ind) == 1
    if not (initial is None):
        prob += o[initial[0]] == 1
        prob += load[initial[0]] == initial[1]
    for order in order_dict:
        od = order_dict[order]
        prob += a[od[1]] >= l[od[0]]
    for i in node_ind:
        prob += off_time >= l[i]
        prob += l[i] >= a[i] + stay_t[i]
        prob += l[i] >= pick_t[i]
        prob += a[i] >= mini_start[i]
        prob += p[i] >= PUNISH_CO*(a[i] - require_t[i])
    for i, j in inter_n:
        prob += a[j] >= l[i] + travel_t[(i, j)] + big_m*(x[(i, j)] - 1)
        prob += t[j] >= t[i] + 1 + big_m*(x[(i, j)] - 1)
        prob += load[j] >= load[i] + num[i] + big_m*(x[(i, j)] - 1)
    prob.solve(lp.CPLEX(msg=0, timelimit=CPLEX_TIME_LIMIT, options=['set logfile cplex/cplex%d.log' % os.getpid()]))
    # set threads 100
    if lp.LpStatus[prob.status] != 'Infeasible':
        sol_list = [int(round(t[i].varValue)) for i in node_ind]
        return lp.value(prob.objective), sol_list, lp.LpStatus[prob.status]
    else:
        return None, None, lp.LpStatus[prob.status]


if __name__ == '__main__':
    import time
    import test
    import cPickle as cP
    f = open('allo', 'rb')
    allo1 = cP.load(f)
    f.close()
    r_t, und = test.generate_route(allo1, 20)
    merge.recal_time(r_t, allo1)
    pre_cal1 = merge.generate_distance_time(r_t, und, allo1)
    t1 = time.time()
    opr1 = merge.bb_tsp(r_t, allo1, und, pre_cal=pre_cal1, append_und=False)
    t2 = time.time()
    opr2, obj1 = opt_tsp(r_t, und, pre_cal1)
    t3 = time.time()
    print(t2-t1)
    print(t3-t2)
    print(opr1)
    print(opr2)
