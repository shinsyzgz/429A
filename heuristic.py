import cPickle as cP
from multiprocessing import Pool
from merge import generate_o2o_minimum_start, node_dis
from optimize_route import opt_route
import numpy as np
from transform_tools import oid_to_str


def find_mini_order(s_order_id, required_length=None):
    remain_set = {x for x in allo['order_id']}
    if required_length is None:
        right_hs = 0
    else:
        right_hs = len(remain_set) - required_length
    remain_set.remove(s_order_id)
    order_list, dis_list = [s_order_id], []
    while len(remain_set) > right_hs:
        print_len(len(remain_set), 1000)
        min_dis, min_node, now_node = np.inf, None, order_list[-1]
        for c_o_id in remain_set:
            this_dis = node_dis(aver_cor[now_node][0], aver_cor[now_node][1], aver_cor[c_o_id][0],
                                aver_cor[c_o_id][1])
            if this_dis < min_dis:
                min_dis, min_node = this_dis, c_o_id
        order_list.append(min_node)
        dis_list.append(min_dis)
        remain_set.remove(min_node)
    return order_list, dis_list


def divide_order(order_list, divide_num=1000, mode=0):
    # mode = 0: equally divide; 1: random divide
    total_len = len(order_list)
    if mode == 0:
        basic_d_num, left_d_num = total_len//divide_num, total_len % divide_num
        d_points = [basic_d_num + (1 if i < left_d_num else 0) for i in range(divide_num)]
        divide_result, now_start = [], 0
        for num in d_points:
            chosen = order_list[now_start:(now_start + num)]
            divide_result.append(oid_to_str(chosen + chosen))
            now_start += num
        return divide_result
    else:
        pass


def re_order(o_str):
    li = o_str.split(',')[:-1]
    li.sort()
    return oid_to_str(li)


def constraint_weighted_set_cover(cost, X, route=None, max_route=1000, alpha=1, m_ben=None, m_gain=None,
                                  route_set=None):
    if route is None:
        route = [set() for i in range(len(cost))]
        for o_id in range(len(X)):
            for r_ind in X[o_id]:
                route[r_ind].add(o_id)
    if m_ben is None:
        m_ben = [len(o_set) for o_set in route]
    if m_gain is None:
        m_gain = [m_ben[i]*1.0/cost[i] for i in range(len(cost))]
    if route_set is None:
        route_set = set(range(len(cost)))
    s, rem, i, obj = set(), alpha * len(X), max_route, 0.0
    while i >= 1:
        q, max_q = None, -np.inf
        for r_ind in route_set:
            if m_ben[r_ind] >= rem*1.0/i and m_gain[r_ind] > max_q:
                q, max_q = r_ind, m_gain[r_ind]
        if q is None:
            return s, None
        s.add(q)
        obj += cost[q]
        rem -= m_ben[q]
        if rem <= 0:
            return s, obj
        route_set.remove(q)
        change_list = set()
        for o_id in route[q]:
            X[o_id].remove(q)
            for r_ind in X[o_id]:
                if r_ind not in change_list:
                    change_list.add(r_ind)
                    route[r_ind] -= route[q]
        for r_ind in change_list:
            m_ben[r_ind] = len(route[r_ind])
            if m_ben[r_ind] <= 0:
                route_set.remove(r_ind)
            else:
                m_gain[r_ind] = m_ben[r_ind]*1.0/cost[r_ind]
        print('Path ' + str(i) + ' found! Remain order: ' + str(rem))
        i -= 1
    return s, obj


def print_len(num, sep):
    if num % sep == 0:
        print(num)

f1 = open('allo', 'rb')
allo = cP.load(f1)
f1.close()
o2o_mini = generate_o2o_minimum_start(allo)
aver_cor = {o_id: ((allo.at[o_id, 'ox'] + allo.at[o_id, 'dx'])/2.0, (allo.at[o_id, 'oy'] + allo.at[o_id, 'dy'])/2.0)
            for o_id in allo['order_id']}
if __name__ == '__main__':
    # first find an order
    timelim = 300
    try:
        f1 = open('feasible', 'rb')
    except IOError:
        print('Start over... first pick start point')
        start_order = 'F5812'
        o_list = find_mini_order(start_order)
        d_res = divide_order(o_list[0])
        f_out = open('feasible', 'wb')
        cP.dump((d_res, 0, []), f_out)
        f_out.close()
        ssp, obj_values, unsolved = 0, [], dict()
        for r_str in d_res:
            r_str = re_order(r_str)
            opt_str, opt_obj = opt_route(r_str, allo, o2o_mini, timelim=timelim)
            if opt_str is None:
                unsolved[ssp] = timelim
                obj_values.append(None)
                ssp += 1
                print('UNSOLVED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                continue
            print('Route ' + str(ssp) + 'complete with obj: ' + str(opt_obj))
            print(opt_str)
            d_res[ssp] = opt_str
            obj_values.append(opt_obj)
            ssp += 1
            f_out = open('feasible', 'wb')
            cP.dump((d_res, ssp, obj_values, unsolved), f_out)
            f_out.close()
    else:
        d_res, ssp, obj_values, unsolved = cP.load(f1)
        f1.close()
        print('Load complete with start ' + str(ssp))
        while ssp < len(d_res):
            r_str = re_order(d_res[ssp])
            opt_str, opt_obj = opt_route(r_str, allo, o2o_mini, timelim=timelim)
            if opt_str is None:
                unsolved[ssp] = timelim
                obj_values.append(None)
                ssp += 1
                print('UNSOLVED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                continue
            print('Route ' + str(ssp) + 'complete with obj: ' + str(opt_obj))
            print(opt_str)
            d_res[ssp] = opt_str
            obj_values.append(opt_obj)
            ssp += 1
            f_out = open('feasible', 'wb')
            cP.dump((d_res, ssp, obj_values, unsolved), f_out)
            f_out.close()
