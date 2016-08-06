import time
import os
# from solveByLP import opt
import csv
import heuristic
from transform_tools import *


def write_routes_res(file_name, route_res):
    f = open('carriers', 'rb')
    carrier_id = cP.load(f)
    f.close()
    f = open(file_name, 'wb')
    write = csv.writer(f)
    c_ind = 0
    for r in route_res:
        c_id = carrier_id[c_ind]
        c_ind += 1
        for node, arr, lea, num, order in zip(r[0], r[1], r[2], r[3], r[4]):
            write.writerow([c_id, node, int(arr), int(lea), int(num), order])
    f.close()


f1 = open('allo', 'rb')
allo = cP.load(f1)
f1.close()

if __name__ == '__main__':
    # Multiprocessing
    # loc, allo = loadData.loadData('../original_data')
    lp_path = 'lpM20/'
    # parameters for self evolve
    rounds = 0
    pairs_num = 5000
    # parameters for interactions
    inter_rounds = 25
    inter_pairs_num = 10000
    inter_prob_o2o = 0.7
    inter_prob_dif = 0.8
    # parameters for random merge
    rnd_rounds = 10
    rnd_pairs_num = 5000
    rnd_prob_o2o, rnd_prob_new = 0.3, 0.4
    # multiprocessing
    pool = Pool(PROCESSORS, process_pro)
    total_routes = []
    # Site and O2O evolve themselves
    # read files:
    print('reading files...')
    total_routes += load_routes(lp_path + 'site_set', need_decompression=False)
    site_num = len(total_routes)
    print('Site complete with num: ' + str(site_num))
    total_routes += load_routes(lp_path + 'o2o_set', need_decompression=False)
    o2o_num = len(total_routes) - site_num
    print('O2O complete with num: ' + str(o2o_num))
    total_routes += load_routes(lp_path + 'new_set', need_decompression=False)
    new_num = len(total_routes) - o2o_num - site_num
    print('New complete with num: ' + str(new_num))
    stime = time.time()
    # generate the costs
    r_costs = pool.map(cal_c, total_routes)
    print('cost generate completed! Time: ' + str(time.time()-stime))
    print(str(len(r_costs)))
    stime = time.time()
    # generate the X
    # X = pool.map(cal_x, [o_id for o_id in allo['order_id']])
    X = [set() for oid in allo['order_id']]
    route = [set() for nouse in total_routes]
    m_ben, m_gain, route_set = [], [], set()
    o_ids = [oid for oid in allo['order_id']]
    x_dic = {oid: i for oid, i in zip(o_ids, range(len(o_ids)))}
    route_index = 0
    for t_r_str in total_routes:
        t_r_o_ids = t_r_str.split(',')[:-1]
        route_set.add(route_index)
        for t_r_o_id in t_r_o_ids:
            X[x_dic[t_r_o_id]].add(route_index)
            route[route_index].add(x_dic[t_r_o_id])
        m_ben.append(len(route[route_index]))
        m_gain.append(len(route[route_index]) * 1.0 / r_costs[route_index])
        route_index += 1
    print('relation matrix complete! Time: ' + str(time.time()-stime))
    print(len(X))

    # Solve the LP problem
    stime = time.time()
    # obj, r_select, status = opt(r_costs, X)
    r_select, obj = heuristic.constraint_weighted_set_cover(r_costs, X, route, 1000, 1, m_ben, m_gain, route_set)
    print('Solve complete with time: ' + str(time.time()-stime))
    if r_select is None:
        print('LP error!')
    else:
        sel_routes = [total_routes[r_s_ind] for r_s_ind in r_select]
        '''
        sel_routes = []
        r_s_ind = 0
        for r_s in r_select:
            if 0.01 < r_s < 0.99:
                print('Relaxation error! Half route detected with selection = ' + str(r_s))
                break
            elif r_s > 0.99:
                sel_routes.append(total_routes[r_s_ind])
            r_s_ind += 1
        debug_routes = []
        debug_cost = []
        debug_X = [set() for oid in allo['order_id']]
        debug_sel = []
        r_s_ind = 0
        for r_s in r_select:
            if r_s > 0.001:
                r_str1 = total_routes[r_s_ind]
                debug_routes.append(r_str1)
                debug_cost.append(r_costs[r_s_ind])
                debug_sel.append(r_s)
                t_r_o_ids = r_str1.split(',')[:-1]
                for t_r_o_id in t_r_o_ids:
                    debug_X[x_dic[t_r_o_id]].add(len(debug_routes)-1)
            r_s_ind += 1
        f11 = open('solved_res', 'wb')
        cP.dump((debug_routes, debug_cost, debug_X, debug_sel), f11)
        f11.close()
        f11 = open('solved_temp.csv', 'wb')
        write11 = csv.writer(f11)
        write11.writerow(['index', 'cost', 'select'])
        for de_ind, de_c, de_s in zip(range(len(debug_cost)), debug_cost, debug_sel):
            write11.writerow([de_ind, de_c, de_s])
        write11.writerow(['X'])
        for de_x in debug_X:
            de_x_l = list(de_x)
            de_x_l.sort()
            write11.writerow(de_x_l)
        f11.close()
        '''
        # Check cost and output
        cost, re_routes = 0.0, []
        for sl_r_str in sel_routes:
            cost += cal_c(sl_r_str)
            re_routes.append(str_to_route(sl_r_str))
        print('Check total cost: ' + str(cost) + '. Difference with lp cost: ' + str(cost - obj))
        write_routes_res(lp_path + 'test_result.csv', re_routes)

    os.system("pause")
