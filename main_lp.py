import time
import os
import merge as mg
import cPickle as cP
from multiprocessing import Pool
from solveByLP import opt
import csv
import heuristic

PROCESSORS = 100


def route_to_str(r):
    r_str = ''
    for ord_id in r[4]:
        r_str += ord_id + ','
    return r_str


def str_to_route(r_str):
    global allo
    r_nodes, pck = [], []
    r_ord = r_str.split(',')[:-1]
    pick_set = {}
    for ord_id in r_ord:
        if ord_id in pick_set:
            # delivery
            if pick_set[ord_id] > 1:
                raise Exception('replicated order in ' + r_str + ' with order id ' + 'ord_id')
            r_nodes.append(allo.at[ord_id, 'dest_id'])
            pck.append(-allo.at[ord_id, 'num'])
            pick_set[ord_id] += 1
        else:
            # pickup
            r_nodes.append(allo.at[ord_id, 'ori_id'])
            pck.append(allo.at[ord_id, 'num'])
            pick_set[ord_id] = 0
    new_r = [r_nodes, [], [], pck, r_ord]
    mg.recal_time(new_r, allo, is_ll=True)
    last, und = mg.find_last(new_r)
    mg.append_to_route(new_r, [last, und])
    return new_r


def cal_c(r_str):
    global allo
    r_nodes, pck = [], []
    r_ord = r_str.split(',')[:-1]
    pick_set = {}
    for ord_id in r_ord:
        if ord_id in pick_set:
            # delivery
            if pick_set[ord_id] > 1:
                raise Exception('replicated order in ' + r_str + ' with order id ' + 'ord_id')
            r_nodes.append(allo.at[ord_id, 'dest_id'])
            pck.append(-allo.at[ord_id, 'num'])
            pick_set[ord_id] += 1
        else:
            # pickup
            r_nodes.append(allo.at[ord_id, 'ori_id'])
            pck.append(allo.at[ord_id, 'num'])
            pick_set[ord_id] = 0
    new_r = [r_nodes, [], [], pck, r_ord]
    new_r, p_info = mg.recal_time(new_r, allo, True, is_ll=True)
    return p_info[0] + new_r[2][-1]


def process_pro():
    import win32api
    import win32process
    import win32con
    pid = win32api.GetCurrentProcessId()
    handle = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, pid)
    win32process.SetPriorityClass(handle, win32process.REALTIME_PRIORITY_CLASS)


def load_routes(f_name, has_set=False, is_set=False, pool1=None, need_decompression=True):
    print('Start to read: ' + f_name)
    f = open(f_name, 'rb')
    if has_set:
        read_r, temp_s = cP.load(f)
    else:
        read_r = cP.load(f)
    f.close()
    print('Load completed')
    if is_set or (not need_decompression):
        return read_r
    print('Start to decompression the routes')
    if pool1 is None:
        pool1 = Pool(PROCESSORS, process_pro)
    r_strans = pool1.map(str_to_route, read_r)
    print('Decompression completed!')
    return r_strans


def dump_routes(f_name, r, is_set=False, pool1=None, is_compressed=False):
    print('Start to dump: ' + f_name)
    f = open(f_name, 'wb')
    if is_set or is_compressed:
        cP.dump(r, f)
        f.close()
        print('Dump complete!')
        return
    print('Start to compression the routes')
    if pool1 is None:
        pool1 = Pool(PROCESSORS, process_pro)
    r_str = pool1.map(route_to_str, r)
    print('Compression completed!')
    cP.dump(r_str, f)
    f.close()
    print('Dump completed!')
    return


def write_routes_res(file_name, route):
    f = open('carriers', 'rb')
    carrier_id = cP.load(f)
    f.close()
    f = open(file_name, 'wb')
    write = csv.writer(f)
    c_ind = 0
    for r in route:
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
        m_ben.append(len(t_r_o_ids))
        m_gain.append(len(t_r_o_ids)*1.0/r_costs[route_index])
        for t_r_o_id in t_r_o_ids:
            X[x_dic[t_r_o_id]].add(route_index)
            route[route_index].add(x_dic[t_r_o_id])
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
