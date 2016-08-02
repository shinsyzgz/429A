from multiprocessing import Pool
from main_lp import process_pro, load_routes, cal_c, str_to_route, write_routes_res
import time
import cPickle as cP


def objective():
    pass

f1 = open('allo', 'rb')
allo = cP.load(f1)
f1.close()
if __name__ == '__main__':
    # Multiprocessing
    # loc, allo = loadData.loadData('../original_data')
    lp_path = 'lpnb100/'
    PROCESSORS = 100
    # multiprocessing
    pool = Pool(PROCESSORS, process_pro)
    total_routes = []
    # Site and O2O evolve themselves
    # read files:
    print('reading files...')
    total_routes += load_routes(lp_path + 'site_re', need_decompression=False)
    site_num = len(total_routes)
    print('Site complete with num: ' + str(site_num))
    total_routes += load_routes(lp_path + 'o2o_re', need_decompression=False)
    o2o_num = len(total_routes) - site_num
    print('O2O complete with num: ' + str(o2o_num))
    total_routes += load_routes(lp_path + 'new_re', need_decompression=False)
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
    o_ids = [oid for oid in allo['order_id']]
    x_dic = {oid: i for oid, i in zip(o_ids, range(len(o_ids)))}
    route_index = 0
    for t_r_str in total_routes:
        t_r_o_ids = t_r_str.split(',')[:-1]
        for t_r_o_id in t_r_o_ids:
            X[x_dic[t_r_o_id]].add(route_index)
        route_index += 1
    print('relation matrix complete! Time: ' + str(time.time()-stime))
    print(len(X))

    # Solve the LP problem
    stime = time.time()
    obj, r_select = opt(r_costs, X)
    print('Solve complete with time: ' + str(time.time()-stime))
    print('Line number: ' + str(len(r_select)))
    sel_routes = [total_routes[rind] for rind in r_select]
    # Check cost and output
    cost, re_routes = 0.0, []
    for sl_r_str in sel_routes:
        cost += cal_c(sl_r_str)
        re_routes.append(str_to_route(sl_r_str))
    print('Check total cost: ' + str(cost) + '. Difference with lp cost: ' + str(cost - obj))
    write_routes_res(lp_path + 'test_result.csv', re_routes)
