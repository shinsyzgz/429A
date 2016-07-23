import time
import os
import merge as mg
import cPickle as cP
from multiprocessing import Pool, Manager

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


def cal_x(ord_id):
    route_ind = 0
    x_row = []
    for route_str in cal_x.total_routes:
        if ord_id in route_str:
            x_row.append(route_ind)
        route_ind += 1
    return x_row


def process_pro(all_r_str):
    cal_x.total_routes = all_r_str
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


f1 = open('allo', 'rb')
allo = cP.load(f1)
f1.close()

if __name__ == '__main__':
    # Multiprocessing
    # loc, allo = loadData.loadData('../original_data')
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
    total_routes = Manager().list()
    pool = Pool(PROCESSORS, process_pro, (total_routes,))
    # Site and O2O evolve themselves
    # read files:
    print('reading files...')
    total_routes += load_routes('lp/site_set', need_decompression=False)
    site_num = len(total_routes)
    print('Site complete with num: ' + str(site_num))
    total_routes += load_routes('lp/o2o_set', need_decompression=False)
    o2o_num = len(total_routes) - site_num
    print('O2O complete with num: ' + str(o2o_num))
    total_routes += load_routes('lp/new_set', need_decompression=False)
    new_num = len(total_routes) - o2o_num - site_num
    print('New complete with num: ' + str(new_num))
    stime = time.time()
    # generate the costs
    r_costs = pool.map(cal_c, total_routes)
    print('cost generate completed! Time: ' + str(time.time()-stime))
    print(str(len(r_costs)))
    stime = time.time()
    # generate the X
    X = pool.map(cal_x, [o_id for o_id in allo['order_id']])
    print('relation matrix complete! Time: ' + str(time.time()-stime))
    print(len(X))

    os.system("pause")
