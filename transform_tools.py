from multiprocessing import Pool
import cPickle as cP
import merge as mg

PROCESSORS = 1
f1 = open('allo', 'rb')
allo = cP.load(f1)
f1.close()


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


def oid_to_str(o_list):
    r_str = ''
    for ord_id in o_list:
        r_str += ord_id + ','
    return r_str


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