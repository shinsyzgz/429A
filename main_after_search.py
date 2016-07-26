# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 02:58:12 2016

@author:
"""

import pickle
import time
import os
import random
import merge as mg
import cPickle as cP
from multiprocessing import Pool
from route_adjust import order_node
from new_merge import merge_remove

PROCESSORS = 140
most_merge = 10
full_iter = (False, 3, 2)
# full_iter = (False, n, (n-1)!)


def remove_duplicate(some_list, result=None, res_set=None):
    if result is None:
        result = []
        for r in some_list:
            if not (r in result):
                result.append(r)
        return result
    else:
        for r in some_list:
            order_str = route_to_str(r)
            if not (order_str in res_set):
                result.append(r)
                res_set.add(order_str)
    return result, res_set
    
    
def read_files(inde):
    f = open('temp_res/ori_routes_C' + str(inde), 'rb')
    t_routes = pickle.load(f)
    print(str(inde) + ' read completed.')
    f.close()
    return t_routes
    
    
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
    mg.recal_time(new_r, allo)
    last, und = mg.find_last(new_r)
    mg.append_to_route(new_r, [last, und])
    return new_r
    
    
def process_init(all_orders):
    global allo
    allo = all_orders
    
    
def process_pro():
    import win32api
    import win32process
    import win32con
    pid = win32api.GetCurrentProcessId()
    handle = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, pid)
    win32process.SetPriorityClass(handle, win32process.REALTIME_PRIORITY_CLASS)
    
    
def load_routes(f_name, allo1, has_set=False, is_set=False, pool1=None, need_decompression=True):
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
        pool1 = Pool(PROCESSORS, process_init, (allo1, ))
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
        pool1 = Pool(PROCESSORS, process_init, (allo, ))
    r_str = pool1.map(route_to_str, r)
    print('Compression completed!')
    cP.dump(r_str, f)
    f.close()
    print('Dump completed!')
    return
    
    
def merge_two(r):
    global allo
    ras, rbs = r
    ra, rb = str_to_route(ras), str_to_route(rbs)
    res_set = mg.merge_two(ra, rb, allo, ra[5][0], rb[5][0], ra[5][1], rb[5][1],
                           most_merge=most_merge, full_iteration=full_iter)
    str_set = []
    for full_r in res_set:
        str_set.append(route_to_str(full_r))
    return str_set


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
    inter_rounds = 0
    inter_pairs_num = 10000
    inter_prob_o2o = 0.7
    inter_prob_dif = 0.8
    # parameters for random merge
    rnd_rounds = 10
    rnd_pairs_num = 5000
    rnd_prob_o2o, rnd_prob_new = 0.3, 0.4
    # multiprocessing
    pool = Pool(PROCESSORS, process_pro)
    # Site and O2O evolve themselves
    # read files:
    print('reading files...')
    site_set = load_routes('site_set', allo, need_decompression=False)
    site_num = len(site_set)
    print('Site complete with num: ' + str(site_num))
    o2o_set = load_routes('o2o_set', allo, need_decompression=False)
    o2o_num = len(o2o_set)
    print('O2O complete with num: ' + str(o2o_num))
    new_set = load_routes('new_set', allo, need_decompression=False)
    new_num = len(new_set)
    print('New complete with num: ' + str(new_num))
    total_set = load_routes('total_set', allo, is_set=True)
    total_num = len(total_set)
    print('Total complete with num: ' + str(total_num))
    total_r = load_routes('total_re', allo, is_set=True)
    print('Total_re complete!')
    stime = time.time()
    # Start rounds:
    # pool = Pool(PROCESSORS, process_init, (allo, ))
    
    now_r = 0
    isD = False
    while now_r < rounds:
        isD = True
        now_r += 1
        print('Start rounds ' + str(now_r) + '. Generating pairs')
        site_pairs, o2o_pairs = [], []
        now_p = 0
        while now_p < pairs_num:
            site_candidate = (random.choice(site_set), random.choice(site_set))
            adj_candidate = order_node(merge_remove(site_candidate[0], site_candidate[1], False), False)
            if adj_candidate in total_r:
                continue
            else:
                site_pairs.append(site_candidate)
                now_p += 1
        now_p = 0
        while now_p < pairs_num:
            o2o_candidate = (random.choice(o2o_set), random.choice(o2o_set))
            adj_candidate = order_node(merge_remove(o2o_candidate[0], o2o_candidate[1], False), False)
            if adj_candidate in total_r:
                continue
            else:
                o2o_pairs.append(o2o_candidate)
                now_p += 1
        print('Generating complete. Now merge site')
        site_add_len = 0
        site_ms = pool.map(merge_two, site_pairs)
        print('Site merge complete. Now delete replicate and add to site set and total set')
        for si_m in site_ms:
            not_add_r = True
            for site_r in si_m:
                if not (site_r in total_set):
                    site_add_len += 1
                    site_set.append(site_r)
                    total_set.add(site_r)
                    if not_add_r:
                        total_r.add(order_node(site_r, False))
                        not_add_r = False
        print('Site completed. New site: ' + str(site_add_len))
        print('Start merge O2O')
        o2o_ms = pool.map(merge_two, o2o_pairs)
        print('o2o merge complete. Now del and add')
        o2o_add_len = 0
        for o_m in o2o_ms:
            not_add_r = True
            for o_str in o_m:
                if not (o_str in total_set):
                    o2o_add_len += 1
                    o2o_set.append(o_str)
                    total_set.add(o_str)
                    if not_add_r:
                        total_r.add(order_node(o_str, False))
                        not_add_r = False
        print('O2O completed. New O2O: ' + str(o2o_add_len))
        print('Round ' + str(now_r) + 'end.')
        if time.time() - stime > 20 * 60:
            dump_routes('site_set', site_set, is_compressed=True)
            dump_routes('o2o_set', o2o_set, is_compressed=True)
            dump_routes('total_set', total_set, is_set=True)
            dump_routes('total_re', total_r, is_set=True)
            print('Dump completed, next round')
            stime = time.time()
    if isD:
        dump_routes('site_set', site_set, is_compressed=True)
        dump_routes('o2o_set', o2o_set, is_compressed=True)
        dump_routes('total_set', total_set, is_set=True)
        dump_routes('total_re', total_r, is_set=True)
        print('Dump completed, self evolve end')
    
    # Set and O2O interaction
    now_r = 0
    stime = time.time()
    isD = False
    while now_r < inter_rounds:
        isD = True
        now_r += 1
        print('Start interaction round ' + str(now_r) + '. Generating pairs')
        inter_pairs, inter_types = [], []
        # inter_type: 0 interaction, 1 site, 2 o2o
        now_p = 0
        while now_p < inter_pairs_num:
            this_type = 0
            if random.random() < inter_prob_o2o:
                # pick one from O2O orders
                fpic = random.choice(o2o_set)
                if random.random() < inter_prob_dif:
                    # pick one from site orders
                    spic = random.choice(site_set)
                    this_type = 0
                else:
                    # pick one from o2o orders
                    spic = random.choice(o2o_set)
                    this_type = 2
            else:
                # pick one from site orders
                fpic = random.choice(site_set)
                if random.random() < inter_prob_dif:
                    # pick from o2o
                    spic = random.choice(o2o_set)
                    this_type = 0
                else:
                    # pick from site
                    spic = random.choice(site_set)
                    this_type = 1
            adj_candidate = order_node(merge_remove(spic, fpic, False), False)
            if adj_candidate not in total_r:
                inter_pairs.append((spic, fpic))
                inter_types.append(this_type)
                now_p += 1
        print('Generate ' + str(len(inter_pairs)) + ' completed! Now start to merge...')
        site_add_len, o2o_add_len, new_add_len = 0, 0, 0
        inter_m_res = pool.map(merge_two, inter_pairs)
        print('Merge complete... Now del and add')
        for inter_m, inter_t in zip(inter_m_res, inter_types):
            not_add_r = True
            if inter_t == 0:
                # interaction
                for inter_str in inter_m:
                    if not (inter_str in total_set):
                        new_add_len += 1
                        new_set.append(inter_str)
                        total_set.add(inter_str)
                        if not_add_r:
                            total_r.add(order_node(inter_str, False))
                            not_add_r = False
            elif inter_t == 2:
                # o2o
                for o2o_str in inter_m:
                    if not (o2o_str in total_set):
                        o2o_add_len += 1
                        o2o_set.append(o2o_str)
                        total_set.add(o2o_str)
                        if not_add_r:
                            total_r.add(order_node(o2o_str, False))
                            not_add_r = False
            else:
                # site
                for site_str in inter_m:
                    if not (site_str in total_set):
                        site_add_len += 1
                        site_set.append(site_str)
                        total_set.add(site_str)
                        if not_add_r:
                            total_r.add(order_node(site_str, False))
                            not_add_r = False
        print('Add complete.')
        print('New added: ' + str(new_add_len))
        print('Site added: ' + str(site_add_len))
        print('O2O added: ' + str(o2o_add_len))
        print('Round ' + str(now_r) + 'end..')
        if time.time() - stime > 30 * 60:
            dump_routes('site_set', site_set, is_compressed=True)
            dump_routes('o2o_set', o2o_set, is_compressed=True)
            dump_routes('new_set', new_set, is_compressed=True)
            dump_routes('total_set', total_set, is_set=True)
            dump_routes('total_re', total_r, is_set=True)
            print('Dump completed, next round')
            stime = time.time()
    if isD:
        dump_routes('site_set', site_set, is_compressed=True)
        dump_routes('o2o_set', o2o_set, is_compressed=True)
        dump_routes('new_set', new_set, is_compressed=True)
        dump_routes('total_set', total_set, is_set=True)
        dump_routes('total_re', total_r, is_set=True)
        print('Dump completed, interaction end.')
    
    # Start random merge process
    now_r = 0
    stime = time.time()
    while now_r < rnd_rounds:
        now_r += 1
        print('Start rnd merge round ' + str(now_r))
        rnd_pairs, rnd_types = [], []
        now_p = 0
        while now_p < rnd_pairs_num:
            this_type = 0
            # type: 0 inter, 1 site, 2 o2o
            f_p_r = random.random()
            s_p_r = random.random()
            f_p_t, s_p_t = 0, 0
            if f_p_r < rnd_prob_o2o:
                # fpick o2o order
                fpic = random.choice(o2o_set)
                f_p_t = 2
            elif f_p_r < rnd_prob_o2o + rnd_prob_new:
                # fpick inter order
                fpic = random.choice(new_set)
                f_p_t = 0
            else:
                # fpick site order
                fpic = random.choice(site_set)
                f_p_t = 1
            if s_p_r < rnd_prob_o2o:
                # spick o2o order
                spic = random.choice(o2o_set)
                s_p_t = 2
            elif s_p_r < rnd_prob_o2o + rnd_prob_new:
                # spick inter order
                spic = random.choice(new_set)
                s_p_t = 0
            else:
                # spick site order
                spic = random.choice(site_set)
                s_p_t = 1
            adj_candidate = order_node(merge_remove(fpic, spic, False), False)
            if adj_candidate not in total_r:
                rnd_pairs.append((fpic, spic))
                if f_p_t == 2 == s_p_t:
                    this_type = 2
                elif f_p_t == 1 == s_p_t:
                    this_type = 1
                else:
                    this_type = 0
                rnd_types.append(this_type)
                now_p += 1
        print('Generate ' + str(len(rnd_pairs)) + ' completed. Now start to merge...')
        site_add_len, o2o_add_len, new_add_len = 0, 0, 0
        rnd_m_res = pool.map(merge_two, rnd_pairs)
        print('Merge complete. Del and add')
        for rnd_m, rnd_t in zip(rnd_m_res, rnd_types):
            not_add_r = True
            if rnd_t == 0:
                # inter
                for rnd_str in rnd_m:
                    if not (rnd_str in total_set):
                        new_add_len += 1
                        new_set.append(rnd_str)
                        total_set.add(rnd_str)
                        if not_add_r:
                            if not_add_r:
                                total_r.add(order_node(rnd_str, False))
                                not_add_r = False
            elif rnd_t == 2:
                # o2o
                for rnd_str in rnd_m:
                    if not (rnd_str in total_set):
                        o2o_add_len += 1
                        o2o_set.append(rnd_str)
                        total_set.add(rnd_str)
                        if not_add_r:
                            total_r.add(order_node(rnd_str, False))
                            not_add_r = False
            else:
                # site
                for rnd_str in rnd_m:
                    if not (rnd_str in total_set):
                        site_add_len += 1
                        site_set.append(rnd_str)
                        total_set.add(rnd_str)
                        if not_add_r:
                            total_r.add(order_node(rnd_str, False))
                            not_add_r = False
        print('Add complete.')
        print('New added: ' + str(new_add_len))
        print('Site added: ' + str(site_add_len))
        print('O2O added: ' + str(o2o_add_len))
        print('Round ' + str(now_r) + 'end..')
        if time.time() - stime > 30 * 60:
            dump_routes('site_set', site_set, is_compressed=True)
            dump_routes('o2o_set', o2o_set, is_compressed=True)
            dump_routes('new_set', new_set, is_compressed=True)
            dump_routes('total_set', total_set, is_set=True)
            dump_routes('total_re', total_r, is_set=True)
            print('Dump completed, next round')
            stime = time.time()
    dump_routes('site_set', site_set, is_compressed=True)
    dump_routes('o2o_set', o2o_set, is_compressed=True)
    dump_routes('new_set', new_set, is_compressed=True)
    dump_routes('total_set', total_set, is_set=True)
    dump_routes('total_re', total_r, is_set=True)
    print('Dump completed, rnd end.')

    '''
    f = open('ori_routes_str', 'rb')
    read_r, read_set = cp.load(f)
    f.close()
    f = open('site_set', 'wb')
    cp.dump(read_r, f)
    f.close()
    print(len(read_set))
    f = open('o2o_set', 'rb')
    o2os = cp.load(f)
    read_set.update(o2os)
    f.close()
    dump_routes('total_set', read_set, isSet=True)
    print(len(read_set))
    ss = load_routes('total_set', allo, isSet=True)
    print(ss == read_set)
    '''
    
    '''
    print('o2o complete: ' + str(len(o2o)))
    pool = Pool(PROCESSORS, process_init, (allo, ))
    dump_routes('o2o_set', o2o, pool=pool)
    
    o2o2 = load_routes('o2o_set', allo, pool=pool)
    print(o2o2 == o2o)
    '''
    
    '''
    pool = Pool(processes=PROCESSORS)
    # this part for read original routes
    routes_list = pool.map(read_files, range(PROCESSORS))
    print('All read complete')
    no_duplicate_routes, order_ids_set = [], set()
    i = 0
    for rs in routes_list:
        no_duplicate_routes, order_ids_set = (
        remove_duplicate(rs, no_duplicate_routes, order_ids_set))
        print(str(i) + 'complete')
        i += 1
    print(len(no_duplicate_routes))
    f = open('ori_routes', 'wb')
    cp.dump((no_duplicate_routes, order_ids_set), f)
    f.close()
    '''
    '''
    # load original routes with full data
    t1 = time.time()
    f = open('ori_routes', 'rb')
    routes, ids_set = cp.load(f)
    f.close()
    t2 = time.time()
    print(len(routes))
    print('Time to load data: ' + str(t2 - t1))
    
    # transfer the full routes to strings and dump them
    t3 = time.time()
    routes_str = []
    for r in routes:
        routes_str.append(route_to_str(r))
    r_set = set(routes_str)
    t4 = time.time()
    print('Time to transform to string: ' + str(t4 - t3))
    t5 = time.time()
    f = open('ori_routes_str', 'wb')
    cp.dump((routes_str, r_set), f)
    f.close()
    t6 = time.time()
    print('Time to dump the route string: ' + str(t6 - t5))
    '''
    '''
    # read the dump strings
    t7 = time.time()
    f = open('ori_routes_str', 'rb')
    read_r, read_set = cp.load(f)
    f.close()
    t8 = time.time()
    print('Time to load to route string: ' + str(t8 - t7))
    
    # transfer the strings to routes: multi process version
    pool = Pool(PROCESSORS, process_init, (allo, ))
    r_strans = pool.map(str_to_route, read_r)
    t9 = time.time()
    print('Time to transform to route: ' + str(t9 - t8))
    print('Total time to load string route and transform: ' + str(t9 - t7))
    '''
    '''
    # This part for test if the convert is good
    # load original routes with full data
    t1 = time.time()
    f = open('ori_routes', 'rb')
    routes, ids_set = cp.load(f)
    f.close()
    t2 = time.time()
    print(len(routes))
    print('Time to load data: ' + str(t2 - t1))    
    
    # check if anything wrong
    if len(r_strans) != len(routes):
        print('trans_num_wrong!')
    else:
        r_ind = 0
        while r_ind < len(r_strans):
            if r_strans[r_ind][:-1] != routes[r_ind]:
                print('trans wrong: ')
                print('before trans: ' + str(routes[r_ind]))
                print('after: ' + str(r_strans[r_ind][:-1]))
                print('string: ' + routes_str[r_ind])
                print('read_string: ' + read_r[r_ind])
            r_ind += 1
    '''
    
    os.system("pause")
