# This is only for test. Do not use it otherwise

import random as rdm
import merge as mg
import numpy as np
import copy


def generate_route(allo, ord_num = 8):
    ord_id = []
    for i in range(ord_num):
        ind = rdm.randint(0, len(allo) - 1)
        ord_id.append(allo.iat[ind, 3])
    nodes = []
    pck = []
    oid = []
    p2d = 0.0
    for ords in ord_id:
        nodes.append(allo.at[ords, 'ori_id'])
        pck.append(allo.at[ords, 'num'])
        oid.append(ords)
        p2d += allo.at[ords, 'num']
    return [nodes, [], [], pck, oid], [p2d, oid]


def monte_tsp(r, und, allo, try_num=400000):
    best_r, best_obj = [], np.inf
    indd = 0
    while indd < try_num:
        indd += 1
        if np.abs(indd/10000.0 - np.floor(indd/10000.0)) <= 0.0001:
            print(indd)
        test_order = und[1][:]
        rdm.shuffle(test_order)
        test_r = copy.deepcopy(r)
        for ordid in test_order:
            test_r[0].append(allo.at[ordid, 'dest_id'])
            test_r[3].append(-allo.at[ordid, 'num'])
            test_r[4].append(ordid)
        test_r, punish = mg.recal_time(test_r, allo, True)
        test_obj = punish[0] + test_r[2][-1]
        if test_obj < best_obj:
            best_r = test_r
            best_obj = test_obj
    return best_r, best_obj
