# This is only for test. Do not use it otherwise

import random as rdm
import merge as mg
import numpy as np
import copy


def gener_routes(allo, re_cal=False, find_last=False):
    r1 = [['A083', 'A083', 'A083', 'A083', 'A083', 'B5800', 'B7555', 'B7182', 'B8307', 'B8461'], [], [],
          [34, 11, 19, 12, 63, -34, -63, -12, -19, -11],
          ['F6344', 'F6360', 'F6358', 'F6353', 'F6354', 'F6344', 'F6354', 'F6353', 'F6358', 'F6360']]
    r2 = [['A083', 'A083', 'A083', 'B6528', 'S245', 'B3266', 'B3266', 'B2337'], [], [],
          [46, 53, 39, -46, 1, -1, -53, -39],
          ['F6349', 'F6325', 'F6314', 'F6349', 'E0895', 'E0895', 'F6325', 'F6314']]
    r3 = [['A083', 'A083', 'A083', 'A083', 'S294', 'B1940', 'B6104', 'B8926', 'B9072', 'B6103'], [], [],
          [36, 27, 36, 33, 1, -33, -36, -1, -36, -27],
          ['F6366', 'F6345', 'F6346', 'F6308', 'E1088', 'F6308', 'F6346', 'E1088', 'F6366', 'F6345']]
    if re_cal:
        mg.recal_time(r1, allo)
        mg.recal_time(r2, allo)
        mg.recal_time(r3, allo)
    if find_last:
        last1, und1 = mg.find_last(r1)
        last2, und2 = mg.find_last(r2)
        last3, und3 = mg.find_last(r3)
        return [r1, r2, r3], [(last1, und1), (last2, und2), (last3, und3)]
    return [r1, r2, r3]


def generate_route(allo, ord_num=8):
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
