import cPickle as cP
from multiprocessing import Pool
from main_lp import load_routes, dump_routes, process_pro, route_to_str


def generate_order_dic():
    order_dict1 = {}
    f = open('allo', 'rb')
    allo = cP.load(f)
    f.close()
    o_ind = 0
    for ord_id in allo['order_id']:
        order_dict1[ord_id] = o_ind
        o_ind += 1
    f = open('order_dict', 'wb')
    cP.dump(order_dict1, f)
    f.close()


def order_node(route_str):
    route_list = route_str.split(',')[:-1]
    route_list.sort(key=lambda x: order_dict[x])
    return route_to_str([[], [], [], [], route_list])


f1 = open('order_dict', 'rb')
order_dict = cP.load(f1)
f1.close()
if __name__ == '__main__':
    PROCESSORS = 20
    pool = Pool(PROCESSORS, process_pro)
    # read files:
    print('reading files...')
    site_set = load_routes('site_set', need_decompression=False)
    site_num = len(site_set)
    print('Site complete with num: ' + str(site_num))
    o2o_set = load_routes('o2o_set', need_decompression=False)
    o2o_num = len(o2o_set)
    print('O2O complete with num: ' + str(o2o_num))
    new_set = load_routes('new_set', need_decompression=False)
    new_num = len(new_set)
    print('New complete with num: ' + str(new_num))
    print('Start to transform site...')
    site_reorder = pool.map(order_node, site_set)
    print('Site complete!')
    o2o_reorder = pool.map(order_node, o2o_set)
    print('O2O complete!')
    new_reorder = pool.map(order_node, new_set)
    print('New complete!')
    total_reorder = set()
    site_record, o2o_record, new_record = [], [], []
    print('Del site')
    for rr in site_reorder:
        if not (rr in total_reorder):
            site_record.append(rr)
            total_reorder.add(rr)
    print('Site with ' + str(len(site_record)))
    dump_routes('site_re', site_record, is_compressed=True)
    print('Site dump complete!')

    print('Del o2o')
    for rr in o2o_reorder:
        if not (rr in total_reorder):
            o2o_record.append(rr)
            total_reorder.add(rr)
    print('o2o with ' + str(len(o2o_record)))
    dump_routes('o2o_re', o2o_record, is_compressed=True)
    print('o2o dump complete!')

    print('Del new')
    for rr in new_reorder:
        if not (rr in total_reorder):
            new_record.append(rr)
            total_reorder.add(rr)
    print('new with ' + str(len(new_record)))
    dump_routes('new_re', new_record, is_compressed=True)
    print('new dump complete!')
    dump_routes('total_re', total_reorder, is_set=True)

