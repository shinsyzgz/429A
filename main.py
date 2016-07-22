import merge
import search
from multiprocessing import Pool
import pickle
import time
import os

PROCESSORS = 100


def search_with_start(start_ind):
    t_routes = []
    this_start = start_ind[0]
    this_end = start_ind[1]
    print(start_ind)
    start_time = time.time()
    for ind in range(this_start, this_end + 1):
        print('Process ' + str(start_ind[2]) + ': start number ' + str(ind))
        a = len(t_routes)
        search.search(search.normalOrders.index[ind])
        t_nodes, t_times = search.resultsNodes, search.resultsTime
        t_routes += merge.format_transform(t_nodes, t_times, search.orders)
        print('Process ' + str(start_ind[2]) + ': end number ' + str(ind) +
              ' with start length ' + str(a) + ' end length ' + str(len(t_routes)) +
              ' add ' + str(len(t_nodes)))
        clear_search_results()
        now_time = time.time()
        if now_time - start_time >= 1200.0:
            f1 = open('temp_res/ori_routes' + str(start_ind[2]), 'wb')
            pickle.dump((ind, this_end, t_routes), f1)
            f1.close()
            start_time = now_time
    f1 = open('temp_res/ori_routes_C' + str(start_ind[2]), 'wb')
    pickle.dump(t_routes, f1)
    f1.close()
    return t_routes


def slice_index(total_num):
    c_size = int(total_num / PROCESSORS)
    left = total_num - c_size * PROCESSORS
    start, end = 0, c_size - 1 + int(0 < left)
    record = [(start, end, 0)]
    for x in range(1, PROCESSORS):
        start = end + 1
        end = start + c_size - 1 + int(x < left)
        record.append((start, end, x))
    return record


def get_search_results():
    return search.resultsNodes, search.resultsTime


def clear_search_results():
    search.resultsTime, search.resultsNodes = [], []
    
    
def remove_duplicate(some_list, result=None, res_set=None):
    if result is None:
        result = []
        for r in some_list:
            if not (r in result):
                result.append(r)
        return result
    else:
        for r in some_list:
            order_str = ''
            for ord_id in r[4]:
                order_str += ord_id
            if not (order_str in res_set):
                result.append(r)
                res_set.add(order_str)
    return result, res_set
    
    
def read_files(inde):
    f1 = open('temp_res/ori_routes_C' + str(inde), 'rb')
    t_routes = pickle.load(f1)
    print(str(inde) + ' read completed.')
    f1.close()
    return t_routes


if __name__ == '__main__':
    # Multiprocessing
    pool = Pool(processes=PROCESSORS)
    '''
    # This part for generate the original routes
    indices = slice_index(search.numOfNormalOrders)
    print(indices)
    routes_list = pool.map(search_with_start, indices)
    routes = []
    for rs in routes_list:
        routes += rs
    print(len(routes))
    r_non_duplicate = remove_duplicate(routes)
    print(len(r_non_duplicate))
    f = open('ori_routes', 'wb')
    pickle.dump(r_non_duplicate, f)
    f.close()
    '''
    
    # this part for read original routes
    routes_list = pool.map(read_files, range(PROCESSORS))
    print('All read complete')
    no_duplicate_routes, order_ids_set = [], set()
    i = 0
    for rs in routes_list:
        no_duplicate_routes, order_ids_set = remove_duplicate(rs, no_duplicate_routes, order_ids_set)
        print(str(i) + 'complete')
        i += 1
    print(len(no_duplicate_routes))
    f = open('ori_routes', 'wb')
    pickle.dump((no_duplicate_routes, order_ids_set), f)
    f.close()
    os.system("pause")

    '''
    routes_list = pool.map(search_with_start, range(PROCESSORS))
    routes = []
    for rs in routes_list:
        routes += rs
    print(len(routes))
    f = open('ori_routes', 'wb')
    pickle.dump(routes, f)
    f.close()
    '''
