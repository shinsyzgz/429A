import merge, search, loadData
from multiprocessing import Pool
import pickle

PROCESSORS = 2


def search_with_start(start_ind):
    if start_ind < 0 or start_ind >= search.numOfNormalOrders:
        raise Exception('Start index out of range: ' + str(start_ind))
        return
    search.search(search.normalOrders.index[start_ind])
    t_nodes, t_times = search.resultsNodes, search.resultsTime
    t_routes = merge.format_transform(t_nodes, t_times, search.orders)
    clear_search_results()
    return start_ind, t_routes


def search_with_start_slice(start_ind):
    t_routes = []
    this_start = start_ind[0]
    this_end = start_ind[1]
    print(start_ind)
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


if __name__ == '__main__':
    # Multiprocessing
    pool = Pool(processes=PROCESSORS)
    a = len(search.resultsNodes)
    routes_list = pool.map(search_with_start, range(5) + [10, 20])
    routes = []
    for rs in routes_list:
        routes += rs[1]
    print(len(routes))
    f = open('ori_routes', 'wb')
    pickle.dump(routes, f)
    f.close()
