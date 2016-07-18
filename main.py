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
    return t_routes


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
        routes += rs
    print(len(routes))
    pickle.dump(routes, open('ori_routes'))
