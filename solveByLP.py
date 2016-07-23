#encoding=utf-8

import pulp as lp


def opt(C, X):
    orderNum = len(X)
    routeNum = len(C)
    routeIdx = range(routeNum)
    orderIdx = range(orderNum)
    # print routeIdx, orderIdx
    eps = 1.0 / 10 ** 7
    print eps
    # var_choice=lp.LpVariable.dicts('route',routeIdx,0,1,cat='Integer')
    var_choice = lp.LpVariable.dicts('route', routeIdx, lowBound=0)  # 尝试松弛掉01变量
    prob = lp.LpProblem("lastMile", lp.LpMinimize)

    prob += lp.lpSum(var_choice[i] * C[i] for i in routeIdx)

    for i in orderIdx:
        prob += lp.lpSum(var_choice[j] for j in X[i]) >= (1 - eps)

    prob.solve(lp.CPLEX(msg=0))
    print "\n\nstatus:", lp.LpStatus[prob.status]
    if lp.LpStatus[prob.status] != 'Infeasible':
        print "\n\nobjective:", round(lp.value(prob.objective))
        sol_list = [var_choice[i].varValue for i in routeIdx]
        print "\n\nroutes:", round(sum(sol_list))
        # print "\n\noriginal problem:\n",prob
        return round(lp.value(prob.objective)), sol_list, lp.LpStatus[prob.status]
    else:
        return None, None, lp.LpStatus[prob.status]


if __name__ == '__main__':
    C = [1, 2, 1.000]
    X = [{0, 1}, {1, 2}]
    o, v, s = opt(C, X)
    print(len(v))
    print(v)
    for vv in v:
        print(vv)
