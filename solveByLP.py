#encoding=utf-8

import pulp as lp
import math
def opt(C,X):
    orderNum=len(X)
    routeNum=len(C)
    routeIdx=range(routeNum)
    orderIdx=range(orderNum)
    print routeIdx,orderIdx
    eps=1.0/10**6
    print eps
    #var_choice=lp.LpVariable.dicts('route',routeIdx,0,1,cat='Integer')
    var_choice=lp.LpVariable.dicts('route',routeIdx,lowBound=0)#尝试松弛掉01变量
    prob=lp.LpProblem("lastMile",lp.LpMinimize)
    
    prob+=lp.lpSum(var_choice[i]*C[i] for i in routeIdx)
    
    for i in orderIdx:
        prob+=lp.lpSum(var_choice[j] for j in X[i])>= (1-eps)
    
    prob.solve()
    print "\n\nstatus:",lp.LpStatus[prob.status]
    print "\n\nobjective:\n",lp.value(prob.objective)
    print "\n\nvariables:\n",[var_choice[i].varValue for i in routeIdx]
    #print "\n\noriginal problem:\n",prob

C=[1,20,3]
X=[[0,1],[1,2]]
opt(C,X)
