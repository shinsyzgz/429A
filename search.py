#encoding=utf-8

from loadData import loadData
import pandas as pd
import numpy as np
import math
import time
import datetime
import pdb
import copy
import scipy.spatial.distance as distance
import matplotlib.pyplot as plt
import itertools

def getDis(x1,y1,x2,y2):
    a=x1-x2
    b=y1-y2
    return math.sqrt(a**2+b**2)

def getAppropriateOrders(ori,dest,choiceSet):
    global orders,sites,locations
    filterAngle=30.0
    #print ori,dest
    thisX=sites.loc[ori,'x']
    thisY=sites.loc[ori,'y']
    xTo=locations.loc[dest,'x']
    yTo=locations.loc[dest,'y']
    choiceX=choiceSet.dx
    choiceY=choiceSet.dy
    a=distance.cdist([[thisX,thisY]],[[xTo,yTo]],'euclidean')
    b=distance.cdist([(thisX,thisY)],zip(choiceX,choiceY),'euclidean')
    c=distance.cdist([(xTo,yTo)],zip(choiceX,choiceY),'euclidean')
    ab=a[0]*b[0]
    a= a[0]**2
    b= b[0]**2
    c= c[0]**2
    angle=np.degrees(np.arccos((a+b-c)/2/ab))
    return list(choiceSet.index[angle<filterAngle])
    
    #a=plt.figure()
    #ax=a.add_subplot(111,aspect=1)
    #ax.scatter([thisX],[thisY],marker='^',s=100)
    #choiceX.index=range(len(choiceX))
    #ax.scatter(choiceX,choiceY,marker='s')
    #ax.scatter([xTo],[yTo],marker='o',s=100)
    #for i in range(len(choiceX)):
    #    ax.annotate(i,(choiceX[i],choiceY[i]))
    #plt.show()
    
def getAppropriateSites(ori,dest):
    global orders,sites
    global filterAngle
    #print ori,dest
    thisX=sites.loc[ori,'x']
    thisY=sites.loc[ori,'y']
    xTo=locations.loc[dest,'x']
    yTo=locations.loc[dest,'y']
    choiceX=sites.x
    choiceY=sites.y
    a=distance.cdist([[thisX,thisY]],[[xTo,yTo]],'euclidean')
    b=distance.cdist([(thisX,thisY)],zip(choiceX,choiceY),'euclidean')
    c=distance.cdist([(xTo,yTo)],zip(choiceX,choiceY),'euclidean')
    ab=a[0]*b[0]
    a= a[0]**2
    b= b[0]**2
    c= c[0]**2
    angle=np.degrees(np.arccos((a+b-c)/2/ab))
    return list(sites.index[(angle<filterAngle)&(b<a)])

def getDest(orderList):
    global orders
    dest=[orders.loc[i,'dest_id'] for i in orderList]
    return dest

def getShortestPath(startNode,nodesToVisit):
    global locations
    if len(nodesToVisit)==0:
        return ([],0,0)
    allNodes=[startNode]+nodesToVisit
    nodeX=locations.loc[allNodes,'x']
    nodeY=locations.loc[allNodes,'y']
    dis=distance.cdist(zip(nodeX,nodeY),zip(nodeX,nodeY),'euclidean')
    
    a=range(1,len(nodesToVisit)+1)
    permu=[]
    permu+=itertools.permutations(a,len(a))
    
    minDis=10**8
    minPermu=[]
    for i in permu:
        t=[0]+list(i)
        thisDis=0
        for j in range(1,len(t)):
            thisDis=thisDis+dis[t[j-1],t[j]]
        if thisDis<minDis:
            minDis=thisDis
            minPermu=t
    
    tmp=np.array(nodesToVisit)
    idx=np.array(minPermu[1:])-1
    return (tmp[idx],minDis,dis[minPermu[0],minPermu[1]])
    
    
    #fig=plt.figure()
    #ax=fig.add_subplot(111,aspect=1)
    #ax.scatter(nodeX,nodeY)
    #for i in range(len(nodeX)):
    #    ax.annotate(i,(nodeX[i],nodeY[i]))
    #plt.show()
    
def getPossibleSite(currentLoc,routeHistory):
    global  sites,locations
    global siteSearchRange
    
    currentX=locations.loc[currentLoc,'x']
    currentY=locations.loc[currentLoc,'y']
    otherSites=list(set(sites.index)-set(routeHistory)-set([currentLoc]))
    otherX=sites.loc[otherSites,'x']
    otherY=sites.loc[otherSites,'y']
    dis=distance.cdist([(currentX,currentY)],zip(otherX,otherY),'euclidean')
    minIndex=int(np.argmin(dis,axis=1))

    minDis= dis[0,minIndex]
    if minDis<siteSearchRange:
        return otherSites[minIndex]
    else:
        return 0
    
    

def getNext(currentLoad,ordersOnBoard,disTravelled,timeElasped,waitingTime,latePenalty,currentLoc,nodesToGo,orderHistory,routeHistory,timeHistory,depth):
    global maxLoad
    global maxSearchDis
    global locations, orders
    global spots,shops,sites
    global resultsNodes,resultsTime
    global maxDepth
    SPEED=250 #meters per minute
    
    #if depth>maxDepth:
    #    return
    
    ordersOnBoard=copy.deepcopy(ordersOnBoard)
    orderHistory=copy.deepcopy(orderHistory)
    routeHistory=copy.deepcopy(routeHistory)
    timeHistory=copy.deepcopy(timeHistory)
    
    pre=str(depth)+'  '+'-'*(depth+1)
    print '%sload:%d, orders on board: %s, time elasped: %.1f, nodes visited: %s'%(pre,currentLoad,'/'.join(ordersOnBoard),timeElasped, '|'.join(routeHistory))
    print '%stime history:'%(pre),timeHistory
    print '%scurrently at: %s, nodesToGo: %s'%(pre,currentLoc,'|'.join(nodesToGo))
    routeHistory=routeHistory+[currentLoc]
    
    if depth==0:
        currentDest=orders.loc[ordersOnBoard[0],'dest_id']
    else:
        if len(nodesToGo)==0:
            (currentRoute,routeLength,nextStopDis)=getShortestPath(currentLoc,getDest(ordersOnBoard))
            if len(currentRoute)==0:
                #if locations.loc[currentLoc,'location_type']!='sites':
                print '%sno more orders to deliver. record and return.'%(pre)
                #record results, which we can then combine later.
                resultsNodes.append(routeHistory)
                resultsTime.append(timeHistory+[(timeElasped,timeElasped,[])])
                return 
            else:
                currentDest=currentRoute[0]
        else:
            currentDest=nodesToGo[0]
            
    
    #if currentLoc is a site, then try to carry up all the appropriate orders.
    if locations.loc[currentLoc,'location_type']=='sites':
        ordersOnThisSite=orders[orders['ori_id']==currentLoc]
        if len(ordersOnBoard)==0:#this means, during our delivery process, we finished all the delivery task and arrived at a new site.
            #we need to start all over. 
            #but, start all over will consume too much calculation. we can skip this and combine short results later.
            #just like the group method in didi project.
            
            #for k in ordersOnThisSite.index:
            #    currentLoad+=orders.loc[k,'num']
            #    ordersOnBoard=ordersOnBoard+[k]
            #    orderHistory=orderHistory+[k]
            #    print '%sempty bag, adding new order %s on this site.'%(pre,k)
            #    getNext(currentLoad,ordersOnBoard,disTravelled,timeElasped,waitingTime,latePenalty,currentLoc,nodesToGo,orderHistory,routeHistory,timeHistory,depth)
            pass
        else:
            ordersToCarry=set(getAppropriateOrders(currentLoc,currentDest,ordersOnThisSite))-set(ordersOnBoard)-set(orderHistory)
            print '%scurrently at a site. '%(pre)
            if len(ordersToCarry)==0:
                print '%sno orders available to carry. This search goes unecessary distance, abondaned.'%(pre)
                return
            print '%smore orders are available to carry: %s'%(pre, '/'.join(ordersToCarry))
            combo=[]
            for i in range(len(ordersToCarry)):
                combo+=itertools.combinations(ordersToCarry,i)
            for i in combo:
                t=list(i)
                
                #if we are at a site, we must fetch sth unless we are at the first depth.
                if depth>0 and len(t)==0:
                    continue
                tmpSum=0
                for j in t:
                    tmpSum+=orders.loc[j,'num']
                if tmpSum+currentLoad<maxLoad:
                    currentLoadThis=tmpSum+currentLoad
                    ordersOnBoardThis=ordersOnBoard+t
                    orderHistoryThis=orderHistory+t
                    print  '%spicking up orders: [%s], new load: %d'%(pre,'/'.join(t),currentLoadThis)
                    (currentRoute,routeLength,nextStopDis)=getShortestPath(currentLoc,getDest(ordersOnBoardThis))
                    currentDest=currentRoute[0]
                    if len(currentRoute)>1:
                        nodesToGo=currentRoute[1:]
                    else:
                        nodesToGo=[]
                    print '%snew route: %s, route length: %.1f'%(pre,'->'.join(currentRoute),routeLength)
                    print '%smoving to: %s'%(pre,currentDest)
                    timeElaspedWithoutNewSites=timeElasped+nextStopDis/SPEED
                    #routeHistoryWithoutNewSites=routeHistory+[currentDest]
                    #timeHistoryWithoutNewSites=timeHistory+[(timeElasped,timeElasped)]
                    disTravelledWithoutNewSites=disTravelled+nextStopDis
                    if (depth==0) :
                        timeHistoryThis=timeHistory+[(timeElasped,timeElasped,ordersOnBoardThis,'pickup')]
                    else:
                        timeHistoryThis=timeHistory+[(timeElasped,timeElasped,t,'pickup')]
                    getNext(currentLoadThis,ordersOnBoardThis,disTravelledWithoutNewSites,timeElaspedWithoutNewSites,waitingTime,latePenalty,currentDest,nodesToGo,orderHistoryThis,routeHistory,timeHistoryThis,depth+1)


                    #try to see if this rider can visit ONE other site before serving orders.
                    #at present, very simple rule: go to the nearest unvisited site.
                    #sitesToVisit=set(getAppropriateSites(currentLoc,currentDest))-set(routeHistory)
                    if depth > maxDepth:
                        continue
                    siteToVisit=getPossibleSite(currentLoc,routeHistory)
                    if siteToVisit==0 or currentLoad>100:
                        print '%sno nearby unvisited sites or bag almost full(%d).'%(pre,currentLoad)
                        continue

                    print '%smoving to sites %s, trying to collect more pacakges.'%(pre,siteToVisit)                
                    currentDest=siteToVisit
                    dis=getDis(locations.loc[currentLoc,'x'],locations.loc[currentLoc,'y'],locations.loc[currentDest,'x'],locations.loc[currentDest,'y'])
                    timeElaspedWithNewSites=timeElasped+ dis/SPEED
                    #routeHistoryWithNewSites=routeHistory+[currentDest]
                    #timeHistoryWithNewSites=timeHistory+[(timeElasped,timeElasped)]
                    disTravelledWithNewSites=disTravelled+dis
                    getNext(currentLoadThis,ordersOnBoardThis,disTravelledWithNewSites,timeElaspedWithNewSites,waitingTime,latePenalty,currentDest,[],orderHistoryThis,routeHistory,timeHistoryThis,depth+1)


    elif locations.loc[currentLoc,'location_type']=='spots':
                   
        arrivalTime=timeElasped
        timeNeeded=0
        
        for o in ordersOnBoard:
            if orders.loc[o,'dest_id']==currentLoc:
                newlyHandled=[o]
                num=orders.loc[o,'num']
                currentLoad-=num
                timeNeeded=3*math.sqrt(num)+5
                timeElasped+=timeNeeded
                ordersOnBoard.remove(o)
                print '%sOrder %s delivered, time consumed: %.1f, remaining load: %d'%(pre,o,timeNeeded,currentLoad)
                break
        assert timeNeeded>0
        departureTime=timeElasped
        timeHistory=timeHistory+[(arrivalTime,departureTime,newlyHandled,'deliver')]
        #nodesToGo.remove(currentLoc)
        #timeHistory=timeHistory+[timeElasped]
        #go to the next spot directly (copy the code of sites handling)
        if len(nodesToGo)>0:
            currentDest=nodesToGo[0]
            nodesToGo=np.delete(nodesToGo,[0])
            print '%smoving to: %s'%(pre,currentDest)
            nextStopDis=getDis(locations.loc[currentLoc,'x'],locations.loc[currentLoc,'y'],locations.loc[currentDest,'x'],locations.loc[currentDest,'y'])
            timeElaspedWithoutNewSites=timeElasped+nextStopDis/SPEED
            #routeHistoryWithoutNewSites=routeHistory+[currentDest]
            #timeHistoryWithoutNewSites=timeHistory+[(arrivalTime,departureTime)]
            disTravelledWithoutNewSites=disTravelled+nextStopDis
            getNext(currentLoad,ordersOnBoard,disTravelledWithoutNewSites,timeElaspedWithoutNewSites,waitingTime,latePenalty,currentDest,nodesToGo,orderHistory,routeHistory,timeHistory,depth+1)
        else:
            print '%sno more nodes to go'%(pre)
        
        if depth<maxDepth:
            #go to the nearest sites if the search is not too deep. otherwise, stop going deeper and devliver all the goods
            #so that we get a result to record.
            siteToVisit=getPossibleSite(currentLoc,routeHistory)
            if siteToVisit==0 or currentLoad>100:
                print '%sno nearby unvisited sites or bag almost full(%d).'%(pre,currentLoad)
            else:
                print '%smoving to sites %s, trying to collect more pacakges.'%(pre,siteToVisit)                
                currentDest=siteToVisit
                dis=getDis(locations.loc[currentLoc,'x'],locations.loc[currentLoc,'y'],locations.loc[currentDest,'x'],locations.loc[currentDest,'y'])
                timeElaspedWithNewSites=timeElasped+ dis/SPEED
                #routeHistoryWithNewSites=routeHistory+[currentDest]
                #timeHistoryWithNewSites=timeHistory+[(arrivalTime,departureTime)]
                disTravelledWithNewSites=disTravelled+dis
                getNext(currentLoad,ordersOnBoard,disTravelledWithNewSites,timeElaspedWithNewSites,waitingTime,latePenalty,currentDest,[],orderHistory,routeHistory,timeHistory,depth+1)


def search(normalOrderToStart):
    global startTime
    global locations,orders
    global maxLoad
    print 'search for %s started at time %.1f'%(normalOrderToStart,time.time()-startTime)
    thisLoad=orders.loc[normalOrderToStart,'num']
    thisLoc=orders.loc[normalOrderToStart,'ori_id']
    getNext(thisLoad,[normalOrderToStart],0,0,0,0,thisLoc,[],[normalOrderToStart],[],[],0)




#main program started here.

maxLoad=140
terminateTime=12*60.0
filterAngle=30.0
siteSearchRange=1000 #only search sites within 5km
maxDepth=10 #search depth
startTime=time.time()
(locations,orders)=loadData('../original_data')
sites=locations[locations['location_type']=='sites']
shops=locations[locations['location_type']=='shops']
spots=locations[locations['location_type']=='spots']

numOfSites=len(sites)
numOfOrders=len(orders)
normalOrders=orders[orders['order_type']==0]
numOfNormalOrders=len(normalOrders)
print 'data set has %d orders, of which %d are normal orders.'%(numOfOrders,numOfNormalOrders)


resultsNodes=[]
resultsTime=[]
search(normalOrders.index[10])


for i in range(len(resultsNodes)):
    print '\n---results no.',i,'---'
    print 'node sequence:\n\t',resultsNodes[i],'\ntimes:\n\t',resultsTime[i]



