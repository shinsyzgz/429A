#encoding=utf-8

from loadData import loadData
import pandas as pd
import math
import time
import datetime
import pdb
import copy

def getAppropriateOrders(ori,dest,choiceSet):
    global orders,sites
    thisX=sites.loc[ori,'x']
    thisY=sites.loc[ori,'y']
    xTo=sites.loc[dest,'x']
    yTo=sites.loc[dest,'y']
    choiceX=orders.loc[choiceSet,'dx']
    choiceY=orders.loc[choiceSet,'dy']


def getNext(currentLoad,ordersOnBoard,disTravelled,timeElasped,waitingTime,latePenalty,currentLoc,currentDest,history,depth):
    global maxLoad
    global maxSearchDis
    global locations, orders
    global spots,shops,sites


    #if currentLoc is a site, then try to carry up all the appropriate orders.
    if depth==0:
        currentDest=orders.loc[ordersOnBoard[0],'dest_id']
    if thisLoc.location_type=='sites':
        ordersOnThisSite=orders[orders['ori_id']==thisLoc]
        ordersToCarry=getAppropriateOrders(currentLoc,currentDest,ordersOnThisSite)


    #get orders that is near to currentlocation
    currentX=locations.loc[currentLoc,'x']
    currentY=locations.loc[currentLoc,'y']




    pass

def search(normalOrderToStart):
    global startTime
    global locations,orders
    global maxLoad
    print 'search for %s started at time %.1f'%(normalOrderToStart,time.time()-startTime)
    thisLoad=orders.loc[normalOrderToStart,'num']
    thisLoc=orders.loc[normalOrderToStart,'ori_id']
    getNext(thisLoad,[normalOrderToStart],0,0,0,0,thisLoc,thisLoc,[],0)





if __name__=='__main__':
    maxLoad=140
    maxSearchDis=5000 #meters in x and y directions
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
    search(normalOrders.index[0])


