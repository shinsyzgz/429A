#encoding=utf-8
import pandas as pd
import math

def loadData(path):
    #accept 'path' as input which specifies the location of the input data.
    print 'loading data...'

    startTimeConstant=8*60;

    sites=pd.read_csv(open(path+'/1.csv','r'),sep=',',header=0)
    spots=pd.read_csv(open(path+'/2.csv','r'),sep=',',header=0)
    shops=pd.read_csv(open(path+'/3.csv','r'),sep=',',header=0)
    orders=pd.read_csv(open(path+'/4.csv','r'),sep=',',header=0)
    twOrders=pd.read_csv(open(path+'/5.csv','r'),sep=',',header=0)

    #combine all the locations (origins and destinations)
    tmp=sites.copy()
    tmp.columns=['location_id','lng','lat']
    tmp['location_type']='sites'
    locations=tmp.copy()
    
    tmp=spots.copy()
    tmp.columns=['location_id','lng','lat']
    tmp['location_type']='spots'
    locations=locations.append(tmp)

    tmp=shops.copy()
    tmp.columns=['location_id','lng','lat']
    tmp['location_type']='shops'
    locations=locations.append(tmp)

    locations.index=locations['location_id']

    #transform lat and lng to x and y (APPROXIMATION)
    #after the search has been done, we can then calculate the exact distance.
    _y_center=locations['lat'].mean()
    PI=math.pi
    R=6378137.0
    locations['x']=locations['lng']*PI*R/180.0*math.cos(_y_center*PI/180.0)
    locations['y']=locations['lat']*PI*R/180.8

    _xMin=locations['x'].mean()
    _yMin=locations['y'].mean()
    locations['x']=locations['x']-_xMin
    locations['y']=locations['y']-_yMin


    #combine the two type of orders. assign type 0 to normal orders so that when calculating 
    #the time window penalty we can just multiply the penalty by the order_type to eliminate the
    #time window penalty for normal orders.
    allOrders=twOrders.copy()
    allOrders['order_type']=1
    allOrders.columns=['order_id','dest_id','ori_id','pickup_time','delivery_time','num','order_type']
    allOrders['pickup_hour']=pd.to_datetime(allOrders['pickup_time']).apply(lambda x: x.hour)
    allOrders['pickup_minute']=pd.to_datetime(allOrders['pickup_time']).apply(lambda x: x.minute)
    allOrders['delivery_hour']=pd.to_datetime(allOrders['delivery_time']).apply(lambda x: x.hour)
    allOrders['delivery_minute']=pd.to_datetime(allOrders['delivery_time']).apply(lambda x: x.minute)

    allOrders.loc[:,'pickup_time']=allOrders['pickup_hour']*60+allOrders['pickup_minute']-startTimeConstant
    allOrders.loc[:,'delivery_time']=allOrders['delivery_hour']*60+allOrders['delivery_minute']-startTimeConstant

    allOrders=allOrders.drop(['pickup_hour','pickup_minute','delivery_hour','delivery_minute'],axis=1)
    tmp=orders.copy()
    tmp.columns=['order_id','dest_id','ori_id','num']
    tmp['pickup_time']=0
    tmp['delivery_time']=0
    tmp['order_type']=0

    allOrders=allOrders.append(tmp)
    allOrders.index=allOrders.order_id

    allOrders['ox']=allOrders.apply(lambda x: locations.loc[x.ori_id,'x'],axis=1)
    allOrders['oy']=allOrders.apply(lambda x: locations.loc[x.ori_id,'y'],axis=1)
    allOrders['dx']=allOrders.apply(lambda x: locations.loc[x.dest_id,'x'],axis=1)
    allOrders['dy']=allOrders.apply(lambda x: locations.loc[x.dest_id,'y'],axis=1)
    
    allOrders.to_csv('allOrders.csv')
  


    #print allOrders.loc[['F0001','E0001'],:]


    print 'load complted.'

    return (locations,allOrders)

def getDis(lng1,lat1,locs):
    PI=math.pi
    R=6378137.0
    toRadian=PI/180.0
    tmp=locs.copy()
    tmp['lng_delta']=(tmp['lng']-lng1)/2.0*toRadian
    tmp['lat_delta']=(tmp['lat']-lat1)/2.0*toRadian
    lat1=lat1*toRadian
    tmp['lat2']=tmp['lat']*toRadian
    tmp['sin_delta_lat']=tmp['lat_delta'].apply(math.sin)
    tmp['cos_lat2']=tmp['lat2'].apply(math.cos)
    tmp['sin_delta_lng']=tmp['lng_delta'].apply(math.sin)

    dis=2*R*(tmp['sin_delta_lat']**2+tmp['cos_lat2']*tmp['sin_delta_lng']**2*math.cos(lat1)).apply(math.sqrt).apply(math.asin)

    return dis


if __name__=='__main__':
    loadData('../original_data')
