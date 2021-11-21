
from google.transit import gtfs_realtime_pb2
#from dotenv import load_dotenv
import os, random
import requests
import pandas as pd
from google.protobuf.json_format import MessageToDict
import threading
from multiprocessing import Process
import sched, time, datetime
s = sched.scheduler(time.time, time.sleep)

def get_RT_Veh_feed():
    # threading.Timer(60.0, get_RT_Veh_feed).start()
    url = ('https://www.metrostlouis.org/RealTimeData/StlRealTimeVehicles.pb')
    #load_dotenv()
    #print ('starting..')
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url, allow_redirects=True)
    columns = ['bus_id', 'trip_id', 'start_time', 'start_date', 'scheduled', 'route_id',
               'latitude', 'longitude', 'timestamp', 'feed_timestamp','request_time','vehicle_id', 'vehicle_label']
    df = pd.DataFrame(columns=columns)
	
    if response.status_code == 200:
        try:
	    #print ('start collecting data..')
            feed.ParseFromString(response.content)
            dictfeeds = MessageToDict(feed)
            feed_timestamp = dictfeeds['header']['timestamp']
            request_time = datetime.datetime.now()
            busses = feed.entity
            for bus in busses:
                bus_id = bus.id
                trip_id = bus.vehicle.trip.trip_id
                start_time = bus.vehicle.trip.start_time
                start_date = bus.vehicle.trip.start_date
                scheduled = bus.vehicle.trip.schedule_relationship
                route_id = bus.vehicle.trip.route_id
                lat = bus.vehicle.position.latitude
                lon = bus.vehicle.position.longitude
                timestamp = bus.vehicle.timestamp
                vehicle_id = bus.vehicle.vehicle.id
                vehicle_label = bus.vehicle.vehicle.label
                df.loc[len(df)] = [bus_id, trip_id, start_time, start_date, scheduled, route_id,
                                   lat, lon, timestamp, feed_timestamp,request_time, vehicle_id, vehicle_label]

            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('utc').dt.tz_convert('US/Central')
            df['feed_timestamp'] = pd.to_datetime(df['feed_timestamp'], unit='s').dt.tz_localize('utc').dt.tz_convert(
                'US/Central')

        except:
            print ('an error occured')
    #print ('data collection done ..')
    if not os.path.isfile('data/RT_VEH_FEED_.csv'):
	#print ('saving to new file ...')
        df.to_csv('data/RT_VEH_FEED_.csv', index=False)
    else:
	#print ('appending to file ..')
        df.to_csv('data/RT_VEH_FEED_.csv', mode='a+', index=False, header=False)

    # print(time.time())
    #s.enter(30, 1, get_RT_Veh_feed)
    return df


def get_RT_GL_Feed():
    threading.Timer(60.0, get_RT_GL_Feed).start()
    url = ('https://www.metrostlouis.org/RealTimeData/StlRealTimeGLVehicle.pb')
    #load_dotenv()
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url, allow_redirects=True)
    feed.ParseFromString(response.content)
    busses = feed.entity
    columns = ['bus_id', 'trip_id', 'start_time', 'start_date', 'scheduled', 'route_id',
               'latitude', 'longitude', 'timestamp', 'vehicle_id', 'vehicle_label']
    df = pd.DataFrame(columns=columns)
    for bus in busses:
        bus_id = bus.id
        trip_id = bus.vehicle.trip.trip_id
        start_time = bus.vehicle.trip.start_time
        start_date = bus.vehicle.trip.start_date
        scheduled = bus.vehicle.trip.schedule_relationship
        route_id = bus.vehicle.trip.route_id
        lat = bus.vehicle.position.latitude
        lon = bus.vehicle.position.longitude
        timestamp = bus.vehicle.timestamp
        vehicle_id = bus.vehicle.vehicle.id
        vehicle_label = bus.vehicle.vehicle.label
        df.loc[len(df)] = [bus_id, trip_id, start_time, start_date, scheduled, route_id,
                           lat, lon, timestamp, vehicle_id, vehicle_label]

    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('utc').dt.tz_convert('US/Central')

    if not os.path.isfile('feed/RT_GL_Feed.csv'):
        df.to_csv('feed/RT_GL_Feed.csv', index=False)
    else:
        df.to_csv('feed/RT_GL_Feed.csv', mode='a+', index=False, header=False)
    return df

def get_feed_updated():
    threading.Timer(30, get_feed_updated).start()
    url = ('https://www.metrostlouis.org/RealTimeData/StlRealTimeTrips.pb')
    #load_dotenv()
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url, allow_redirects=True)
    feed.ParseFromString(response.content)
    dictfeeds = MessageToDict(feed)
    columns = ['bus_id', 'trip_id', 'start_time', 'start_date',  'scheduled', 'route_id',
               'stop_sequence', 'stop_timestamp', 'delay','stop_id', 'vehicle_id', 'vehicle_label','timestamp']
    df = pd.DataFrame(columns=columns)
    for dictfeed in dictfeeds['entity']:

        trips = dictfeed['tripUpdate']['trip']
        vehicle = dictfeed['tripUpdate']['vehicle']
        stopUpdates = dictfeed['tripUpdate']['stopTimeUpdate']
        for stopUpdate in stopUpdates:
            df.loc[len(df)] = [dictfeed['id'],trips['tripId'],trips['startTime'],trips['startDate'],trips['scheduleRelationship'],trips['routeId'],
                   stopUpdate['stopSequence'],stopUpdate['departure']['time'],stopUpdate['departure']['delay'],stopUpdate['stopId'],
                   vehicle['id'], vehicle['label'],dictfeed['tripUpdate']['timestamp']]
    df['stop_timestamp'] = pd.to_datetime(df['stop_timestamp'], unit='s').dt.tz_localize('utc').dt.tz_convert(
        'US/Central')
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('utc').dt.tz_convert('US/Central')

    if not os.path.isfile('feed/updated_feed.csv'):
        df.to_csv('feed/updated_feed.csv', index=False)
    else:
        val = str(random.randint(1,10000))
        fname = os.path.join('feed', 'temp'+val+'.csv')
        df.to_csv(fname, index=False, header=True)
        df1 = pd.read_csv('feed/updated_feed.csv')
        df2 = pd.read_csv(fname)
        df_out = pd.merge(df1, df2, how='outer', indicator=True)
        df_out = df_out[df_out['_merge'] == 'right_only']
        df_out = df_out.drop('_merge',1)
        print(df_out.shape)
        df_out = pd.concat([df1,df_out])
        print (df1.shape, df_out.shape)
        df_out.to_csv('data/updated_feed.csv', mode='a+', index=False, header=False)
        # df.to_csv('feed/updated_feed.csv', mode='a+', index=False, header=False)
    return df


while True:
    try:
	print ('starting')
	get_RT_Veh_feed()
	time.sleep(30)
        #s.enter(30, 1, get_RT_Veh_feed)
        #s.run()
    except:
        pass
    time.sleep(15)
    print ('trying again')

# if __name__=='__main__':
#     p1 = Process(target=get_RT_Veh_feed)
#     p1.start()
    # p1.join()
    # p2 = Process(target=get_RT_GL_Feed)
    # p2.start()
    # p3 = Process(target=get_feed_updated)
    # p3.start()

