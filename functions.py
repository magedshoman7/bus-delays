import pandas as pd
from vincenty import vincenty
import numpy as np
import os, datetime
from datetime import timedelta

'''
This code contains several functions for processing transit data. The main function is get_stop_delay, which takes two arguments: val and dist. val is a binary value (1 or 0) that indicates whether or not to initialize the processing. If val is 1, the function calls init_proc to generate a CSV file of stop and trip information. If val is 0, the function reads in the CSV file. dist is a distance threshold in miles for finding the closest stop to a bus from the real-time feed data.
The init_proc function reads in static transit data from the 'data/static' directory and generates a CSV file of stop and trip information. The stop_info_merge function reads in static transit data and merges stop and trip information to create a single data frame. The get_bus_feed function reads in real-time transit data from the 'RT_VEH_FEED_.csv' file in the specified directory. The get_vehicle_stops function finds the closest stop to each bus and calculates the delay in minutes between the expected arrival time at the stop and the actual arrival time reported by the real-time feed data.
'''

# merge stops info and times
def define_dxns(folder):
    stops_info = pd.read_csv(os.path.join(folder, 'stops.txt'))
    stops_info = stops_info[['stop_id', 'stop_code', 'stop_lat',
                             'stop_lon', 'wheelchair_boarding', 'stop_name']]
    dxns = ['NB','SB','EB','WB']
    stop_names = stops_info['stop_name'].values.tolist()
    dxn_col = []
    for stname in stop_names:
        cnt = 0
        stname_split = stname.split(' ')
        for dxn in dxns:
            if dxn in stname_split:
                dxn_col.append(dxn)
                cnt+=1
        if cnt==0:
            dxn_col.append('CC')
    stops_info['direction'] = dxn_col
    return (stops_info)

def get_trip_info(folder):
    trips_info = pd.read_csv(os.path.join(folder, 'trips.txt'))
    trips_info = trips_info[['block_id', 'trip_id', 'route_id',
                             'direction_id', 'trip_headsign']]
    return trips_info

def stop_info_merge(folder):
    stops_info = define_dxns(folder)
    stop_times = pd.read_csv(os.path.join(folder,'stop_times.txt'))
    stop_times = stop_times[['trip_id', 'arrival_time', 'departure_time',
                             'stop_id', 'stop_sequence', 'shape_dist_traveled']]
    trips = get_trip_info(folder)

    stop_times = pd.merge(trips,stop_times,on='trip_id')
    # print(stop_times.columns)

    # stop_times['arrival_time'] = pd.to_datetime(stop_times['arrival_time'],
    #                                             format= '%H:%M:%S').dt.time

    stop_times['hour'] = stop_times['arrival_time'].str.split(':').apply(lambda x: x[0])
    stop_times['hour'] = stop_times['hour'].apply(lambda x: int(x) if int(x) < 24 else int(x)-24)
    stop_times['min'] = stop_times['arrival_time'].str.split(':').apply(lambda x: x[1])
    stop_times['sec'] = stop_times['arrival_time'].str.split(':').apply(lambda x: x[2])
    stop_times['arrival_time_'] = stop_times.apply(lambda x:datetime.timedelta(hours=int(x['hour']),
                                                                      minutes=int(x['min']),
                                                                      seconds=int(x['sec'])), axis=1)


    stop_info_times = pd.merge(stops_info,stop_times,on='stop_id')
    stop_info_times = stop_info_times.sort_values(['trip_id',
                                                   'stop_sequence'],
                                                  ascending=[True, True])
    return stop_info_times

## get realtime feed data
def get_bus_feed(folder):
    bus_feed = pd.read_csv(os.path.join(folder,'RT_VEH_FEED_.csv'))
    dates = bus_feed['start_date'].unique()
    trips = bus_feed['trip_id'].unique()
    bus_feed['timestamp'] = pd.to_datetime(bus_feed['timestamp'])
    bus_feed['timestamp'] = bus_feed['timestamp'].dt.tz_localize(None)
    # print (bus_feed['timestamp'].head())
    # print (bus_feed.columns)
    return bus_feed, dates, trips

def get_vehicle_stops(df_merge_feed_stop, dist):
    df_merge_feed_stop['closest'] = df_merge_feed_stop.apply(
        (lambda row: vincenty(
            (row['stop_lat'], row['stop_lon']),
            (row['latitude'], row['longitude']), miles=True)),
        axis=1
    )
    closest = df_merge_feed_stop.loc[df_merge_feed_stop.groupby(
        ['stop_lat', 'stop_lon'])["closest"].idxmin()]
    closest = closest[closest['closest'] <= dist]
    # closest = closest[out_columns]

    # closest['timestamp'] = closest['timestamp'] - timedelta(hours=6)
    closest['timestamp'] = closest['timestamp']

    closest['start_date'] = closest['timestamp']. \
        apply(lambda x: str(pd.to_datetime(x).date()))
    closest['arrival_time_'] = closest['arrival_time_']. \
        apply(lambda x: str(x.split(' ')[-1]))
    closest['start_date'] = closest['start_date'].astype(str)
    closest['arrival_time_'] = pd.to_datetime(closest['start_date'] + ' ' + closest['arrival_time_'])
    # print (pd.to_datetime(closest['arrival_time_']))
    # print (pd.to_datetime(closest['timestamp']))
    closest['delay'] = (pd.to_datetime(closest['arrival_time_']) -
                        pd.to_datetime(closest['timestamp'])) / np.timedelta64(1, 'm')
    # print ('done')

    # closest['delay'] = closest['delay'].apply(lambda x:x if abs(x)<1200 else 1440-x)

    closest['delay'] = closest['delay'].apply(
        lambda x: x if (x > -1200 and x < 1200) else (1440 - x if x > 1200 else -(x + 1440)))

    return closest

def init_proc(val):
    if val ==1:
        # stops_folder = ['..','data','static']
        stops_folder = ['..', 'data', 'static']
        stops_folder = os.path.join( [1], stops_folder[2])
        stop_info_times = stop_info_merge(stops_folder)
        out_path = os.path.join(stops_folder,'stops_trips_times.csv')
        stop_info_times.to_csv(out_path,index=False)

def get_stop_delay(val,dist):
    init_proc(val)
    stops_folder = ['..', 'data', 'static']
    stops_merge_file = os.path.join( stops_folder[1], stops_folder[2],'stops_trips_times.csv')
    feed_folder = ['..','data']
    result_folder = ['..','data','results']
    result_folder = os.path.join( result_folder[1], result_folder[2])
    #print (stops_merge_file)
    stops_trips_times = pd.read_csv(stops_merge_file)
    filt_columns = ['stop_id', 'stop_code', 'stop_name','stop_lat', 'stop_lon',
                    'wheelchair_boarding','direction', 'block_id', 'trip_id', 'route_id',
                    'direction_id', 'trip_headsign', 'arrival_time', 'departure_time',
                    'stop_sequence', 'shape_dist_traveled','arrival_time_']

    stops_trips_times = stops_trips_times[filt_columns]
    stops_trips_times = stops_trips_times.sort_values(['arrival_time_'], ascending=['True'])
    print ('getting bus_feed')
    bus_feed, dates, trips = get_bus_feed(os.path.join(feed_folder[1]))
    ## remove delay_file if exist
    if os.path.isfile(os.path.join(result_folder,'closest.csv')):
        os.remove(os.path.join(result_folder,'closest.csv'))

    for date in dates:
        cur_bus_feed = bus_feed[bus_feed['start_date'] == date]
        for trip in trips:
            cur_trip_feed = cur_bus_feed[cur_bus_feed['trip_id'] == trip]
            cur_trip_feed = cur_trip_feed.sort_values(['timestamp'], ascending=['True'])

            # bus_ids = cur_trip_feed['bus_id'].unique()
            trip_stop_info = stops_trips_times[stops_trips_times['trip_id'] == trip]
            # for bus_id in bus_ids:
            # cur_trip_feed_id = cur_trip_feed[cur_trip_feed['bus_id']==bus_id]
            ## drop out feed data less than 5 minutes
            if cur_trip_feed.shape[0]>10:
                # print(trip, bus_id,cur_trip_feed_id.shape[0])
                # min_time = cur_trip_feed_id['timestamp'].min() - timedelta(hours=2)
                # max_time = cur_trip_feed_id['timestamp'].max() + timedelta(hours=2)
                df_merge_feed_stop = pd.merge(trip_stop_info.assign(key=0),
                                              cur_trip_feed.assign(key=0),
                                              on='key').drop('key', axis=1)

                if not df_merge_feed_stop.empty:
                    closest = get_vehicle_stops(df_merge_feed_stop, dist)
                    if not os.path.isfile(os.path.join(result_folder,'closest.csv')):
                        closest.to_csv(os.path.join(result_folder,'closest.csv'),
                                           header=True, index=False)
                    else:
                        closest.to_csv(os.path.join(result_folder,'closest.csv'),
                                           index=False, header=False, mode='a+')



# val = 0
# dist = 0.01 # minimum distance for assumed stop
# get_stop_delay(val,dist)

# calc_headways()
