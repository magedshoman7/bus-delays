import pandas as pd
import os

'''
The code defines a function calc_merged that merges five different CSV files (delay_file_, headway_file, trip_dur_file, dwell_file, and cycle_file) to create two new CSV files (merged_file and merged_file1).

First, the function reads the contents of the five CSV files into separate dataframes using the read_csv function from the pandas library. It then drops certain columns from three of these dataframes (closest, headway, and dwell) using the drop method from pandas.

Next, the function merges the dataframes closest, trip_duration, and headway on certain columns using the merge method from pandas. The resulting dataframe is then merged with dwell on certain columns using the same merge method. The merged dataframe is then saved to a CSV file specified by the merged_file parameter.

Finally, the function drops certain columns from the merged dataframe and merges it with the cycle dataframe on certain columns. The resulting dataframe is then saved to a CSV file specified by the merged_file1 parameter.
'''


def calc_merged(delay_file_,headway_file,trip_dur_file,dwell_file,cycle_file, merged_file,merged_file1):
    if os.path.isfile(merged_file):
        os.remove(merged_file)
    if os.path.isfile(merged_file1):
        os.remove(merged_file1)

    closest = pd.read_csv(delay_file_)
    headway = pd.read_csv(headway_file)
    trip_duration = pd.read_csv(trip_dur_file)
    dwell = pd.read_csv(dwell_file)
    cycle = pd.read_csv(cycle_file)

    closest = closest.drop(['wheelchair_boarding','block_id','bus_id','trip_id_y','scheduled','route_id_y','feed_timestamp','request_time','departure_time'],axis=1)
    headway = headway.drop(['arrival_time_','start_time','feed_timestamp','request_time'],axis=1)
    dwell = dwell.drop(['stop_code','stop_name','stop_lat','stop_lon','wheelchair_boarding','direction','direction_id','block_id','trip_headsign','arrival_time','departure_time','shape_dist_traveled','arrival_time_',
                            'bus_id','start_time','scheduled','latitude','longitude','timestamp','feed_timestamp','request_time','vehicle_label','closest','delay'],axis=1)



    clo_dur = closest.merge(trip_duration, left_on=['start_date','trip_id_x','route_id_x','vehicle_id'], right_on=['start_date','trip_id','route_id','vehicle_id'],how='left')
    clo_dur_head = clo_dur.merge(headway, left_on=['start_date','trip_id_x','route_id_x','vehicle_id','stop_id','stop_sequence'], right_on=['start_date','trip_id','route_id','vehicle_id','stop_id','stop_sequence_x'],how='left')

    clo_dur_head = clo_dur_head.loc[:,~clo_dur_head.columns.duplicated()]

    clo_dur_head_dwell = clo_dur_head.merge(dwell, left_on=['start_date','trip_id_x','route_id_x','vehicle_id','stop_id','stop_sequence'], right_on=['start_date','trip_id_y','route_id_y','vehicle_id','stop_id','stop_sequence'],how='left')
    clo_dur_head_dwell.to_csv(merged_file, header=True, index=False)

    # closest = closest.drop(['stop_id','stop_code','stop_name','stop_lat','stop_lon','wheelchair_boarding','direction','block_id','trip_headsign','arrival_time','stop_sequence','shape_dist_traveled','arrival_time_',
    #                         'bus_id','trip_id_y','start_time','scheduled','route_id_y','latitude','longitude','timestamp','feed_timestamp','request_time','vehicle_label','closest','delay',
    #                         'direction_id', 'departure_time'],axis=1)
    print(clo_dur_head_dwell.columns)
    print(cycle.columns)
    clo_dur_head_dwell_cycle = clo_dur_head_dwell.merge(cycle, left_on=['start_date','route_id_x_x','vehicle_id','trip_id_x_x'],right_on=['start_time','route_id','vehicle_id','trip_id'],how='left')
    clo_dur_head_dwell_cycle.to_csv(merged_file1, header=True, index=False)



# clo_dur_head_cycle = clo_dur_head.merge(cycle_times, left_on=['start_date','route_id_x','vehicle_id'], right_on=['start_time','route_id','vehicle_id'],how='left')
