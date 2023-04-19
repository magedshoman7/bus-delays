import pandas as pd
import os

'''
This code defines a function calc_headways that takes three arguments: stops_file, delay_file_, and headway_file. The purpose of this function is to calculate the headways of buses at each stop based on the time differences between their arrivals.

First, the function reads in two CSV files using the Pandas library: stops_file and delay_file_. These files contain information about the stops and the delays of buses, respectively. The relevant columns are selected from stops_file and renamed to cols, and df_delay is trimmed to only the relevant columns listed in delay_cols, which are then renamed to update_cols.

A new dataframe df_out is created to store the output, and its columns are defined by out_cols. This dataframe is then written to a CSV file at headway_file.

Next, the function iterates through each unique stop_id in df_stop_times, which represents a particular stop. For each stop_id, the function retrieves all stop times from df_stop_times that correspond to that stop. These stop times are sorted by their arrival time and assigned an order based on their position in the sorted list.

Then, for each unique date in start_date from df_delay, the function retrieves all delay information for that date and stop from df_delay. The function merges this delay information with the stop times for the current stop and sorts the resulting dataframe by timestamp. If the resulting dataframe contains more than one row, the function calculates the time differences between consecutive rows in the timestamp column using pd.to_datetime().diff().dt.total_seconds(). The number of buses that arrived between consecutive rows is also calculated as the absolute difference in their order values. Finally, the headway is calculated as the time difference divided by the number of buses that arrived between those two rows. The resulting dataframe is then written to the headway_file CSV file.

In summary, this code reads in stop and delay information, calculates the headway for each bus stop, and outputs the results to a CSV file.
'''

def calc_headways(stops_file,delay_file_, headway_file):
    # stops_file = 'data/static/stop_times.txt'
    # delay_file_ = 'data/results/closest.csv'
    if os.path.isfile(headway_file):
        os.remove(headway_file)

    df_stop_times = pd.read_csv(stops_file)
    cols = ['trip_id', 'arrival_time', 'stop_id', 'stop_sequence','shape_dist_traveled']
    df_stop_times = df_stop_times[cols]

    df_delay = pd.read_csv(delay_file_)

    delay_cols = ['stop_id','trip_id_x','route_id_x', 'arrival_time','stop_sequence',
                  'arrival_time_', 'start_time', 'start_date','timestamp','feed_timestamp',
                  'request_time', 'vehicle_id', 'delay']

    update_cols = ['stop_id', 'trip_id', 'route_id', 'arrival_time', 'stop_sequence',
                  'arrival_time_', 'start_time', 'start_date', 'timestamp', 'feed_timestamp',
                  'request_time', 'vehicle_id', 'delay']

    out_cols = ['trip_id', 'stop_id', 'stop_sequence_x', 'order', 'route_id','arrival_time_',
                'start_time', 'start_date', 'timestamp', 'feed_timestamp', 'request_time',
                'vehicle_id', 'delay', 'time_diff', 'total_bus', 'headway']

    df_out = pd.DataFrame(columns=out_cols)
    df_out.to_csv(headway_file,index=False, header=True)

    df_delay = df_delay[delay_cols]
    df_delay.columns = update_cols
    u_dates = df_delay['start_date'].unique()
    u_stopids = df_stop_times['stop_id'].unique()
    for stop_id in u_stopids:
        cur_stop_times = df_stop_times[df_stop_times['stop_id'] == stop_id]
        cur_stop_times = cur_stop_times.sort_values('arrival_time')
        arr_order = range(0, cur_stop_times.shape[0])
        cur_stop_times['order'] = arr_order

        for cdate in u_dates:
            cur_df_delay = df_delay[df_delay['start_date'] == cdate]
            cur_df_delay_stop = cur_df_delay[cur_df_delay['stop_id'] == stop_id]
            df_merge = pd.merge(cur_stop_times,cur_df_delay_stop,on=['trip_id','stop_id'])
            df_merge = df_merge.sort_values('timestamp')
            if df_merge.shape[0]>1:
                df_merge['time_diff'] = pd.to_datetime(df_merge['timestamp']).diff().dt.total_seconds()
                df_merge['total_bus'] = abs(df_merge['order'].diff())
                df_merge['headway'] = df_merge['time_diff']/df_merge['total_bus']
                df_merge = df_merge[out_cols]
                df_merge.to_csv(headway_file, index=False, header=False, mode='a+')

