import pandas as pd
import os

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

