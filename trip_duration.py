import pandas as pd
from datetime import timedelta
import os

def check_misassignment(cur_trip):
    cur_trip['time_diff'] = pd.to_datetime(cur_trip['timestamp']).diff().dt.total_seconds()
    df_check = cur_trip[cur_trip['time_diff'] < 0].shape[0]
    if df_check == 0:
        check_val = 0
    else:
        check_val = 1
    return check_val

def duration_estimate(cur_trip, check_val, cur_date):
    if check_val:  # if mismatch found
        ref_val = pd.to_datetime(cur_date + ' ' + str(timedelta(hours=0, minutes=59, seconds=59)))
        min_val = pd.to_datetime(cur_date + ' ' + str(timedelta(hours=0, minutes=0, seconds=0)))
        cur_trip['timestamp'] = pd.to_datetime(cur_trip['timestamp'])
        ref_trip = cur_trip[(cur_trip['timestamp'] >= min_val) & (cur_trip['timestamp'] < ref_val)][[
            'stop_sequence', 'shape_dist_traveled', 'timestamp']]
        if ref_trip.shape[0] > 0:  # process after 24 hours separately and add ..
            tt_extra = pd.to_datetime(ref_trip['timestamp']).max() - min_val
            cur_trip = cur_trip[cur_trip['timestamp'] >= ref_val]
            max_time = pd.to_datetime(cur_date + ' ' + str(timedelta(hours=23, minutes=59, seconds=59)))
            tt_ = max_time - pd.to_datetime(cur_trip['timestamp']).min()
            dist_ = ref_trip['shape_dist_traveled'].max() - cur_trip['shape_dist_traveled'].min()
            tt_ = tt_ + tt_extra
        else:
            dist_ = cur_trip['shape_dist_traveled'].max() - cur_trip['shape_dist_traveled'].min()
            tt_ = pd.to_datetime(cur_trip['timestamp']).max() - pd.to_datetime(cur_trip['timestamp']).min()
    else:  # if no mismatching
        dist_ = cur_trip['shape_dist_traveled'].max() - cur_trip['shape_dist_traveled'].min()
        tt_ = pd.to_datetime(cur_trip['timestamp']).max() - pd.to_datetime(cur_trip['timestamp']).min()
    return [dist_, tt_]


def check_increasing(df_stop_times, delay_file_, out_file):

    if os.path.isfile(out_file):
        os.remove(out_file)

    df_out = pd.DataFrame(
        columns=['trip_id', 'vehicle_id', 'route_id', 'direction_id',
                 'start_date', 'hour', 'cval', 'duration'])

    dft = pd.read_csv(delay_file_)
    u_dates = dft['start_date'].unique()
    for cur_date in u_dates:
        cur_dft = dft[dft['start_date'] == cur_date]
        trip_id = cur_dft['trip_id_x'].unique()
        for trip_ in trip_id:
            cur_trip = cur_dft[cur_dft['trip_id_x'] == trip_]
            cur_trip = cur_trip.sort_values('stop_sequence')
            route_id = cur_trip['route_id_x'].unique()[0]
            direction_id = cur_trip['direction_id'].unique()[0]
            vehicle_id = cur_trip['vehicle_id'].unique()[0]
            period = pd.to_datetime(cur_trip['timestamp'].values[-1]).hour
            ### remove erroneous timestamps--> timestamps and stops sequence should increase correspondingly
            if cur_trip.shape[0] > 0:
                check_val = check_misassignment(cur_trip)
                [dist_, tt_] = duration_estimate(cur_trip, check_val, cur_date)

                trip_stop = df_stop_times[df_stop_times['trip_id'] == trip_]
                trip_stop = trip_stop.sort_values('stop_sequence')
                all_stops = trip_stop['stop_sequence'].unique()
                last_stop = all_stops[-1]
                end_dist_ = trip_stop[trip_stop['stop_sequence'] == last_stop]['shape_dist_traveled'].tolist()[0]
                if (dist_>100):
                    df_out.loc[len(df_out)] = [trip_, vehicle_id, route_id, direction_id, cur_date, period,
                          dist_ / end_dist_,  (end_dist_ / dist_) * (tt_.total_seconds() / 60)]

                    # print(trip_, vehicle_id, route_id, direction_id, cur_date, period,
                    #       dist_/end_dist_, tt_, (end_dist_ / dist_) * (tt_.total_seconds() / 60))
    df_out.to_csv(out_file, index=False)
