import pandas as pd
from datetime import timedelta
import os

'''
This code takes in a CSV file of trip data (delay_file_), as well as a CSV file of stop times (df_stop_times), and produces an output CSV file (out_file) containing information about each trip, such as the trip ID, vehicle ID, route ID, direction ID, start date, hour, distance traveled, speed, and duration.

The code works by iterating over each unique date in the delay_file_ and then iterating over each unique trip ID within that date. For each trip, the code first checks if there are any erroneous timestamps by calling the check_misassignment function, which calculates the time difference between consecutive timestamps and checks if any are negative. If there are no negative time differences, the function returns a check_val of 0, indicating no mismatching. If there are negative time differences, the function returns a check_val of 1, indicating mismatching timestamps.

Next, the duration_estimate function is called, which takes in the current trip data, the check_val, and the current date, and calculates the distance traveled and the duration of the trip. If check_val is 1, meaning there are mismatching timestamps, the function calculates the duration of the trip by splitting it into two parts: the first part from the start of the trip to 59 minutes and 59 seconds past midnight, and the second part from 0 hours and 0 minutes and 0 seconds to 23 hours and 59 minutes and 59 seconds past midnight the next day. The function then calculates the total time of the trip as the sum of these two parts. If check_val is 0, meaning there are no mismatching timestamps, the function calculates the duration of the trip as the difference between the maximum and minimum timestamps.

After calculating the distance and duration of the trip, the code checks if the distance traveled is greater than 100 (presumably in some units of distance). If it is, the code calculates the speed of the trip as the ratio of the distance traveled to the distance between the first and last stops, and the duration of the trip as the product of the distance traveled and the ratio of the distance between the first and last stops to the duration of the trip. Finally, the code appends this information to a dataframe, df_out, which is later saved to the output CSV file.
'''

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
