import pandas as pd
import os

#This code calculates the cycle times for a set of bus trips, based on a file containing trip durations. It takes in two input parameters: the name of the file containing trip durations (trip_duration_file), and the name of the file to output the cycle times to (cycles_file_out).
#The code first checks if the output file already exists, and deletes it if it does. Then, it reads in the trip duration file as a pandas dataframe and extracts the unique dates from the "start_date" column.
#The code then loops over each unique date, and for each date, loops over each unique route ID. For each route, the code then loops over each unique vehicle ID, and sorts the dataframe by the hour column. The code then iterates over every two consecutive rows in the sorted dataframe, and if the rows belong to the same trip and have opposite direction IDs, it considers them to be part of a single cycle. The cycle ID is incremented for each new cycle encountered, and the total cycle time (sum of the durations for the two consecutive trips) is added to the output dataframe.
#Finally, the code outputs the resulting dataframe with columns for start time, route ID, trip ID, vehicle ID, cycle ID, and cycle time to the specified output file.



def calc_cycle_times (trip_duration_file,cycles_file_out):

    if os.path.isfile(cycles_file_out):
        os.remove(cycles_file_out)

    df = pd.read_csv(trip_duration_file)
    dates = df['start_date'].unique()
    df_out = pd.DataFrame(columns=['start_time','route_id', 'trip_id','vehicle_id','cycle_id', 'cycle_time'])
    for cdate in dates:
        df_cur_date = df[df['start_date'] == cdate]
        rids = df_cur_date['route_id'].unique()

        for rid in rids:
            cur_df = df_cur_date[df_cur_date['route_id'] == rid]
            vids = cur_df['vehicle_id'].unique()


            for vid in vids:
                cur_df_vid = cur_df[cur_df['vehicle_id'] == vid]
                cur_df_vid = cur_df_vid.sort_values('hour')
                cur_df_vid = cur_df_vid.reset_index(drop=True)

                cycle_id = 0
                for idx in range(0, cur_df_vid.shape[0], 2):
                    listval = [idx,idx+1]

                    filt_df = cur_df_vid[cur_df_vid.index.isin(listval)]
                    # print (filt_df['trip_id'].values)
                    filt_df = filt_df.reset_index(drop=True)
                    if filt_df.shape[0] == 2 and abs(filt_df['direction_id'][1]-filt_df['direction_id'].values[0])==1:
                        cycle_id+=1
                        for cur_trip in filt_df['trip_id'].values:
                            # print ([cdate, rid, cur_trip, vid, cycle_id, filt_df['duration'].values[0]+filt_df['duration'][1]])
                            df_out.loc[len(df_out)] = [cdate, rid, cur_trip, vid, cycle_id, filt_df['duration'].values[0]+filt_df['duration'][1]]

    # print (df_out.head())
    df_out.to_csv(cycles_file_out, index=False)

# calc_cycle_times ()
