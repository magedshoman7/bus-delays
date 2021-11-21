import pandas as pd
import os

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