import pandas as pd
import os
import numpy as np

def calc_dwell(feed_file,delay_file_,dwell_file):
    if os.path.isfile(dwell_file):
        os.remove(dwell_file)

    _feed_file = pd.read_csv(feed_file)
    _delay_file_ = pd.read_csv(delay_file_)

    _feed_file['start_date'] = _delay_file_['timestamp']. \
        apply(lambda x: str(pd.to_datetime(x).date()))
    _delay_file_['start_date'] = _delay_file_['timestamp']. \
        apply(lambda x: str(pd.to_datetime(x).date()))
    unique_dates = _delay_file_['start_date'].unique()

    for unique_date in unique_dates:
        closest = _delay_file_[_delay_file_['start_date'] == unique_date]
        feed = _feed_file[_feed_file['start_date'] == unique_date]

        unique_ids = closest['trip_id_x'].unique()

        for unique_id in unique_ids:
            closest_ = closest[closest['trip_id_x'] == unique_id]
            feed_ = feed[feed['trip_id'] == unique_id]

            unique_lats = closest_['latitude'].unique()

            for unique_lat in unique_lats:
                closest__ = closest_[closest_['latitude'] == unique_lat]
                feed__ = feed_[feed_['latitude'] == unique_lat]

                unique_longs = closest__['longitude'].unique()

                for unique_long in unique_longs:
                    closest___ = closest__[closest__['longitude'] == unique_long]
                    feed___ = feed__[feed__['longitude'] == unique_long]
                    # print(closest___.columns)
                    # print(feed___.columns)
                    df_all = pd.merge( closest___,feed___, on=['latitude'])
                    # print(df_all.columns)
                    # df_all = df_all.drop_duplicates()
                    df_all['timestamp_y'] = pd.to_datetime(df_all['timestamp_y'])
                    df_all['dwell'] = df_all.groupby(['closest'])['timestamp_y'].diff()
                    closest___['dw'] = df_all['dwell'].sum()

                    # print(closest___.head(5))

                    closest___['dw'] = closest___['dw'].astype(str)
                    closest___['dw'] = closest___['dw'].replace("0.0", "0:0:0")
                    # closest___['hour'] = closest___['dw'].str.split(':').str[0]
                    closest___['minute'] = closest___['dw'].str.split(':').str[1]
                    closest___['seconds'] = closest___['dw'].str.split(':').str[2]
                    closest___['seconds'] = closest___['seconds'].str.split('.').str[0]
                    # closest___['hour'] = closest___['hour'].astype(int)
                    closest___['minute'] = closest___['minute'].astype(int)
                    closest___['seconds'] = closest___['seconds'].astype(int)
                    closest___['dwell'] = (closest___['seconds']/60) + (closest___['minute'])

                    if not os.path.isfile(dwell_file):
                        closest___.to_csv(dwell_file, header=True, index=False)
                    else:
                        closest___.to_csv(dwell_file, index=False, header=False, mode='a+')