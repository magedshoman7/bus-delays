import pandas as pd
import os
import numpy as np

'''
This code calculates the dwell time for bus stops. The input consists of three files: feed_file, delay_file_, and dwell_file. The function starts by removing the existing dwell_file if it already exists. Then, the feed_file and delay_file_ are read as Pandas data frames.
The code then gets the unique dates from the delay_file_ and iterates over them. For each date, it gets the corresponding data from feed_file and delay_file_. It then gets the unique trip_id from the delay_file_ data for the current date and iterates over them. For each trip_id, it gets the corresponding data from feed_file and delay_file_.
For each unique latitude and longitude from the closest column of the closest___ data, the corresponding data from feed___ is merged based on the latitude column. It then calculates the dwell time by subtracting the timestamps of the current and previous rows for the timestamp_y column and assigns the result to the dwell column. The function then sums the dwell column for each unique closest value and assigns the result to the dw column of closest___.
The function then formats the dw column to hour:minute:second format. Finally, the function writes the resulting data frame to dwell_file. If the file does not exist, the function writes the header row, otherwise, it appends the data to the existing file.
'''

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
