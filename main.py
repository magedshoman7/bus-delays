from utils_.functions import *
from utils_.trip_duration_final import check_increasing
from utils_.cycle_times import calc_cycle_times
from utils_.headways import calc_headways
from utils_.dwell import calc_dwell
from utils_.merging import calc_merged

'''
This code can calling several functions that perform calculations on bus data. Here is a brief summary of each function's purpose:

get_stop_delay: Calculates the delay at each stop based on the assumed minimum distance between stops and a given constant value.
check_increasing: Checks the increasing order of the stop times in the given file and filters out any decreasing values. It then calculates the trip duration between each stop and saves the result to a new file.
calc_cycle_times: Calculates the cycle time of each trip by adding up the trip duration between each stop, and saves the result to a new file.
calc_headways: Calculates the headway (the time interval between successive vehicles on the same route) at each stop, and saves the result to a new file.
calc_dwell: Calculates the dwell time (the time spent at a stop) at each stop based on real-time data, and saves the result to a new file.
calc_merged: Merges the results of the previous calculations (delay, headway, trip duration, dwell time, and cycle time) into a single file, and saves the result to a new file.
'''

## calculate delays
# val = 1
# dist = 0.01 # minimum distance for assumed stop
# get_stop_delay(val, dist)

## calculate trip duration
# stops_file = 'data/static/stop_times.txt'
# delay_file_= 'data/results/closest.csv'
# df_stop_times = pd.read_csv(stops_file)
# out_file = 'data/results/trip_duration.csv'
# check_increasing(df_stop_times, delay_file_, out_file)
#
# ## cycle time calculation
# cycles_file_out = 'data/results/cycle_times.csv'
# trip_dur_file = 'data/results/trip_duration.csv'
# calc_cycle_times (trip_dur_file,cycles_file_out)
# # #
# # # headways
# stops_file = 'data/static/stop_times.txt'
# delay_file_ = 'data/results/closest.csv'
# headway_file = 'data/results/headways.csv'
# calc_headways(stops_file, delay_file_, headway_file)
# #
# # # dwell
# feed_file = 'data/RT_VEH_FEED_.csv'
# delay_file_ = 'data/results/closest.csv'
# dwell_file = 'data/results/dwell.csv'
# calc_dwell(feed_file, delay_file_,dwell_file)
#
# # merging
delay_file_ = 'data/results/closest.csv'
headway_file = 'data/results/headways.csv'
trip_dur_file = 'data/results/trip_duration.csv'
dwell_file = 'data/results/dwell.csv'
cycle_file = 'data/results/cycle_times.csv'
merged_file = 'data/results/merged.csv'
merged_file1 = 'data/results/merged1.csv'
calc_merged(delay_file_,headway_file,trip_dur_file,dwell_file, cycle_file,merged_file,merged_file1)



