# Bus Operations Metrics Calculator: Cycle Times, Dwell Times, Headways, and Trip Durations

The project enables you to calculate cycle times, dwell times, headways, and trip duration for buses that uses input data from various sources, such as GPS data, scheduling (GTFS) information, to calculate these important metrics for bus operations.

The code works by first collecting data on the starting and ending times for each bus trip, as well as the number of stops along the route and the average time spent at each stop (dwell time). The code then calculates the total trip duration by summing up the time spent driving and the time spent at each stop.

Next, the code calculates the cycle time, which is the time it takes for a bus to complete one full trip cycle, including any layover or turnaround time at the end of the cycle. This is calculated by adding up the trip duration and the turnaround time, and then dividing by the number of trips per cycle.

The code also calculates the headway, which is the time between two consecutive buses on a route. This is determined by taking the average trip duration for a route and adding in any layover time, and then dividing by the number of buses running on the route.

Finally, the code provides output for each of these metrics, including the trip duration, cycle time, dwell time, and headway, along with any relevant statistics or visualizations to help operators and planners make data-driven decisions for bus operations.

A description for what each code file does is included within the file. 
