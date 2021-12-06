# import libraries
from data_collection import taxi_files_list, taxi_data_collect_year, \
    taxi_data_mappings, collect_traffic_sensor, collect_vehicle_collisions, collect_borough_weather
from config import username, password, apptoken, api_key, start_year, end_year
from data_processing import process_taxi_data, process_sensor_data, process_weather_data,\
    process_collision_data

# check if taxi files exist, if not download taxi data
f = taxi_files_list()
if len(f) < (end_year - start_year):
    for y in range(start_year, end_year + 1):
        taxi_data_collect_year(y)

# collect and save taxi map files
taxi_data_mappings()

# collect and save vehicle collision files
collect_vehicle_collisions(apptoken, username, password)

# collect and save traffic sensor files
collect_traffic_sensor()

# collect and weather files
collect_borough_weather(api_key)

# process taxi files
f = taxi_files_list()
for file in f:
    process_taxi_data(file)

# process collisions data
process_collision_data()

# process weather data
process_weather_data()

# process sensor data
process_sensor_data()
