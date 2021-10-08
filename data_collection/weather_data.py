import json
import os
import requests
from creds import api_key

# obtain weather data by city
city = 'bronx'
link1 = f'https://api.openweathermap.org/data/2.5/weather?q={city}&units=imperial&APPID={api_key}'

# obtain weather data by lat + lon
test_ll = {"latitude": "40.68358", "longitude": "-73.97617"}  # test Brooklyn,NY coordinates
link2 = f"https://api.openweathermap.org/data/2.5/weather?lat={test_ll['latitude']}&lon={test_ll['longitude']}&units=imperial&APPID={api_key}"

r = requests.get(link1)
weather = r.json()

save_path = os.getcwd()
file_name = 'traffic_weather'
complete_name = os.path.join(save_path, file_name + ".txt")

with open(complete_name, 'a') as f:
    json.dump(weather, f, indent=1)

print('Location:', weather['name'])
print('Temp:', weather['main']['temp'])
print('Main:', weather['weather'][0]['main'])
print('Desc:', weather['weather'][0]['description'])
print('Visibility:', weather['visibility'])
print('Wind Speed:', weather['wind']['speed'])
