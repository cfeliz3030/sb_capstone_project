import requests
import pandas as pd
import os.path

url = 'http://207.251.86.229/nyc-links-cams/LinkSpeedQuery.txt'
r = requests.get(url)
data = r.text

save_path = os.getcwd()
file_name = 'traffic_avg_speed'
complete_name = os.path.join(save_path, file_name + ".txt")

with open(complete_name, 'w') as f:
    f.write(data)

df = pd.read_csv(complete_name, sep='\t')
print(df.head())
