import json
import os
import pandas as pd
from sodapy import Socrata
from creds import username, password, apptoken
import logging

logger = logging.getLogger(__name__)
file = logging.FileHandler(f'{os.getcwd()}/vehicle_collisions.log', mode='a')
logger.setLevel(logging.INFO)
special_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
file.setFormatter(special_format)
logger.addHandler(file)

logger.info('Connecting to data API')
client = Socrata('data.cityofnewyork.us',
                 app_token=apptoken,
                 username=username,
                 password=password)

logger.info('Collecting data from API')
results = client.get("h9gi-nx95", limit=250000)

save_path = os.getcwd()
file_name = 'vehicle_collisions'
complete_name = os.path.join(save_path, file_name + ".txt")

logger.info('Writing data to file')
with open(complete_name, 'w') as f:
    json.dump(results, f)
logger.info('Finished collecting new data.')

results_df = pd.DataFrame.from_records(results)
print(results_df.columns)
print(results_df.head())
