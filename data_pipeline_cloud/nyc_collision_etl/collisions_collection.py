import os
import sys
import time
from io import BytesIO
import boto3
import logging
from collisions_utils import user_filepath
import requests

logger = logging.getLogger(__name__)
# file = logging.FileHandler(f'{user_filepath}/data_collection.log', mode='a')
streamHandler = logging.StreamHandler(sys.stdout)
logger.setLevel(logging.INFO)
special_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
streamHandler.setFormatter(special_format)
logger.addHandler(streamHandler)


def collect_vehicle_collisions():
    """ Collects vehicle collision data using Socrata API."""
    start = time.time()
    logger.info('Collecting Vehicle Collisions Data')

    url = 'https://data.cityofnewyork.us/resource/h9gi-nx95.csv?$limit=2000000&$order=crash_date'
    r = requests.get(url)

    save_path = user_filepath.split('/')[-1]
    file_name = 'vehicle_collisions.csv'
    complete_name = os.path.join(save_path, file_name)

    logger.info('Writing data to file')

    s3_bucket = user_filepath.split('/')[2]
    s3 = boto3.client('s3')

    bytesIO = BytesIO(bytes(r.content))
    with bytesIO as data:
        s3.upload_fileobj(data, s3_bucket, complete_name)

    logger.info('Finished collecting new collision data.')
    end = time.time()
    logger.info("Collisions data complete,Execution time in minutes: %s ", (end - start) // 60)