import os
import requests


def taxi_data_collect_year(year):
    """
    Collect taxi data for specified year from NY TLC
    Years available 2015-2020
    ex.https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-04.csv
    :param year: int
    :return:
    """
    month = 0
    save_path = os.getcwd()
    file_name = f'taxi_data_{year}'
    complete_name = os.path.join(save_path, file_name + ".csv")

    while year:
        month += 1
        if month < 10:
            str_month = str(month).zfill(2)
        else:
            str_month = str(month)
        print(f'Downloading {str_month} {year}')
        url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year}-{str_month}.csv'
        r = requests.get(url, stream=True)

        with open(complete_name, 'ab') as f:
            for chunk in r.iter_content(10000):
                f.write(chunk)
        print(f'Finished {str_month} {year}')
        if month == 12:
            break
    return f'{year} File Download Complete'


print(taxi_data_collect_year(2018))
