#!/bin/bash
set -x
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install pandas==1.2.5
sudo python3 -m pip install boto3
sudo python3 -m pip install s3fs
sudo python3 -m pip install uszipcode
sudo python3 -m pip install requests
sudo python3 -m pip install logging
sudo python3 -m pip install sodapy
sudo python3 -m pip install pyarrow
sudo python3 -m pip install meteostat
sudo python3 -m pip freeze
