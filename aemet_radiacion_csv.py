#!/usr/bin/python3

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import requests
import time
import datetime
from datetime import datetime, timedelta
from dateutil import parser
from dateutil.relativedelta import relativedelta
import pytz
import csv
import os

# Load the configuration from local.json
with open('local.json', 'r') as json_file:
    config = json.load(json_file)

aemet_apikey = config['aemet_apikey']
url = "https://opendata.aemet.es/opendata/api/red/especial/radiacion/"

# Set up the InfluxDB client
client = InfluxDBClient(url=config['influxdb']['url'], token=config['influxdb']['token'], org=config['influxdb']['org'])

# Set the bucket name
bucket_name = config['influxdb']['bucket']

# Define the measurement name
r = requests.get(url, params={"api_key": aemet_apikey})
r.raise_for_status()

r_data = requests.get(r.json()['datos'], stream=True) # https://stackoverflow.com/a/39064678
r_data.raise_for_status()

csv_reader = csv.DictReader(r_data.iter_lines(decode_unicode=True))
data = []
for row in csv_reader:
    data.append(row)

date = data[0]['RADIACION SOLAR']

if r_data.status_code == 200:
    # Set the filename
    date_str = datetime.strptime(date, '%d-%m-%y').strftime('%Y%m%d')
    filename = f'dailyRadData/radiaciondiaria_{date_str}.csv'  # Specify the folder in the filename

    if os.path.isfile(filename):
        # print(f"File '{filename}' already exists. Data will not be saved.")
        exit()

    # Create the directory if it doesn't exist
    folder = os.path.dirname(filename)
    os.makedirs(folder, exist_ok=True)

    with open(filename, 'wb') as f:
        r_data = requests.get(r.json()['datos'], stream=True) # https://stackoverflow.com/a/39064678
        for chunk in r_data.iter_content(chunk_size=1024):
            f.write(chunk)
    exit()