#!/usr/bin/python3

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import requests
import time
from datetime import datetime, timedelta
from dateutil import parser, tz
from dateutil.relativedelta import relativedelta
import pytz
import csv
import os
import argparse 
from timezonefinder import TimezoneFinder
from multiprocessing import Pool
import math
from astral.sun import sun
from astral import Observer
import logging
from logging.handlers import RotatingFileHandler

# Load the configuration from local.json
with open('local.json', 'r') as json_file:
    config = json.load(json_file)

# Define the log file path and folder
log_folder = 'logs'
log_file = 'aemet_radiacion.log'
log_file_path = os.path.join(log_folder, log_file)

# Create the "logs" folder if it doesn't exist
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Initialize the logger with log rotation
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a rotating log handler to limit the log file
handler = RotatingFileHandler(log_file_path, maxBytes=512, backupCount=5)

# Define the log format
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

aemet_apikey = config['aemet_apikey']
base_url = "https://opendata.aemet.es/opendata/api/"

# Set up the InfluxDB client
client = InfluxDBClient(url=config['influxdb']['url'], token=config['influxdb']['token'], org=config['influxdb']['org'])

# Set the bucket name
bucket_name = config['influxdb']['bucket']

# Get a write API instance for the bucket
write_api = client.write_api(write_options=SYNCHRONOUS)
write_api = client.write_api(bucket=bucket_name)

def solar_to_epoch(latitude, longitude, altitude, date, solar_time): # Alternative: https://gist.github.com/mnpenner/8b6e7cceb930402d3f37ba9c59fa1d26
    """
    Convert solar time to epoch timestamp.

    Args:
    - latitude (float): Latitude of the location.
    - longitude (float): Longitude of the location.
    - altitude (float): Altitude of the location.
    - date (str): Date in the format 'YYYY-MM-DD'.
    - solar_time (float): Solar time in hours.

    Returns:
    - int: Epoch timestamp corresponding to the solar time.
    """

    # Create an observer using the given latitude, longitude and altitude
    observer = Observer(latitude, longitude, altitude)

    # Convert date string to date object
    date_obj = datetime.strptime(date, '%Y-%m-%d').date()

    # Convert solar time to hours and minutes
    hours, minutes = divmod(int(solar_time * 60), 60)

    # Convert date string to datetime object
    solar_datetime = datetime.strptime(date, '%Y-%m-%d')

    # Add hours and minutes to datetime object
    solar_datetime += timedelta(hours=hours, minutes=minutes)

    # Calculate sunrise and sunset times for the specified date
    s = sun(observer, date=date_obj)

    # Save solar noon
    solar_noon = s['noon']
    logger.info("%s %s", solar_datetime, solar_noon)

    # Calculate the time difference between solar noon and UTC
    offset = solar_datetime - solar_noon

    # Calculate the adjusted solar time
    adjusted_solar_time = int(solar_time) + int(offset.seconds / 3600)
    adjusted_solar_minutes = int((solar_time % 1) * 60)

    # Handle cases where adjusted solar time exceeds 24 or is negative
    if adjusted_solar_time >= 24:
        adjusted_solar_time -= 24
    elif adjusted_solar_time < 0:
        adjusted_solar_time += 24

    # Create a datetime object for the adjusted solar time
    solar_datetime = date_obj.combine(date_obj, date_obj.time(hour=adjusted_solar_time, minute=adjusted_solar_minutes))

    # Get the epoch timestamp for the solar datetime
    solar_epoch = int(solar_datetime.timestamp())

    return solar_epoch

def process_entry(entry):
    """
    Process a single entry in the JSON data and write it to InfluxDB.

    Args:
    - entry (dict): A dictionary representing a single entry in the JSON data.

    Returns:
    - None
    """
    # Create a new point for the entry
    point = Point(measurement_name) \
        .tag("idema", entry["idema"]) \
        .tag("ubi", entry["ubi"])

    # print(entry["ubi"],entry)

    for key, value in entry.items():
        # Check if the key is 'ifema' or 'fint'
        if key not in ['ifema', 'fint']:
            # Check if the key is 'geo850'
            if key == 'geo850':
                # Add the value to the InfluxDB point
                point.field(key, value['value'])
            else:
                # Add the key-value pair to the InfluxDB point
                point.field(key, value)

    # convert the string to a datetime object
    dt = parser.parse(entry["fint"])

    # get the timezone at the given latitude and longitude
    tf = TimezoneFinder()
    lat, lon = entry["lat"], entry["lon"]
    tzname = pytz.timezone(tf.timezone_at(lng=lon, lat=lat))

    # check if daylight saving time is in effect at the given datetime
    dst_in_effect = tzname.localize(dt, is_dst=None).dst() != timedelta(0)

    # apply the timezone and daylight saving time to the datetime object
    dt_tz = tzname.localize(dt, is_dst=dst_in_effect)

    # convert the datetime object to epoch time
    epoch = int(dt_tz.timestamp())
    point.time(epoch - 60, WritePrecision.S)

    # Write the point to the bucket
    write_api.write(bucket=bucket_name, record=point)

if __name__ == '__main__':
    r = requests.get(base_url + 'observacion/convencional/todas', params={"api_key": aemet_apikey})
    r = requests.get(r.json()['datos'], params={"api_key": aemet_apikey})
    json_data = r.json() # We can get lat/lon from here

    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', help='CSV filename')
    args = parser.parse_args()

    if args.filename:
        with open(args.filename, 'r') as file:
            csv_reader = csv.DictReader(file, delimiter=';')
            data = []
            for row in csv_reader:
                data.append(row)
    else:
        # Define the measurement name
        r = requests.get(base_url + 'red/especial/radiacion/', params={"api_key": aemet_apikey})
        r.raise_for_status()
        # print(r.json())

        r_data = requests.get(r.json()['datos'], stream=True) # https://stackoverflow.com/a/39064678
        r_data.raise_for_status()

        csv_reader = csv.DictReader(r_data.iter_lines(decode_unicode=True), delimiter=';')
        data = []
        for row in csv_reader:
            data.append(row)

    row_tipo = [] # To save the header so we can access the hours


    # Define the measurement name
    measurement_name = "sun_radiation"

    identifier_array = []

    for row in data:
        # print(row.keys())
        if None not in row:
            date = row['RADIACION SOLAR']
        else:
            if row[None][1] == 'Tipo': # Header
                row_tipo = row[None]
            else: # Data row
                # print('estation', row['RADIACION SOLAR'], date, 'identifier', row[None][0])
                # print(row[None][1:])
                identifier = row[None][0]
                estation = row['RADIACION SOLAR']
                latitude = 0.0
                longitude = 0.0
                for data in json_data:
                    if data['idema'] == identifier:
                        # print(data)
                        latitude = data['lat']
                        longitude = data['lon']
                        altitude = data['alt']
                        break
                if latitude == 0 or longitude == 0: # In case the station isn't on the observacion/convencional/todas endpoint
                    logger.warning("%s (%s) isn't on the observacion/convencional/todas endpoint. Data will not be saved", estation, identifier)
                    continue
                else:
                    logger.info("Saving data from %s (%s), date: %s", estation, identifier, date)
                    identifier_array.append(identifier)
                for i, value in enumerate(row[None][1:]):
                    if not value.isdigit():
                        radiation_type = value
                    elif row_tipo[i+1].isdigit():
                        solar_time = float(row_tipo[i+1])
                        value_date = datetime.strptime(date, '%d-%m-%y') + timedelta(hours=solar_time)
                        value_date_epoch = int(value_date.timestamp())
                        # actual_time = solar_to_epoch(latitude, longitude, altitude, datetime.strptime(date, '%d-%m-%y').strftime('%Y-%m-%d'), solar_time)
                        # logger.info("%s %s", solar_datetime, solar_noon)
                        # logger.info("Saving data estation: %s identifier: %s radiation_type: %s value_date: %s value: %s", estation, identifier, radiation_type, value_date, value)
                        # Create a new point for the entry
                        point = Point(measurement_name) \
                            .tag("estation", estation) \
                            .tag("identifier", identifier) \
                            .tag("radiation_type", radiation_type) \
                            .field("value", float(value)) \
                            .time(int(value_date_epoch), WritePrecision.S)
                        # Write the point to the bucket
                        write_api.write(bucket=bucket_name, record=point)

    # Define the measurement name
    measurement_name = "aemet_observacion_convencional"
    # Create a multiprocessing pool
    pool = Pool()
    for data in json_data:
        if data['idema'] in identifier_array: # We save only relevant data
            pool.apply_async(process_entry, args=(data,))
    # Close the pool and wait for all processes to complete
    pool.close()
    pool.join()

    # Close the write API connection
    write_api.__del__()
    exit()