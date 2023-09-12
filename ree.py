#!/usr/bin/python3

# Documentation: https://www.ree.es/en/apidatos
# I haven't found a list of API calls; I inspected the network traffic to obtain them.

import requests
import json
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta, timezone
import time

# Load the configuration from local.json
with open('local.json', 'r') as json_file:
    config = json.load(json_file)

headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
  # 'X-API-KEY': 'request_your_personal_token_sending_email_to_consultasios@ree.es',
  'X-API-KEY': config["ree"]["X-API-TOKEN"],
  'Accept': 'application/json; application/vnd.esios-api-v1+json',
  'Content-Type': 'application/json'
}

# Set up the InfluxDB client
client = InfluxDBClient(url=config['influxdb']['url'], token=config['influxdb']['token'], org=config['influxdb']['org'])

# Set the bucket name
bucket_name = config['influxdb']['bucket']

# Get a write API instance for the bucket
write_api = client.write_api(write_options=SYNCHRONOUS)
write_api = client.write_api(bucket=bucket_name)

# Define the measurement name
measurement_name = "ree"

indicators = [ "1001", "1295", "1739" ]
  # 1001: Término de facturación de energía activa del PVPC 2.0TD
  # 1739: Precio de la energía excedentaria del autoconsumo para el mecanismo de compensación simplificada (PVPC)
  # 10229: Término de facturación de energía activa del PVPC peaje por defecto suma componentes: !!! No existen datos para la fecha seleccionada !!!
  # 1295: Generación T.Real Solar fotovoltaica

def influxdb_save_ree(data):
  indicator = str(data['indicator']['id'])
  if indicator not in ["1001", "1295", "1739"]:
    print("The code to save to influxdb the indicator", indicator, "is missing")
  else:
    for entry in data["indicator"]["values"]:
      print(json.dumps(entry, indent=4, ensure_ascii=False))
      # Create a new point for the entry
      point = Point(measurement_name) \
        .tag("indicator", indicator) \
        .tag("name", data['indicator']['name']) \
        .tag("short_name", data['indicator']['short_name'])

      # Parse the UTC timestamp string
      timestamp = datetime.strptime(entry['datetime_utc'], '%Y-%m-%dT%H:%M:%SZ')

      # Convert the parsed datetime to a UTC aware datetime object
      utc_aware_time = timestamp.replace(tzinfo=timezone.utc)

      # Convert the UTC aware datetime object to a string in RFC3339 format
      point.time(utc_aware_time.isoformat())

      exclude_keys = [
        "datetime",
        "datetime_utc",
        "tz_time",
        "geo_ids"
      ]

      for key, value in entry.items():
        if key not in exclude_keys:
          point.field(key, value)

      # Write the point to the bucket
      print("Saving hourly data | indicator:", indicator, "Fecha:", timestamp, entry['value'])
      write_api.write(bucket=bucket_name, record=point)

for indicator in indicators:

  # Query InfluxDB to know up to which date do we have
  query_api = client.query_api()
  query = (f'from(bucket:"{bucket_name}")'
        ' |> range(start: 0, stop: now())'
       f' |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")'
       f' |> filter(fn: (r) => r["indicator"] == "{indicator}")'
        ' |> sort(columns: ["_time"])'
        ' |> last()')
  result = query_api.query(query)

  if result and result[0].records:
    dateToCheck = result[0].records[0]['_time'] + timedelta(days=1) # previous date + 1 day
    print("Found previous values in filter", measurement_name, "for", indicator + ". Using timer after previous last value:", str(dateToCheck))
  else:
    dateToCheck = datetime(2022, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
    print("There are no previous", measurement_name, "values for", indicator + ". Using default hard coded value:", str(dateToCheck))

  yesterday = (datetime.today() - timedelta(days=1)).astimezone(timezone.utc)
  today = datetime.today().astimezone(timezone.utc)
  tomorrow = (datetime.today() + timedelta(days=1)).astimezone(timezone.utc)

  if indicator == "1295":
    maxDate = tomorrow
  else:
    maxDate = today

  while dateToCheck <= maxDate + timedelta(days=1): # Rewrite this so it gets better intervals 
                            # A month or two should be fine
    params = {
    #  "values": "false",
      "start_date": dateToCheck.strftime("%Y-%m-%d") + "T00:00:00",
      "end_date": dateToCheck.strftime("%Y-%m-%d") + "T23:55:00",
      "locale": "es",
      "geo_agg": "sum"
    }
    if indicator != "1295":
      params["time_trunc"] = "hour"

    r = requests.get("https://api.esios.ree.es/indicators/" + str(indicator), params=params, headers=headers)

    if r.status_code == 200:
      # Increment the date by one day
      dateToCheck += timedelta(days=1)
      data = r.json()
      # Process the data as needed
      # print(json.dumps(data, indent=4, ensure_ascii=False))
      influxdb_save_ree(data)
      time.sleep(3) # hug
    else:
      print("Request failed with status code:", r.status_code)
      time.sleep(20)

write_api.__del__()
exit()