#!/usr/bin/python3

from influxdb_client import InfluxDBClient, Point, WritePrecision
import json

# Load the configuration from local.json
with open('local.json', 'r') as json_file:
    config = json.load(json_file)

# Set up InfluxDB client
client = InfluxDBClient(url=config['influxdb']['url'], token=config['influxdb']['token'], org=config['influxdb']['org'])

# Set the bucket name
bucket_name = influxdb_org = config['influxdb']['bucket']

# Define the predicate to delete all records in the bucket
predicate = '_measurement="ree"'
predicate = 'indicator = 1295'

# Initialize the delete API
delete_api = client.delete_api()

# Delete all records in the bucket
delete_api.delete(predicate=predicate, start='1970-01-01T00:00:00Z', stop='2220-01-01T00:00:00Z', bucket=bucket_name)