#!/usr/bin/python3

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import requests
import time
import datetime 

# Load the configuration from local.json
with open('local.json', 'r') as json_file:
    config = json.load(json_file)

# Set up the InfluxDB client
client = InfluxDBClient(url=config['influxdb']['url'], token=config['influxdb']['token'], org=config['influxdb']['org'])

# Set the bucket name
bucket_name = influxdb_org = config['influxdb']['bucket']

base_url = 'https://eu5.fusionsolar.huawei.com/thirdData/'
login_credentials = { "userName": config["fusionsolar"]["userName"], "systemCode": config["fusionsolar"]["systemCode"]}

s = requests.session()
response = s.post(url, json=login)

if response.status_code == 200: 
    # {'data': None, 'success': True, 'failCode': 0, 'params': {}, 'message': None}
    data = response.json()
    if data['success'] != True:
        print('Error at login. Credentials error?')
        if data['message'] != None:
            print(data['Message'])
        else:
            print(data)
        exit()
    print("Login successfull")
    s.headers['XSRF-TOKEN'] = s.cookies.get('XSRF-TOKEN')
else:
    print('Error at login:', response.status_code, response.json())


url = 'https://eu5.fusionsolar.huawei.com/thirdData/getStationList'
response = s.post(url)
# print('getStationList\n' + json.dumps(response.json(), indent=1) + '\n\n')

stationCodes = []
for station in response.json()['data']:
    stationCodes.append(station['stationCode'])

# Get a write API instance for the bucket
write_api = client.write_api(write_options=SYNCHRONOUS)
write_api = client.write_api(bucket=bucket_name)

epoch_time = int(time.time())
currentTime = epoch_time 
for stationCode in stationCodes:
    url = 'https://eu5.fusionsolar.huawei.com/thirdData/getStationRealKpi'
    while True:
        response = s.post(url, json={ "stationCodes": stationCode, "currentTime": epoch_time*1000 })
        retry_delay = 60
        if response.status_code != 200:
            print(f"Received != 200 status code. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)            
        elif response.json()['failCode'] == 407:
            print(f"Received 407 status code, we are being rate limited. Retrying in {retry_delay} seconds... Only " + str((datetime.datetime.now() - query_time).days) + " days to go!")
            time.sleep(retry_delay)
        else:
            data = response.json()
            break
    data = response.json()
    #print('getStationRealKpi\n' + json.dumps(data, indent=1) + '\n\n')

    # Iterate over each entry in the JSON data and create a point for each one
    for entry in data["data"]:
        print("Saving data from: " + str(datetime.datetime.fromtimestamp(data["params"]["currentTime"]/1000).strftime('%Y-%m-%d %H:%M:%S')) + ' (' + str(data["params"]["currentTime"]/1000) + ')')
        # Create a new point for the entry
        point = Point("getStationRealKpi") \
            .tag("stationCode", entry["stationCode"]) \
            .field("total_income", entry["dataItemMap"].get("total_income")) \
            .field("total_power", entry["dataItemMap"].get("total_power")) \
            .field("day_power", entry["dataItemMap"].get("day_power")) \
            .field("day_income", entry["dataItemMap"].get("day_income")) \
            .field("real_health_state", entry["dataItemMap"].get("real_health_state")) \
            .field("month_power", entry["dataItemMap"].get("month_power")) \
            .time(data["params"]["currentTime"], WritePrecision.MS)

        # Write the point to the bucket
        write_api.write(bucket=bucket_name, record=point)

# Close the write API connection
write_api.__del__()
