#!/usr/bin/python3

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import requests
import time
import datetime 
from dateutil import parser
import pytz

# Load the configuration from local.json
with open('local.json', 'r') as json_file:
    config = json.load(json_file)

# Set up the InfluxDB client
client = InfluxDBClient(url=config['influxdb']['url'], token=config['influxdb']['token'], org=config['influxdb']['org'])

# Set the bucket name
bucket_name = influxdb_org = config['influxdb']['bucket']

base_url = 'https://eu5.fusionsolar.huawei.com/thirdData/'
login_credentials = { "userName": config["fusionsolar"]["userName"], "systemCode": config["fusionsolar"]["systemCode"]}

# Define the measurement name
measurement_name = "getKpiStationHour"

s = requests.session()

def login_to_huawei(s):
    response = s.post(base_url + 'login', json=login_credentials)

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
        print("Login successful")
        s.headers['XSRF-TOKEN'] = s.cookies.get('XSRF-TOKEN')
        return s
    else:
        print('Error at login:', response.status_code, response.json())

s = login_to_huawei(s)

url = base_url + 'getStationList'
response = s.post(url)
# print('getStationList\n' + json.dumps(response.json(), indent=1) + '\n\n')

stationCodes = []
for station in response.json()['data']:
    stationCodes.append(station['stationCode'])

# Get a write API instance for the bucket
write_api = client.write_api(write_options=SYNCHRONOUS)
write_api = client.write_api(bucket=bucket_name)

epoch_time = int(time.time())
query_time = datetime.datetime(2022, 9, 14, 22, 0, 0)

for stationCode in stationCodes:  
    # Query InfluxDB to get the latest time entry was saved
    query_api = client.query_api()
    query = (f'from(bucket:"{bucket_name}")'
              ' |> range(start: 0, stop: now())'
             f' |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")'
             f' |> filter(fn: (r) => r["stationCode"] == "{stationCode}")'
              ' |> sort(columns: ["_time"])'
              ' |> last()')
    result = query_api.query(query)

    if result and result[0].records:
        query_time = result[0].records[0]['_time']

    while query_time < datetime.datetime.utcnow().replace(tzinfo=pytz.utc):
        url = base_url + measurement_name
        while True:
            response = s.post(url, json={ "stationCodes": stationCode, "collectTime": int(query_time.timestamp() * 1000) })
            retry_delay = 30
            if response.status_code != 200:
                print(f"Received != 200 status code. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)            
            elif response.json()['failCode'] == 407:
                print(f"Received 407 status code, we are being rate limited. Retrying in {retry_delay} seconds... Only " + str((datetime.datetime.utcnow().replace(tzinfo=pytz.utc) - query_time).days) + " days to go!")
                time.sleep(retry_delay)
            else:
                data = response.json()
                break
        data = response.json()
        # print(measurement_name + '\n' + json.dumps(data, indent=1) + '\n\n')

        if "data" not in data:
            # print(f"Error: data not found in response, status code {response.status_code}")
            if response.status_code == 200:
                if data['failCode'] == 305 and data['message'] == 'USER_MUST_RELOGIN':
                    print('User must re-login')
                    # Re-logging will kick previous login, if you are relogging too much it's probably
                    # someone or something else using the API too, like a crontab
                    s = login_to_huawei(s)
                    query_time -= datetime.timedelta(days=1)
                else:
                    print(data)
        else:
            # Iterate over each entry in the JSON data and create a point for each one
            for entry in data["data"]:
                print("Saving data from: " + str(datetime.datetime.fromtimestamp(entry["collectTime"]/1000).strftime('%Y-%m-%d %H:%M:%S')) + ' (' + str(entry["collectTime"]/1000) + ')')
                # Create a new point for the entry
                point = Point(measurement_name) \
                    .tag("stationCode", entry["stationCode"]) \
                    .field("radiation_intensity", entry["dataItemMap"]["radiation_intensity"]) \
                    .field("inverter_power", entry["dataItemMap"]["inverter_power"]) \
                    .field("power_profit", entry["dataItemMap"]["power_profit"]) \
                    .field("theory_power", entry["dataItemMap"]["theory_power"]) \
                    .field("ongrid_power", entry["dataItemMap"]["ongrid_power"]) \
                    .time(entry["collectTime"], WritePrecision.MS)

                # Write the point to the bucket
                write_api.write(bucket=bucket_name, record=point)

        query_time += datetime.timedelta(days=1)
        time.sleep(2)  

# Close the write API connection
write_api.__del__()