#!/usr/bin/python3

import json
import requests
import time
import datetime 
from dateutil import parser
from dateutil.relativedelta import relativedelta
import pytz

# Define the measurement name
measurement_name = "getAlarmList"

# Load the configuration from local.json
with open('local.json', 'r') as json_file:
    config = json.load(json_file)

base_url = 'https://eu5.fusionsolar.huawei.com/thirdData/'
login_credentials = { "userName": config["fusionsolar"]["userName"], "systemCode": config["fusionsolar"]["systemCode"]}

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

epoch_time = int(time.time())
# print(epoch_time)

url = base_url + 'getStationList'
response = s.post(url)
# print('getStationList\n' + json.dumps(response.json(), indent=1) + '\n\n')

stationCodes = []
for station in response.json()['data']:
    stationCodes.append(station['stationCode'])

for stationCode in stationCodes:
    url = base_url + 'getDevList'
    response = s.post(url, json={ "stationCodes": stationCode, "collectTime": int(epoch_time * 1000) })
    print('getDevList\n' + json.dumps(response.json(), indent=1) + '\n\n')

for devList in response.json()['data']:
    query_time = epoch_time
    devId = devList['id']
    devTypeId = devList['devTypeId']
    if devTypeId not in [1, 10, 17, 38, 39, 41, 47]: # The following device types are supported
      continue
    stationCode = devList['stationCode']

    url = base_url + 'getDevHistoryKpi'
    while True:
        # response = s.post(url, json={ "devTypeId": devTypeId, "devIds": devId, "startTime": int(query_time.timestamp()*1000), "endTime": int(time.time() * 1000 - 10000)})
        response = s.post(url, json={ "devTypeId": devTypeId, "devIds": devId, "startTime": int(epoch_time*1000), "endTime": int((epoch_time+3600*72)*1000)})
        retry_delay = 10
        if response.status_code != 200:
            print(f"Received != 200 status code. Retrying in {retry_delay} seconds... Status code {response.status_code}. Url: {url}")
            time.sleep(retry_delay)            
        elif response.json()['failCode'] == 407:
            print(f"Received 407 status code, we are being rate limited. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        else:
            data = response.json()
            break
    # if response.json()["success"] != 'true':
        # print('ERROR: Empty response in the data, probably wrongly formatted request')
        # print(measurement_name + '\n' + json.dumps(data, indent=1) + '\n\n')
        # time.sleep(retry_delay/2)        
        # continue
    data = response.json()

    if "data" not in data:
        # print(f"Error: data not found in response, status code {response.status_code}")
        if response.status_code == 200:
            if data['failCode'] == 305 and data['message'] == 'USER_MUST_RELOGIN':
                print('User must re-login')
                # Re-logging will kick previous login, if you are relogging too much it's probably
                # someone or something else using the API too, like a crontab
                s = login_to_huawei(s)
            else:
                print(data)
    else:
        print(data)
        # Iterate over each entry in the JSON data and create a point for each one
        for entry in data["data"]:
            print(entry["sns"])

url = base_url + measurement_name

beginTime = int((epoch_time-3600)*1000)
endTime = int(epoch_time*1000)
for stationCode in stationCodes:
    response = s.post(url, json={ "stationCodes": stationCode, "beginTime": beginTime, "endTime": endTime, "language": "en_US"})
    retry_delay = 10
    if response.status_code != 200:
        print(f"Received != 200 status code. Retrying in {retry_delay} seconds... Status code {response.status_code}. Url: {url}")
        time.sleep(retry_delay)            
    elif response.json()['failCode'] == 407:
        print(f"Received 407 status code, we are being rate limited. Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)
    else:
        data = response.json()
    if response.json()["success"] != 'true':
        print('ERROR: Empty response in the data, probably wrongly formatted request')
        print(measurement_name + '\n' + json.dumps(data, indent=1) + '\n\n')
        continue
    data = response.json()
    print(data)