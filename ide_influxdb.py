#!/usr/bin/python3

import requests
import time
import json
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta, timezone

# Ide Credentials:
username = config["ide"]["username"]
password = config["ide"]["password"]

# Set up the InfluxDB client
client = InfluxDBClient(url=config['influxdb']['url'], token=config['influxdb']['token'], org=config['influxdb']['org'])

# Set the bucket name
bucket_name = config['influxdb']['bucket']

# Define the measurement name
measurement_name = "ide"

s = requests.session()
s.post("https://www.i-de.es/consumidores/rest/loginNew/login", json=[username,password,"null","Windows 10","PC","Firefox 112.0","0","","n"], headers={"dispositivo": "desktop", "AppVersion": "v2"}, cookies={"COOKIE_SUPPORT": "true", "GUEST_LANGUAGE_ID": "es_ES", "leyAnticookies": "true"})
# We iterate over the contract list
r = s.get("https://www.i-de.es/consumidores/rest/cto/listaCtos/", params={"_": str(int(time.time() * 1000))}, headers={"dispositivo": "desktop", "AppVersion": "v2"}) 

# Get yesterday's date
yesterday = (datetime.today() - timedelta(days=1)).strftime('%d-%m-%Y')

# Get a write API instance for the bucket
write_api = client.write_api(write_options=SYNCHRONOUS)
write_api = client.write_api(bucket=bucket_name)

for contrato in r.json()["contratos"]:
    codContrato = contrato["codContrato"]
    cups = contrato['cups']
    s.get("https://www.i-de.es/consumidores/rest/cto/seleccion/" + codContrato, headers={"dispositivo": "desktop", "AppVersion": "v2"}, cookies={"COOKIE_SUPPORT": "true", "GUEST_LANGUAGE_ID": "es_ES", "leyAnticookies": "true"})

    r = s.get('https://www.i-de.es/consumidores/rest/consumoNew/obtenerLimiteFechasConsumo', headers={"dispositivo": "desktop", "AppVersion": "v2"}, cookies={"COOKIE_SUPPORT": "true", "GUEST_LANGUAGE_ID": "es_ES", "leyAnticookies": "true"})
    # Example response: {'diferenciaMinima': '2004', 'rangoMaximo': '12', 'fechaMaxima': '27-06-202300:00:00', 'diferenciaMaxima': '1', 'fechaMinima': '01-01-201800:00:00'}
    fechaMínima = datetime.strptime(r.json()['fechaMinima'], '%d-%m-%Y%H:%M:%S')
    fechaMáxima = datetime.strptime(r.json()['fechaMaxima'], '%d-%m-%Y%H:%M:%S')
    if contrato["tipUsoEnergiaCorto"] != "-": # Autoconsumo: obtenerDatosProduccionDH
        # First, we check if yesterday total wasn't 0 to see if there is energy generation in this contract:
        date_str = fechaMáxima.strftime('%d-%m-%Y')
        r = s.get("https://www.i-de.es/consumidores/rest/consumoNew/obtenerDatosProduccionDH/{}/{}/horas/".format(date_str, date_str), headers={"dispositivo": "desktop", "AppVersion": "v2"}, cookies={"COOKIE_SUPPORT": "true", "GUEST_LANGUAGE_ID": "es_ES", "leyAnticookies": "true"})
        if r.json()[0]['total'] == 0.0:
            print('Skipping obtenerDatosProduccionDH on cups', cups, "as yesterday's total energy generation was 0")
        else:
            # We check in influxdb for the last date we have data from and we request from the following day until fechaMáxima (aka yesterday)
            query_api = client.query_api()
            query = (f'from(bucket:"{bucket_name}")'
                      ' |> range(start: 0, stop: now())'
                     f' |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")'
                     f' |> filter(fn: (r) => r["cups"] == "{cups}")'
                     f' |> filter(fn: (r) => r["frequency"] == "day")'
                      ' |> sort(columns: ["_time"])'
                      ' |> last()')
            result = query_api.query(query)

            if result and result[0].records:
                dateToCheck = result[0].records[0]['_time']
                dateToCheck = dateToCheck.astimezone(timezone.utc).replace(tzinfo=None)
                print("Found previous values in filter " + measurement_name + " for " + cups + ". Using timer after previous last value: " + str(dateToCheck))
            else:
                dateToCheck = fechaMínima
                print("There are no previous " + measurement_name + " values for " + cups + ". Using fechaMínima given by the ide: " + str(dateToCheck))
            while dateToCheck <= fechaMáxima:
                date_str = dateToCheck.strftime('%d-%m-%Y')
                r = s.get("https://www.i-de.es/consumidores/rest/consumoNew/obtenerDatosProduccionDH/{}/{}/horas/".format(date_str, date_str), headers={"dispositivo": "desktop", "AppVersion": "v2"}, cookies={"COOKIE_SUPPORT": "true", "GUEST_LANGUAGE_ID": "es_ES", "leyAnticookies": "true"})

                if r.status_code != 200:
                    print("Request failed with status code " + str(int(r.status_code)) + ". Date we tried: " + date_str + ". Retrying in 5 seconds...")
                    time.sleep(5)  # Wait for 5 seconds before retrying
                    continue

                print(r.json())

                # Increment the date by one day
                dateToCheck += timedelta(days=1)
                time.sleep(1) # To avoid hugging too hard

                for entry in r.json():
                    try:
                        # Save one daily value
                        timestamp = datetime.strptime(entry['fechaDesde'], '%d-%m-%Y')
                        # Create a new point for the entry
                        point = Point(measurement_name) \
                            .tag("cups", entry['cups']) \
                            .tag("apiUrl", 'obtenerDatosProduccionDH') \
                            .tag("frequency", 'day')

                        point.time(int(timestamp.timestamp()), WritePrecision.S)

                        exclude_keys = [
                            "valoresPeriodosTarifarios",
                            "totalesPeriodosTarifarios",
                            "fechaDesde",
                            "fechaHasta",
                            "periodos",
                            "valores",
                            "cups"
                        ]

                        for key, value in entry.items():
                            if key == 'totalesPeriodosTarifarios':
                                for i, n in enumerate(entry[key]):
                                    # print(entry['periodos'][i], n)
                                    point.field(entry['periodos'][i], n)
                            elif key not in exclude_keys:
                                point.field(key, value)

                        # Write the point to the bucket
                        print("Saving daily data  | CUPS:", entry['cups'], "Fecha:", timestamp, 'maximo', entry['maximo'], 'posicionMaximo', entry['posicionMaximo'], 'periodoTarifarioMaximo', entry['periodoTarifarioMaximo'], 'total', entry['total']) 
                        write_api.write(bucket=bucket_name, record=point)

                        # Save one value for each hour: timepoint, value, periodo (punta, llano, valle)
                        for i, n in enumerate(entry['valoresPeriodosTarifarios']):
                            timestamp = datetime.strptime(entry['fechaDesde'], '%d-%m-%Y') + timedelta(hours=i+1)
                            for j, m in enumerate(n):
                                if m != None:
                                    periodo = entry['periodos'][j]
                                    break

                            # timestamp = datetime.strptime(entry['fechaDesde'], '%d-%m-%Y')
                            # Create a new point for the entry
                            point = Point(measurement_name) \
                                .tag("cups", entry['cups']) \
                                .tag("apiUrl", 'obtenerDatosProduccionDH') \
                                .tag("frequency", 'hour')

                            point.time(int(timestamp.timestamp()), WritePrecision.S)

                            exclude_keys = [
                                "valoresPeriodosTarifarios",
                                "totalesPeriodosTarifarios",
                                "fechaDesde",
                                "fechaHasta",
                                "periodos",
                                "valores",
                                "cups"
                            ]

                            for key, value in entry.items():
                                if key == 'totalesPeriodosTarifarios':
                                    for i, n in enumerate(entry[key]):
                                        point.field(entry['periodos'][i], n)
                                elif key not in exclude_keys:
                                    point.field(key, value)

                            point.field("periodos", periodo)

                            # Write the point to the bucket
                            print("Saving hourly data | CUPS:", entry['cups'], "Fecha:", timestamp, m, periodo)
                            write_api.write(bucket=bucket_name, record=point)
                    except:
                        print(r.status_code)
                        print(r.json())

    # else:
        # print("Contrato", codContrato, "no es de autoconsumo")

write_api.__del__()
exit()
