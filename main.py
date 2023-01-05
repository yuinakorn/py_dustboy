import pymysql as pymysql
from fastapi import FastAPI, Request, status, HTTPException, Depends, Body, Form
from dotenv import dotenv_values
from datetime import datetime, timezone, timedelta
import os
import requests

config_env = {
    **dotenv_values(".env"),
    **os.environ,
}

app = FastAPI()

connection = pymysql.connect(host=config_env['HOST'],
                             user=config_env['USER_DB'],
                             password=config_env['PASSWORD_DB'],
                             db=config_env['DB_NAME'],
                             port=int(config_env["PORT"]),
                             charset='utf8')


# cursor = connection.cursor()

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/station/")
async def create_station(request_key: str = Form()):
    if request_key == config_env['SECRET_KEY']:
        try:
            with connection.cursor() as cursor:
                url = "https://www.cmuccdc.org/api/ccdc/stations"

                payload = {}
                headers = {}

                response = requests.request("GET", url, headers=headers, data=payload)

                data = response.json()

                for doc in data:
                    sql = "REPLACE INTO dustboy_station (dustboy_id,dustboy_uri,dustboy_alias,dustboy_name_th," \
                          "dustboy_name_en,dustboy_lat,dustboy_lng,dustboy_status,dustboy_pv,dustboy_version," \
                          "db_co,db_mobile,db_addr,db_status,db_model) " \
                          "VALUES " \
                          "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    val = (doc['dustboy_id'], doc['dustboy_uri'], doc['dustboy_alias'], doc['dustboy_name_th'],
                           doc['dustboy_name_en'], doc['dustboy_lat'], doc['dustboy_lng'], doc['dustboy_status'],
                           doc['dustboy_pv'], doc['dustboy_version'], doc['db_co'], doc['db_mobile'], doc['db_addr'],
                           doc['db_status'], doc['db_model'])

                    cursor.execute(sql, val)

                    connection.commit()

            return {"message": "Station created"}

        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

        finally:
            connection.close()
    else:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


@app.post("/value/all/")
async def create_value_all(request_key: str = Form()):
    if request_key == config_env['SECRET_KEY']:
        print(request_key)
        try:
            url = "https://www.cmuccdc.org/api/ccdc/stations"
            payload = {}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)

            data = response.json()

            for doc in data:
                print(doc['dustboy_id'])
                url = f"https://www.cmuccdc.org/api/ccdc/value/{doc['dustboy_id']}"
                print(url)
                payload = {}
                headers = {}
                response = requests.request("GET", url, headers=headers, data=payload)
                data = response.json()
                print(data)
                if data == "[]" or data == []:
                    print("No data")
                    continue

                print(data['dustboy_id'])

                daily_humid = data['daily_humid'] if data['daily_humid'] is not None else 0
                daily_temp = data['daily_temp'] if data['daily_temp'] is not None else 0
                province_code = data['province_code'] if data['province_code'] is not None else 0

                with connection.cursor() as cursor:
                    sql = "REPLACE INTO dustboy_value (id,dustboy_id,log_datetime,pm10,pm25,daily_temp,daily_humid,province_code)" \
                          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

                    val = (data['id'], data['dustboy_id'], data['log_datetime'], data['pm10'], data['pm25'], daily_temp,
                           daily_humid, province_code)

                    print(sql)
                    cursor.execute(sql, val)
                    connection.commit()

            return {"message": "Value created"}

        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    else:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


@app.post("/value/r1/")
async def create_value_r1(request_key: str = Form()):
    sql = "SELECT dustboy_station.dustboy_id FROM dustboy_station " \
          "INNER JOIN dustboy_value ON dustboy_value.id = dustboy_station.dustboy_id " \
          "WHERE dustboy_value.province_code IN ('50','51','52','54','55','56','57','58')"

    if request_key == config_env['SECRET_KEY']:
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
                # convert to list
                result = [list(i) for i in result]

                for doc in result:
                    # covert to string
                    doc = ''.join(doc)
                    print(doc)

                    url = f"https://www.cmuccdc.org/api/ccdc/value/{doc}"
                    print(url)
                    payload = {}
                    headers = {}
                    response = requests.request("GET", url, headers=headers, data=payload)
                    data = response.json()
                    if data == "[]" or data == []:
                        print("No data")
                        continue

                    daily_humid = data['daily_humid'] if data['daily_humid'] is not None else 0
                    daily_temp = data['daily_temp'] if data['daily_temp'] is not None else 0
                    province_code = data['province_code'] if data['province_code'] is not None else 0

                    sql = "REPLACE INTO dustboy_value (id,dustboy_id,log_datetime,pm10,pm25,daily_temp,daily_humid,province_code)" \
                          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

                    val = (data['id'], data['dustboy_id'], data['log_datetime'], data['pm10'], data['pm25'], daily_temp,
                           daily_humid, province_code)

                    cursor.execute(sql, val)
                    connection.commit()

        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

        finally:
            connection.close()

    return {"message": "Value R1 created"}

