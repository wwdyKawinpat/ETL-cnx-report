import pandas as pd
import datetime
from datetime import datetime as dt, timedelta
import pytz
from bs4 import BeautifulSoup
import psycopg2
import urllib.request, json

import psycopg2 
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import requests

def to_celsius(temp):
    temp = temp-273
    return temp

def open_weather_extract():
    open_weather_url = 'https://api.openweathermap.org/data/2.5/weather?q=Chiang Mai&appid={API_KEY}'
    open_weather_data = requests.get(open_weather_url)
    open_weather_data_json = open_weather_data.json()
    return open_weather_data_json


#pytz.all_timezones : List all timezone.
#Convert UTC to Asia/Bangkok timezone.
def convert_utc_time():
    open_weather_data_json = open_weather_extract()
    
    local_timezone = pytz.timezone('Asia/Bangkok')
    sunrise_datetime_utc = dt.strptime(str(datetime.datetime.fromtimestamp(open_weather_data_json['sys']['sunrise'])),'%Y-%m-%d %H:%M:%S')
    sunrise_datetime = sunrise_datetime_utc.astimezone(local_timezone)
    sunrise_time = sunrise_datetime.strftime("%H:%M:%S")
    sunrise_date = sunrise_datetime.strftime("%Y-%m-%d")

    sunset_datetime_utc = dt.strptime(str(datetime.datetime.fromtimestamp(open_weather_data_json['sys']['sunset'])),'%Y-%m-%d %H:%M:%S')
    sunset_datetime = sunset_datetime_utc.astimezone(local_timezone)
    sunset_time = sunset_datetime.strftime("%H:%M:%S")
    sunset_date = sunset_datetime.strftime("%Y-%m-%d")
    return sunrise_time, sunrise_date, sunset_time, sunset_date



def extract_cnx_pop():
    pop_url = 'https://populationstat.com/thailand/chiang-mai'
    pop_data = requests.get(pop_url)
    soup = BeautifulSoup(pop_data.content, 'html.parser')

    population = soup.find("div",{"class":"main-clock clock"}).text.replace(",","")
    population = int(population)
    return population


def extract_aqi():
    AQI_TOKEN = '{API_TOKEN}'
    air_quality_url = f'https://api.waqi.info/feed/chiangmai/?token={AQI_TOKEN}'
    air_quality = requests.get(air_quality_url)
    air_quality_json = air_quality.json()
    aqi = air_quality_json['data']['aqi']
    return aqi


def to_dataframe():
    aqi = extract_aqi()
    sunrise_time, sunrise_date, sunset_time, sunset_date = convert_utc_time()
    population = extract_cnx_pop()
    open_weather_data_json = open_weather_extract()
    timestamp = dt.now().astimezone(pytz.timezone('Asia/Bangkok'))
    
    data = {
    'timestamp' : timestamp,
    'name' : open_weather_data_json['name'],
    'weather_main' : open_weather_data_json['weather'][0]['main'],
    'sunrise_date' : sunrise_date,
    'sunrise_time' : sunrise_time,
    'sunset_date' : sunset_date,
    'sunset_time' : sunset_time,
    'temp' : to_celsius(open_weather_data_json['main']['temp']),
    'temp_feellike' : to_celsius(open_weather_data_json['main']['feels_like']),
    'temp_max' : to_celsius(open_weather_data_json['main']['temp_max']),
    'temp_min' : to_celsius(open_weather_data_json['main']['temp_min']),
    'aqi' : aqi,
    'population' : population
    }
    
    data = [data]
    df = pd.DataFrame(data)
    return df


def load_to_db():
    conn_string = 'postgresql://airflow:airflow@host.docker.internal:5432/report'
    db = create_engine(conn_string) 
    conn = db.connect() 
    df = to_dataframe()
    df.to_sql('cnx_report', con=conn, if_exists='append', 
              index=False) 
    conn = psycopg2.connect(conn_string) 
    conn.autocommit = True


default_args = {
    'owner': 'kkawin',
    'start_date': dt(2023, 10, 13),
    'email': ['kawinpat.kit@gmail.com']
}

with DAG('cnx-weather-pipeline',
        schedule_interval = '@daily',
        default_args = default_args,
        description = 'Chiangmai weather report',
        catchup = False ) as dag:
    
    t1 = PythonOperator(
        task_id = 'get_weather',
        python_callable=open_weather_extract
    )
    
    t2 = PythonOperator(
        task_id = 'convert_utc',
        python_callable=convert_utc_time
    )
    
    t3 = PythonOperator(
        task_id = 'get_population',
        python_callable=extract_cnx_pop
    )
    
    t4 = PythonOperator(
        task_id = 'get_aqi',
        python_callable=extract_aqi
    )
    
    t5 = PythonOperator(
        task_id='create_dataframe',
        python_callable=to_dataframe
    )
    
    t6 = PythonOperator(
        task_id = 'load_to_postgres',
        python_callable=load_to_db
    )
                
    t1 >> t2 >> t5
    [t3,t4] >> t5
    t5 >> t6