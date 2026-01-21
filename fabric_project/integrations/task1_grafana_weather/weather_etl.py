import requests
import time
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# config
INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = "YOUR_INFLUXDB_TOKEN_HERE"
INFLUX_ORG = "YOUR_ORG_NAME"
INFLUX_BUCKET = "weather_data"
CITY_LAT = 40.7128
CITY_LON = -74.0060

def fetch_weather():
    # gets weather from openmeteo api
    url = f"https://api.open-meteo.com/v1/forecast?latitude={CITY_LAT}&longitude={CITY_LON}&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m&timezone=auto"
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r.json()['current']
    except Exception as e:
        print(f"Error geting weather: {e}")
        return None

def write_to_influx(weather_data):
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    p = Point("weather_reading") \
        .tag("location", "NYC_Manhattan") \
        .field("temperature_c", weather_data['temperature_2m']) \
        .field("humidity_pct", weather_data['relative_humidity_2m']) \
        .field("wind_speed_kmh", weather_data['wind_speed_10m']) \
        .field("precipitation_mm", weather_data['precipitation']) \
        .time(datetime.utcnow())

    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
        print(f"Wrote data: {weather_data['temperature_2m']}C")
    except Exception as e:
        print(f"Influx write eror: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    print("Starting weather ETL...")
    while True:
        current = fetch_weather()
        if current:
            write_to_influx(current)
        time.sleep(60)
