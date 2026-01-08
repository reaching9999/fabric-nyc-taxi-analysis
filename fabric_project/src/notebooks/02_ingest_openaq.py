import requests, json, os
from datetime import datetime

# settings for air quality
# fetching nyc
LAT, LON = 40.7128, -74.0060
START, END = "2024-01-01", "2024-05-01"
BRONZE_DIR = "/lakehouse/default/Files/Bronze/AirQuality/Raw"

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

def get_aq_data(lat, lon, start_date, end_date):
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "hourly": "pm10,pm2_5,nitrogen_dioxide,ozone",
        "timezone": "America/New_York"
    }
    print(f"calling api for {lat},{lon}")
    
    try:
        r = requests.get(API_URL, params=params)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"api failed: {e}")
        return None

# main
try:
    mssparkutils.fs.mkdirs(BRONZE_DIR)
except:
    os.makedirs(BRONZE_DIR, exist_ok=True)

data = get_aq_data(LAT, LON, START, END)

if data:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"airquality_nyc_{ts}.json"
    path = f"{BRONZE_DIR}/{fname}"
    
    # dump to json
    with open(path, 'w') as f:
        json.dump(data, f)
        
    print(f"saved {len(data.get('hourly', {}).get('time', []))} records to {fname}")
else:
    print("got no data back")
