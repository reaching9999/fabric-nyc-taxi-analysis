import requests
import os
from datetime import datetime

# config
# taxi data is usually 2 months behind so check ranges
START_DATE = "2024-01-01"
END_DATE = "2024-05-01"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
# Fabric default path
LAKEHOUSE_ROOT = "/lakehouse/default"
BRONZE_DIR = f"{LAKEHOUSE_ROOT}/Files/Bronze/Taxi/Raw"

def get_months(start_str, end_str):
    """get list of year-month tuples"""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    res = []
    
    curr = start
    while curr <= end:
        res.append((curr.strftime("%Y"), curr.strftime("%m")))
        # increment logic
        if curr.month == 12:
            curr = curr.replace(year=curr.year + 1, month=1)
        else:
            curr = curr.replace(month=curr.month + 1)
    return res

def download_file(url, path):
    print(f"dl {url}...")
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        
        # writing stream to file
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        # print(f"saved to {path}")
        return True
    except Exception as e:
        print(f"err: {e}")
        return False

# ensure folder exists
try:
    mssparkutils.fs.mkdirs(BRONZE_DIR)
except:
    os.makedirs(BRONZE_DIR, exist_ok=True)

for year, month in get_months(START_DATE, END_DATE):
    fname = f"yellow_tripdata_{year}-{month}.parquet"
    fpath = f"{BRONZE_DIR}/{fname}"
    
    # skip if we already have it
    if os.path.exists(fpath):
        print(f"skipping {fname}, exists")
        continue
        
    url = BASE_URL.format(year=year, month=month)
    download_file(url, fpath)

print("done ingestion")
