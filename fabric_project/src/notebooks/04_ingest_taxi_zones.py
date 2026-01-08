import requests
import os

# lookup table for taxi zones
URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
DEST = "/lakehouse/default/Files/Bronze/Taxi/Reference"
FNAME = "taxi_zone_lookup.csv"

try:
    mssparkutils.fs.mkdirs(DEST)
except:
    os.makedirs(DEST, exist_ok=True)

print("downloading zone lookup...")
r = requests.get(URL)
if r.status_code == 200:
    with open(f"{DEST}/{FNAME}", 'wb') as f:
        f.write(r.content)
    print("ok saved")
else:
    print(f"failed {r.status_code}")
