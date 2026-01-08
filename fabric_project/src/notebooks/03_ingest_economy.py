import requests
import json
import os

LAKEHOUSE = "/lakehouse/default"
BRONZE_ECO = f"{LAKEHOUSE}/Files/Bronze/Economy/Raw"

# endpoints
URL_GDP = "https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&per_page=100"
URL_FX = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"

def get_gdp():
    print("getting world bank gdp...")
    try:
        r = requests.get(URL_GDP)
        r.raise_for_status()
        d = r.json()
        
        # wb returns [metadata, data]
        if len(d) > 1:
            local_path = f"{BRONZE_ECO}/worldbank_gdp_usa.json"
            with open(local_path, 'w') as f:
                json.dump(d[1], f)
            print("gdp saved")
        else:
            print("weird response from wb")
    except Exception as e:
        print(f"gdp error: {e}")

def get_fx():
    print("getting ecb fx rates...")
    try:
        r = requests.get(URL_FX)
        r.raise_for_status()
        
        # its csv text
        local_path = f"{BRONZE_ECO}/ecb_fx_usd_eur.csv"
        with open(local_path, 'w') as f:
            f.write(r.text)
        print("fx saved")
    except Exception as e:
        print(f"fx error: {e}")

# entry point
try:
    mssparkutils.fs.mkdirs(BRONZE_ECO)
except:
    os.makedirs(BRONZE_ECO, exist_ok=True)

get_gdp()
get_fx()
print("done")
