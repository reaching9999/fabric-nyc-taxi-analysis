import dropbox
import json
import datetime
import os
import pandas as pd

# config
DROPBOX_ACCESS_TOKEN = "sl.u.AGMhPJ0t1lA5WLMOxz0qAau-iL9JjA7pgUTnvsf60ttobi6iQbsOzlhrxNMxSmZoGzvTrO5gbKTIbgoTQFEbghjqOtFhODKdh9MbMfe68X5nrVlgpScRAfHFS1jnCfhhhTQSFFt8ecoAbAt2kGlwz5rpFUj4ihpahnKATuNHZw67SxD7yslc_6okVgN0AKBDkw7J9qvms01Vshp0IdgCXL4CvN2FIIpdWa-v4mPGb2sjmR4of06DMTOhxKXeCghKp_4ED8OnhB44NomFupTQ7lQNX491S-HP_B6jk1Nkq0dIOQq3XuWafPULPR7v0pc2cWgA2PdZOoTH58UKcHhkXTWWhOqRkalT4OI6RvCe92nRShWAGa2eMxTR6xPrzCdGN-goMR-6_M_BqK44vqDMndgS46NIf5VAQIhXPVDvd-DKTEa2Klyvf1lIvRG5cQTAFQmKYMA3mo0ydthSRPHnn1je9_sZdZtPKxpzhgK-o0ipCH2LheW5m0kk-M6hEIF88rh1SYcuGirGw5ldQypUNybLxT9AKkDGG8Zun2OE5-yBIRqYCiPJAUi1J4Sq0XWY6IN9RkVK7RcxCDjZBww-gSSQAkqOm70TQrP2I-EwSH9DOCTiQQvA8SPsXszFpIWTv7kOgHK8tbL0KuItXgC2pvrBD6HRJ0jR-065eJkTlEcoQXpj1sFBONdb0HxCNzMUF8fDD8CwAYkyrqPo88YF0aERiWxHl-3Z-rsONPTIQPlaPEEWMWtecbr1jVyr-mjAJDoNJOy8O7gAWiwNImSK8BlqMrupERzvQ0QgZPIvnatbiBUqHVqD6gbfwLBla9x6uRZ7kFR1NNKegAdgFd_nmzxMPc2MxUwat1oECTXwT6nU9l-ahD-SngjdTZSeyH7K1l759SZa1zPppUWlvmfgCyA0uwXMfUWm63A6jwgkbx9cje260dC5vl2M17NCLcOf7kdAstVBRYb9Ynle59dFAnE0NdB-IrarRcPu7J9kFXB_fhk72zow7FOVgHpwTXNvPIfBst-TA43kTX-inNCPheBiTvd_cxYu1EHAlne5-w2YYkFuXTLepN0-ovu7VVMxkJ451ZMjBYX5hCXzT0rut492-ESPy2feOodNSjr5EwZctz7IG3Hfv6PnppymaRZLrwQ56xquA7nKz_BW3Js2Mo9gw4FyqOiKWPhaEsb04SSqSaIAa77dLhTzu1xLDnlYYFnqjAv4lvZtkFk_5NwPXiBBIYdsuAe52R2plsWYLjpOndTMbPtBkAUMUIhSyRiBUUlYAhI3LmoF3slJdo6ujyxYHCzDcsGHTO538iascA58Cw"
FILE_NAME_PREFIX = "taxi_daily_summary"

def fetch_local_data():
    print("Loading CSV...")
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data_samples", "taxi_gold_sample.csv")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # agreggate by date
    summary = df.groupby('date').agg({
        'total_trips': 'sum',
        'total_fare_amount': 'sum',
        'total_distance': 'sum',
        'total_passengers': 'sum'
    }).reset_index()
    
    return summary

def generate_report(df):
    today = datetime.date.today().isoformat()
    records = df.to_dict(orient='records')
    
    data = {
        "report_generated_date": today,
        "source": "Taxi Data Analytics Pipeline",
        "period": "Daily Summary",
        "summary_data": records
    }
    return data

def upload_to_dropbox(json_data):
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    
    json_bytes = json.dumps(json_data, indent=2, default=str).encode('utf-8')
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"/{FILE_NAME_PREFIX}_{timestamp}.json"
    
    try:
        dbx.files_upload(json_bytes, filename)
        print(f"Sucess! Uploaded {filename}")
        return filename
    except dropbox.exceptions.ApiError as err:
        print(f"Dropbox eror: {err}")
        return None

if __name__ == "__main__":
    print("--- Dropbox Upload ---")
    try:
        df = fetch_local_data()
        print(f"Loaded {len(df)} rows.")
        print("\nPreveiw:")
        print(df.head())
        
        report = generate_report(df)
        print("\nJSON Genrated.")
        
        upload_to_dropbox(report)
        
    except Exception as e:
        print(f"Faild: {e}")
