import dropbox
import json
import datetime
import os
import pandas as pd

# config
DROPBOX_ACCESS_TOKEN = "YOUR_DROPBOX_TOKEN"
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
