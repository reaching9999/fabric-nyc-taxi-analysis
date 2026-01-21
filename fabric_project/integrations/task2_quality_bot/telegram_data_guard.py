import os
import pandas as pd
import great_expectations as gx
import telebot
from sqlalchemy import create_engine

# config
TELEGRAM_TOKEN = "8391793561:AAHbm6BMKjPC5iOQIDNwaA9Vf1NGj5j5c00" 
FABRIC_SERVER = "bauwbbk34g5ehekihc6hooubny-ffwut4ps5ntudmoo5qrcb6xwwq.datawarehouse.fabric.microsoft.com" 
DATABASE_NAME = "lh_main"
MOCK_MODE = True

bot = telebot.TeleBot(TELEGRAM_TOKEN)

def get_fabric_connection_string():
    driver = "ODBC Driver 17 for SQL Server" 
    return f"mssql+pyodbc://{FABRIC_SERVER}/{DATABASE_NAME}?driver={driver}&authentication=ActiveDirectoryInteractive&LoginTimeout=120"

def get_data():
    if MOCK_MODE:
        print("Loading from local CSV...")
        csv_path = os.path.join(os.path.dirname(__file__), "..", "data_samples", "taxi_gold_sample.csv")
        if os.path.exists(csv_path):
            return pd.read_csv(csv_path)
        else:
            print(f"File not found at {csv_path}, createing dummy data.")
            return pd.DataFrame({
                "date": ["2024-01-01"],
                "total_trips": [100],
                "location_id": [132],
                "total_fare_amount": [50.0],
                "total_distance": [10.2],
                "total_passengers": [2]
            })
    else:
        print(f"Conecting to Fabric: {FABRIC_SERVER}...")
        conn_str = get_fabric_connection_string()
        engine = create_engine(conn_str)
        query = "SELECT TOP 1000 * FROM dbo.fact_taxi_daily ORDER BY date DESC"
        return pd.read_sql(query, engine)

def run_quality_checks():
    print("Runing checks...")
    
    try:
        df = get_data()
        
        if df.empty:
            return False, "Warning: No data returned."
            
        print(f"Got {len(df)} rows.")

        # setup great expecations
        ge_df = gx.from_pandas(df)

        validation_results = []
        
        # check trips are positive
        r1 = ge_df.expect_column_values_to_be_between(
            column="total_trips", min_value=1, max_value=1000000
        )
        validation_results.append(r1)
        
        # check location ids exist
        r2 = ge_df.expect_column_values_to_not_be_null(column="location_id")
        validation_results.append(r2)
        
        success = all(res["success"] for res in validation_results)
        success_count = sum(1 for res in validation_results if res["success"])
        total_rules = len(validation_results)
        
        source_name = "MOCK CSV" if MOCK_MODE else f"{DATABASE_NAME} (Live)"
        
        report = f"""
🏭 **Fabric Data Quality Report**
Source: {source_name}
Rows Checked: {len(df)}
Status: {'✅ PASSED' if success else '❌ FAILED'}
Rules Passed: {success_count}/{total_rules}
        """
        return success, report
        
    except Exception as e:
        return False, f"Error: {str(e)}"

@bot.message_handler(commands=['check', 'start'])
def send_welcome(message):
    msg = "Runing checks..." if MOCK_MODE else "Conecting to Fabric..."
    bot.reply_to(message, msg)
    
    success, report = run_quality_checks()
    bot.reply_to(message, report)

if __name__ == "__main__":
    print("Bot running...")
    bot.infinity_polling()
