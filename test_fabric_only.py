import pandas as pd
from sqlalchemy import create_engine
import time

# --- CONFIGURATION (Identical to your other scripts) ---
FABRIC_SERVER = "bauwbbk34g5ehekihc6hooubny-ffwut4ps5ntudmoo5qrcb6xwwq.datawarehouse.fabric.microsoft.com"
DATABASE_NAME = "lh_main"

def get_fabric_connection_string():
    driver = "ODBC Driver 17 for SQL Server"
    # Port 1433 forced + Trust Certs to avoid handshake freeze
    return f"mssql+pyodbc://{FABRIC_SERVER},1433/{DATABASE_NAME}?driver={driver}&authentication=ActiveDirectoryInteractive&LoginTimeout=120&Encrypt=yes&TrustServerCertificate=yes"

def test_connection():
    print("--- TESTING FABRIC CONNECTION (NO TELEGRAM) ---")
    print(f"Target: {FABRIC_SERVER}")
    
    try:
        print("1. Creating Engine...")
        conn_str = get_fabric_connection_string()
        engine = create_engine(conn_str)
        
        print("2. Connecting... (Please check for Browser Popup!)")
        # We run a simple query to force the connection to open
        query = "SELECT TOP 5 * FROM dbo.fact_taxi_daily"
        
        start_time = time.time()
        df = pd.read_sql(query, engine)
        end_time = time.time()
        
        print(f"✅ SUCCESS! Connected in {round(end_time - start_time, 2)} seconds.")
        print(f"Fetched {len(df)} rows.")
        print(df.head())
        
    except Exception as e:
        print("\n❌ FAILURE / TIMEOUT")
        print(str(e))

if __name__ == "__main__":
    test_connection()
