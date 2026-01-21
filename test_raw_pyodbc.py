import pyodbc
import pandas as pd
import os

# --- CONFIGURATION ---
FABRIC_SERVER = "bauwbbk34g5ehekihc6hooubny-ffwut4ps5ntudmoo5qrcb6xwwq.datawarehouse.fabric.microsoft.com"
DATABASE_NAME = "lh_main"
CSV_PATH = os.path.join("fabric_project", "integrations", "data_samples", "taxi_gold_sample.csv")

# Raw connection string
conn_str = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={FABRIC_SERVER},1433;"
    f"Database={DATABASE_NAME};"
    f"Authentication=ActiveDirectoryInteractive;"
    f"Encrypt=yes;"
    f"TrustServerCertificate=yes;"
    f"Connection Timeout=120;"
)

print("--- FABRIC DATA LOAD TEST ---")

try:
    # 1. Read CSV
    print(f"Reading CSV from: {CSV_PATH}")
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Could not find file: {CSV_PATH}")
    
    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df)} rows from CSV.")

    # 2. Connect
    print(f"Connecting to {FABRIC_SERVER}...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected.")

    # 3. Create Table (Drop if exists)
    table_name = "dbo.fact_taxi_daily_sample"
    print(f"Recreating table {table_name}...")
    
    # Check if table exists and drop
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        
    # Create table
    create_query = f"""
    CREATE TABLE {table_name} (
        date DATE,
        location_id INT,
        total_trips INT,
        total_fare_amount DECIMAL(10, 2),
        total_distance DECIMAL(10, 2),
        total_passengers INT
    )
    """
    cursor.execute(create_query)
    conn.commit()
    print("Table created.")

    # 4. Insert Data
    print("Inserting rows...")
    insert_query = f"""
    INSERT INTO {table_name} (date, location_id, total_trips, total_fare_amount, total_distance, total_passengers)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    
    # Convert dataframe to list of tuples for insertion
    for index, row in df.iterrows():
        cursor.execute(insert_query, 
                       row['date'], 
                       int(row['location_id']), 
                       int(row['total_trips']), 
                       float(row['total_fare_amount']), 
                       float(row['total_distance']), 
                       int(row['total_passengers']))
    
    conn.commit()
    print("Data inserted.")

    # 5. Verify
    print("Verifying data...")
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    
    print(f"\n✅ SUCCESS! Table {table_name} contains {len(rows)} rows:")
    for row in rows:
        print(row)
        
    conn.close()

except Exception as e:
    print("\n❌ FAILED:")
    print(str(e))
