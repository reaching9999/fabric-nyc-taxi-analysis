from pyspark.sql import functions as F

# 1. fx rates
print("processing fx...")
try:
    df_fx = spark.read.option("header", True).csv("Files/Bronze/Economy/Raw/ecb_fx_usd_eur.csv")
    df_fx.select(
        F.to_date("TIME_PERIOD").alias("date"),
        F.col("OBS_VALUE").cast("double").alias("exchange_rate_usd_eur")
    ).filter("exchange_rate_usd_eur is not null").write.mode("overwrite").saveAsTable("silver_fx_rates")
except Exception as e:
    print(f"fx skipped: {e}")

# 2. gdp
print("processing gdp...")
try:
    df_gdp = spark.read.option("multiline", True).json("Files/Bronze/Economy/Raw/worldbank_gdp_usa.json")
    df_gdp.select(
        F.col("date").cast("int").alias("year"),
        F.col("value").cast("double").alias("gdp_usd"),
        F.col("country.value").alias("country")
    ).filter("gdp_usd is not null").write.mode("overwrite").saveAsTable("silver_gdp")
except Exception as e:
    print(f"gdp skipped: {e}")

# 3. taxi zones
print("processing zones...")
try:
    df_z = spark.read.option("header", True).csv("Files/Bronze/Taxi/Reference/taxi_zone_lookup.csv")
    df_z.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.trim("Borough").alias("borough"),
        F.trim("Zone").alias("zone"),
        F.trim("service_zone").alias("service_zone")
    ).write.mode("overwrite").saveAsTable("silver_taxi_zones")
except Exception as e:
    print(f"zones skipped: {e}")
