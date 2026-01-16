from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. dim_fx creation
print("building dim_fx...")
df_fx = spark.table("silver_fx_rates")

df_dim_fx = df_fx.select(
    F.col("date"), 
    F.col("exchange_rate_usd_eur").alias("fx_rate")
).distinct()

# added overwriteSchema=true because we significantly changed columns
# from the old 'dim_economy' version that might be lingering
df_dim_fx.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_fx")

# 2. dim_gdp creation
print("building dim_gdp...")
df_gdp = spark.table("silver_gdp")

# filter for US only and select relevant cols
df_dim_gdp = df_gdp.filter(F.col("country") == "United States").select(
    F.col("year"), 
    F.col("gdp_usd")
).distinct()

df_dim_gdp.write.format("delta").mode("overwrite").saveAsTable("dim_gdp")

# 3. update fact_taxi_daily with eur
print("updating fact_taxi_daily with eur...")
df_fact = spark.table("fact_taxi_daily")
# reload fx for join
df_fx_lookup = spark.table("dim_fx")

# joining back to get rates
df_fact_updated = df_fact.join(df_fx_lookup, "date", "left") \
    .withColumn("total_fare_eur", F.round(F.col("total_fare_amount") * F.col("fx_rate"), 2)) \
    .drop("fx_rate")

df_fact_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_taxi_daily")

print("economy dims done")

