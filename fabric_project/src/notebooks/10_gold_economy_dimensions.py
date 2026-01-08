from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. dim_economy creation
print("building dim_economy...")
# reusing silver tables
df_fx = spark.table("silver_fx_rates")
df_gdp = spark.table("silver_gdp")

# simplified join logic, skipping the complex date fill for now
# mainly just want the fx rates joined to daily dates
df_dim = df_fx.select(
    F.col("date"), 
    F.col("exchange_rate_usd_eur").alias("fx_rate")
).distinct()

# taking only US gdp for simplicity
df_gdp_us = df_gdp.filter(F.col("country") == "United States").select(
    F.col("year"), 
    F.col("gdp_usd")
)

# join isn't perfect but works for year-level granularity
df_dim = df_dim.withColumn("year", F.year("date"))
df_dim = df_dim.join(df_gdp_us, "year", "left")

df_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_economy")

# 2. update fact table with eur
print("updating fact_taxi_daily with eur...")
df_fact = spark.table("fact_taxi_daily")
df_dim_eco = spark.table("dim_economy")

# joining back to get rates
df_fact_updated = df_fact.join(df_dim_eco.select("date", "fx_rate"), "date", "left") \
    .withColumn("total_fare_eur", F.round(F.col("total_fare_amount") * F.col("fx_rate"), 2))

df_fact_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_taxi_daily")

print("done economy stuff")

