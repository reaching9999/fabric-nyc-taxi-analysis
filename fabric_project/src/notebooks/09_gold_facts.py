from pyspark.sql import functions as F

# 1. fact taxi daily
print("building fact_taxi_daily...")
df_t = spark.read.table("silver_taxi_trips")

# aggregating to day/location granularity
df_fact = df_t.groupBy(
    F.to_date("tpep_pickup_datetime").alias("date"),
    F.col("PULocationID").alias("location_id")
).agg(
    F.count("*").alias("total_trips"),
    F.sum("fare_amount").alias("total_fare_amount"),
    F.sum("trip_distance").alias("total_distance"),
    F.sum("passenger_count").alias("total_passengers"),
    F.avg("fare_amount").alias("avg_fare")
)

df_fact.write.format("delta").mode("overwrite").saveAsTable("fact_taxi_daily")

# 2. fact air quality daily
print("building fact_airquality_daily...")
df_aq = spark.read.table("silver_air_quality")

# daily averages
df_fact_aq = df_aq.groupBy(
    F.to_date("measurement_time").alias("date")
).agg(
    F.round(F.avg("pm10"), 2).alias("avg_pm10"),
    F.round(F.avg("pm2_5"), 2).alias("avg_pm2_5"),
    F.round(F.avg("no2"), 2).alias("avg_no2"),
    F.round(F.avg("o3"), 2).alias("avg_o3")
)

df_fact_aq.write.format("delta").mode("overwrite").saveAsTable("fact_airquality_daily")
print("facts done")
