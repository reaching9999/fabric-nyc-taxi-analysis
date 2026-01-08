from pyspark.sql import functions as F

print("loading bronze taxi...")
df = spark.read.parquet("Files/Bronze/Taxi/Raw/*.parquet")

# standard cleanup (ts casting, filter bad data)
df_clean = df \
    .withColumn("tpep_pickup_datetime", F.to_timestamp(F.col("tpep_pickup_datetime"))) \
    .withColumn("tpep_dropoff_datetime", F.to_timestamp(F.col("tpep_dropoff_datetime"))) \
    .withColumn("trip_year", F.year("tpep_pickup_datetime")) \
    .withColumn("trip_month", F.month("tpep_pickup_datetime")) \
    .withColumn("ingestion_date", F.current_timestamp()) \
    .filter("fare_amount > 0 and trip_distance >= 0")

df_clean.write.format("delta").mode("overwrite").saveAsTable("silver_taxi_trips")
print("saved silver")
