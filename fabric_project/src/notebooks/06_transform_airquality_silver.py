from pyspark.sql import functions as F

print("parsing air quality json...")
# open-meteo returns columnar arrays we need to zip and explode
df_raw = spark.read.option("multiline", "true").json("Files/Bronze/AirQuality/Raw/*.json")
df_hourly = df_raw.select("hourly.*")

# zip arrays into rows
df_exp = df_hourly.select(
    F.explode(F.arrays_zip(
        "time", "pm10", "pm2_5", "nitrogen_dioxide", "ozone"
    )).alias("d")
)

df_final = df_exp.select(
    F.to_timestamp("d.time").alias("measurement_time"),
    F.col("d.pm10").cast("double").alias("pm10"),
    F.col("d.pm2_5").cast("double").alias("pm2_5"),
    F.col("d.nitrogen_dioxide").cast("double").alias("no2"),
    F.col("d.ozone").cast("double").alias("o3"),
    F.lit("New York").alias("city"),
    F.current_timestamp().alias("ingestion_date")
)

df_final.write.format("delta").mode("overwrite").saveAsTable("silver_air_quality")
print("silver_air_quality done")
