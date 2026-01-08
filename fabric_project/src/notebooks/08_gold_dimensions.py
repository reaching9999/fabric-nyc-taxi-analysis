from pyspark.sql import functions as F

# 1. date dimension
print("building dim_date...")
# doing 2024 full year
# sequence generates array of dates, explode makes rows
df_dates = spark.sql("SELECT sequence(to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day) as dates") \
    .withColumn("date", F.explode("dates"))

df_dim_date = df_dates.select(
    "date",
    F.year("date").alias("year"),
    F.month("date").alias("month"),
    F.dayofmonth("date").alias("day"),
    F.dayofweek("date").alias("day_of_week"),
    F.date_format("date", "EEEE").alias("day_name"),
    F.quarter("date").alias("quarter"),
    F.when(F.dayofweek("date").isin([1, 7]), True).otherwise(False).alias("is_weekend")
)

df_dim_date.write.format("delta").mode("overwrite").saveAsTable("dim_date")

# 2. taxi zone dimension
print("building dim_taxizone...")
df_z = spark.read.table("silver_taxi_zones")
df_z.select("location_id", "borough", "zone", "service_zone") \
    .write.format("delta").mode("overwrite").saveAsTable("dim_taxizone")
    
print("dims done")
