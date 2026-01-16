from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd

# make sure LH is attached or table() fails!
plt.style.use('ggplot')

# 1. basic mobility
print("plotting mobility...")
df_taxi = spark.table("fact_taxi_daily")
df_zones = spark.table("dim_taxizone")

# daily trips
# filtering to 2024 to skip old junk
pdf_trend = df_taxi.filter("date >= '2024-01-01'").groupBy("date").agg(F.sum("total_trips").alias("trips")).orderBy("date").toPandas()

plt.figure(figsize=(10, 5))
plt.plot(pdf_trend['date'], pdf_trend['trips'], color='blue')
plt.title('Moblity Trend: Daily Taxi Trips')
plt.show()

# busy zones
df_joined = df_taxi.join(df_zones, df_taxi.location_id == df_zones.location_id)
pdf_zones = df_joined.groupBy("zone").agg(F.sum("total_trips").alias("trips")).orderBy(F.desc("trips")).limit(10).toPandas()

plt.figure(figsize=(10, 5))
plt.barh(pdf_zones['zone'], pdf_zones['trips'], color='orange')
plt.title('Top 10 Busiest Pckup Zones')
plt.gca().invert_yaxis()
plt.show()

# 2. air quality
print("plotting air qual...")
df_aq = spark.table("fact_airquality_daily")
pdf_aq = df_aq.orderBy("date").toPandas()

plt.figure(figsize=(10, 5))
plt.plot(pdf_aq['date'], pdf_aq['avg_pm2_5'], label='PM2.5', color='red')
plt.plot(pdf_aq['date'], pdf_aq['avg_no2'], label='NO2', color='green')
plt.title('Air Qualty Trends (NYC)')
plt.legend()
plt.show()

# 3. correlation
print("checking corr...")
df_corr = df_taxi.groupBy("date").agg(F.sum("total_trips").alias("trips")).join(df_aq, "date").orderBy("date")
pdf_corr = df_corr.toPandas()

fig, ax1 = plt.subplots(figsize=(12, 6))
ax1.set_ylabel('Taxi Trips', color='tab:blue')
ax1.plot(pdf_corr['date'], pdf_corr['trips'], color='tab:blue', alpha=0.6)

ax2 = ax1.twinx()
ax2.set_ylabel('PM 2.5', color='tab:red')
ax2.plot(pdf_corr['date'], pdf_corr['avg_pm2_5'], color='tab:red', linestyle='--')
plt.title('Correlation: Taxi vs Air Polution')
plt.show()

# 4. money stuff
print("plotting eco...")
# monthly agg
pdf_eco = df_taxi.filter("date >= '2024-01-01'").withColumn("month", F.trunc("date", "month")) \
    .groupBy("month").agg(F.sum("total_fare_amount").alias("usd"), F.sum("total_fare_eur").alias("eur")).orderBy("month").toPandas()

pdf_eco.plot(x="month", y=["usd", "eur"], kind="bar", figsize=(10, 6))
plt.title("Revnue: USD vs EUR")
plt.show()

# gdp context
try:
    df_gdp = spark.table("dim_gdp")
    pdf_gdp = df_gdp.filter("year >= 2019").orderBy("year").toPandas()
    
    plt.figure(figsize=(8, 4))
    plt.plot(pdf_gdp['year'], pdf_gdp['gdp_usd'], marker='o', color='purple')
    plt.title("Macro context (gdp trend)")
    plt.show()
except:
    pass

# 5. drill downs
print("extra analysis...")

# hourly
df_h = spark.sql("SELECT hour(tpep_pickup_datetime) as h, count(*) as c FROM silver_taxi_trips GROUP BY h ORDER BY h").toPandas()
plt.figure(figsize=(10,4))
plt.plot(df_h['h'], df_h['c'], marker='o')
plt.title("Hourly Dmand")
plt.show()

# dist vs fare scatter
df_sc = spark.sql("SELECT trip_distance, fare_amount FROM silver_taxi_trips WHERE trip_distance < 20 AND fare_amount < 100 LIMIT 1000").toPandas()
plt.figure(figsize=(8,6))
plt.scatter(df_sc['trip_distance'], df_sc['fare_amount'], alpha=0.3)
plt.title("Dist vs Fare")
plt.show()

# weekend pollution
df_wk = spark.sql("SELECT CASE WHEN dayofweek(measurement_time) IN (1,7) THEN 'Wkend' ELSE 'Wkday' END as d, avg(pm2_5) as p FROM silver_air_quality GROUP BY 1").toPandas()
plt.figure(figsize=(6,4))
plt.bar(df_wk['d'], df_wk['p'], color=['teal', 'gray'])
plt.title("Pollution: Wkday vs Wkend")
plt.show()

# 6. advanced
print("rolling avgs & heatmaps...")

# 7d rolling window
w = Window.orderBy("date").rowsBetween(-6, 0)
df_roll = df_taxi.filter("date >= '2024-01-01'").groupBy("date").agg(F.sum("total_trips").alias("dailies")) \
    .withColumn("roll", F.avg("dailies").over(w)).orderBy("date").toPandas()

plt.figure(figsize=(10, 5))
plt.plot(df_roll['date'], df_roll['dailies'], alpha=0.3, color='gray')
plt.plot(df_roll['date'], df_roll['roll'], color='blue', linewidth=2)
plt.title("Taxi Demand (7d rolling)")
plt.show()

# heatmap logic
df_hm = spark.sql("""
    SELECT hour(tpep_pickup_datetime) as h, date_format(tpep_pickup_datetime, 'EEE') as d, dayofweek(tpep_pickup_datetime) as dn, count(*) as c
    FROM silver_taxi_trips
    GROUP BY 1, 2, 3 ORDER BY dn, h
""").toPandas()

plt.figure(figsize=(12, 6))
# hack for bubble size
sizes = (df_hm['c'] - df_hm['c'].min()) / (df_hm['c'].max() - df_hm['c'].min()) * 500
plt.scatter(df_hm['h'], df_hm['d'], s=sizes, alpha=0.6, c=df_hm['c'])
plt.title("Heatmap: hour vs day")
plt.show()

# rev per mile
df_eff = spark.sql("""
    SELECT CASE WHEN trip_distance < 1 THEN '0-1 m' WHEN trip_distance < 3 THEN '1-3 m' WHEN trip_distance < 5 THEN '3-5 m' ELSE '5+ m' END as dist,
    avg(fare_amount / trip_distance) as rpm
    FROM silver_taxi_trips WHERE trip_distance > 0 AND fare_amount > 0 GROUP BY 1 ORDER BY 2 DESC
""").toPandas()

plt.figure(figsize=(8, 5))
plt.bar(df_eff['dist'], df_eff['rpm'], color='forestgreen')
plt.title("Eco Efficiency ($/mile)")
plt.show()

print("viz done")
