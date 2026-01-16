# Data Dictionary / Schema

## Gold Layer (star schema)

### Fact Tables
**fact_taxi_daily**
- `date`: trip date (PK)
- `location_id`: pickup zone id (FK to dim_taxi_zones)
- `total_trips`: count of rides
- `total_fare_amount`: revenue in USD
- `total_fare_eur`: revenue in EUR (calc via dim_fx)

**fact_airquality_daily**
- `date`: measurement date
- `avg_pm2_5`: prticulate matter < 2.5um
- `avg_no2`: no2 level

### Dimension Tables
**dim_taxi_zones**
- `LocationID`: unique id
- `Borough`: nyc borough name
- `Zone`: neigborhood name

**dim_fx**
- `date`: date
- `fx_rate`: usd to eur rates

**dim_gdp**
- `year`: year
- `gdp_usd`: us gdp val
