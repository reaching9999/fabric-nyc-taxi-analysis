# NYC Taxi Data Platform (Microsoft Fabric)

## Overview
End-to-end data engineering project built on Microsoft Fabric.  
Ingests NYC Taxi trip data, joins it with weather (OpenAQ) and economic indicators (WB/ECB) to analyze ride demand vs external factors.

## Architecture
**Medallion Architecture (Lakehouse)**
1. **Bronze**: Raw ingestion (Parquet/JSON)
2. **Silver**: Cleaned Delta Tables, deduplicated, type-cast
3. **Gold**: Star Schema (Facts/Dims) ready for Power BI

## Stack
- **Platform**: Microsoft Fabric
- **Compute**: Spark (PySpark)
- **Storage**: OneLake (Delta Lake)
- **Orchestration**: Notebook flows

## Data Sources
- **NYC TLC**: Trip records (Parquet)
- **OpenAQ**: Air quality history (API)
- **WorldBank/ECB**: GDP and FX rates (CSV/API)

## Setup
1. Import notebooks from `src/notebooks/` into a Fabric Workspace.
2. Attach a Lakehouse named `lh_main`.
3. Run notebooks `01` through `10` sequentially.
