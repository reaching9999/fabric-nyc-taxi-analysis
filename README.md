# NYC Taxi & Economic Analysis Platform

## Overview
This project implements a comprehensive data engineering solution on **Microsoft Fabric**, integrating mobility, environmental, and economic datasets to analyze urban transport patterns.

The system ingests raw data from multiple sources, processes it through a **Medallion Architecture** (Bronze/Silver/Gold) using **PySpark**, and serves it for reporting via **Power BI**.

## Architecture
*   **Platform**: Microsoft Fabric (Lakehouse & Notebooks)
*   **Compute**: Spark Runtime 3.5
*   **Storage**: OneLake (Delta Format)
*   **Orchestration**: Fabric Data Pipelines

## Data Sources
1.  **NYC Taxi & Limousine Commission (TLC)**: Trip record data.
2.  **OpenAQ**: Real-time air quality metrics for detailed timestamps.
3.  **World Bank / ECB**: Macroeconomic indicators and FX rates.

## Project Structure
```
fabric_project/
├── src/
│   └── notebooks/       # PySpark transformation logic
│       ├── 01_ingest... # Bronze Ingestion
│       ├── 05_trans...  # Silver Transformation
│       └── 09_gold...   # Gold Modeling
└── docs/                # Architecture diagrams and schema docs
```

## Setup & Deployment
1.  Provision a Microsoft Fabric capacity.
2.  Create a Workspace and a Lakehouse named `lh_main`.
3.  Import the notebooks from `src/notebooks/` into the Fabric workspace.
4.  Execute the notebooks in sequence (01 through 10).
