# COVID Tracker Analytics

## Description

`-COVID-Tracker-Analytics` is a data analysis project combining COVID-19 case trends with demographic data.
The goal is to build a **complete data pipeline**, from data ingestion and storage to analysis and interactive dashboards.

The project includes both the **current state** (Python scripts for data analysis) and the **target state** (full data-engineering pipeline with PySpark, HDFS, ETL, analysis, and visualization).

---

## Current Project State

* Reads static data files in the `data/` folder
* Python scripts in `src/` for basic data cleaning and analysis
* Generates exploratory charts and statistics
* **No production-ready ingestion pipeline, structured storage, or interactive dashboards yet**

---

## Target Architecture

### 1. Data Sources

* **Census ACS 2021** → demographic data
* **Google Trends Simulator 2024** → COVID search trends
* **Technologies**: Parquet files on HDFS
* **Techniques**: batch collection simulation, random data generation for trends

### 2. Ingestion Layer

* **Purpose**: retrieve data from sources and store raw files
* **Technologies**: PySpark, HDFS, JSON for checkpointing
* **Techniques**:

  * Batch ingestion with checkpointing for recovery
  * Retry logic for robustness
  * Raw data storage in `/healthcare/raw/`

### 3. Storage Layer

* **Purpose**: structured and partitioned data storage
* **Technologies**: HDFS, Parquet files
* **Techniques**:

  * Organize layers: `raw/`, `clean/`, `metadata/`
  * Partition by year/month or batch
  * Separate data and metadata for auditing

### 4. Processing Layer

* **Purpose**: transform data for analysis
* **Technologies**: PySpark, Spark SQL functions (`col`, `avg`, `sum`, `md5`)
* **Techniques**:

  * Data cleaning (handling nulls)
  * Generate unique IDs (MD5)
  * ETL for:

    * Census → select business-relevant columns + metadata
    * COVID Trends → filter volumes > 0, transform dates, batch metadata
  * Partitioning for performance

### 5. Analysis Layer

* **Purpose**: data exploration and aggregation
* **Technologies**: PySpark, Pandas, Plotly
* **Techniques**:

  * Dynamic filtering (dates, states, search terms)
  * Monthly and state-level aggregations
  * Correlation analysis (% elderly, poverty, income, transit)
  * Identify key trends (top COVID searches, elderly population, etc.)

### 6. Visualization Layer

* **Purpose**: interactive dashboards for insights
* **Technologies**: Streamlit, Plotly
* **Techniques**:

  * Multi-tab dashboards: Overview, Demographics, Trends, Correlations, Data Upload
  * Interactive charts: bar, line, scatter, pie, correlation matrices
  * Dynamic tables and key metrics for quick insights

### 7. Governance & Reliability

* **Purpose**: monitoring, auditing, and pipeline robustness
* **Technologies**: HDFS, JSON, PySpark caching
* **Techniques**:

  * Batch checkpointing
  * Store batch metadata for traceability
  * Separate data and metadata for audit purposes
  * Spark caching to optimize reads and processing

---

## Future Roadmap

1. Implement PySpark ingestion jobs for all data sources
2. Set up HDFS for structured and partitioned storage
3. Build ETL transformations for cleaning, enrichment, and unique IDs
4. Develop analysis scripts for correlations, aggregations, and trend detection
5. Create interactive dashboards in Streamlit with Plotly charts
6. Add governance: checkpointing, metadata tracking, caching, and lineage
