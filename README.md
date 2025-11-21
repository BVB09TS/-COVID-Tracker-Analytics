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
**AI / Machine Learning Usage**
  * Apply AI models (e.g., time-series forecasting, regression) to Google Trends data.
  * Predict emerging COVID-19 search trends and potential hotspots.

### 7. Governance & Reliability

* **Purpose**: monitoring, auditing, and pipeline robustness
* **Technologies**: HDFS, JSON, PySpark caching
* **Techniques**:

  * Batch checkpointing
  * Store batch metadata for traceability
  * Separate data and metadata for audit purposes
  * Spark caching to optimize reads and processing



*
# COVID Tracker Analytics – Interpretation of Results

This section provides an interpretation of the results produced by the current `-COVID-Tracker-Analytics` project, focusing on state-level COVID search trends and insights.

---

## Interpretation of Results

### 1. **State-Level Trends**

* The analysis breaks down COVID-related search interest by state.
* **Key insights you can extract:**

  * States with the **highest search volume** likely reflect higher public concern or awareness about COVID in that region.
  * States with **low search volumes** may indicate lower concern, fewer cases, or underreporting.
  * Comparing states over time allows identification of **hotspots** and **shifts in attention** (e.g., a sudden spike in a particular state could signal a surge in cases or media coverage).

### 2. **Search Trend Patterns**

* Trends over time show **when people were most concerned** about COVID-related topics.
* **Interpretation of spikes and dips:**

  * Spikes in search trends correspond to **outbreaks, policy announcements, or media coverage**.
  * Declines in search interest may reflect **pandemic fatigue**, stabilization of cases, or shifting public focus.
* Trends can be **compared across states** to see which regions experienced concern earlier or later than others.

### 3. **Demographic Correlation**

* If combined with Census ACS data:

  * States with a **higher elderly population** might show increased search interest for health-related COVID topics.
  * Correlation with **income, poverty, or transit usage** can reveal socio-economic factors influencing public concern.

### 4. **Actionable Insights**

* **Public health monitoring:** States with sudden search spikes may require more public health messaging or intervention.
* **Policy decisions:** Identify regions where awareness is low and targeted campaigns might be needed.
* **Trend forecasting:** Early spikes in search trends can predict emerging hotspots before official case data catches up.

---

* **California:** High search volume for mask mandates and testing → indicates high public concern and active monitoring.
* **Texas:** Moderate search volume but rising trends → potential early warning for increasing cases.
* **Wyoming:** Low search volume → may reflect smaller population or less concern.

--