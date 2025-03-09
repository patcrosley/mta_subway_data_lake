# MTA Subway Data Lake
#### Implementation of an example data lake with NYC public transport data using Azure Databricks

## Introduction

This project builds a data lake for NYC Subway data using Databricks and a multi-layered architecture (Bronze, Silver, Gold). It leverages multiple data engineering tools to process, clean, and transform raw transit data into meaningful insights.

The pipeline follows a medallion architecture:

- **Bronze Layer**: Stores raw, unprocessed data.
- **Silver Layer**: Cleans and transforms data for structured querying.
- **Gold Layer**: Optimized for analytics and reporting.

This project integrates Git version control for managing Databricks notebooks and ensuring good practices of collaborative development.

### Technologies used

- **Azure Databricks** (Cloud-based big data processing)
- **Azure Data Lake Storage Gen2** (data storage)
- **Apache Spark** (Distributed data processing engine)
- **Python and PySpark** (Python API for Spark)
- **Spark SQL** (Sparse usage for data lake objects management)
- **Delta Lake** (ACID-compliant data lake storage)
- **Git and Github** (Version control for Databricks notebooks)

## Data Architecture

This project follows the **Medallion Architecture**, structuring data into three layers: **Bronze** (raw ingestion with minimalistic transformations), **Silver** (cleansed data with dimensional modelling), and **Gold** (analytics-ready aggregated data for reporting and other consumption). Each layer progressively refines the data for better usability and performance.

### Bronze Layer
- **Ingestion from ADLS Gen2**: Supports multiple formats (CSV, JSON, XML).
- **Minimal transformations**:
  - **CSV**: Applied schema definitions and column aliasing for consistency:
  - **JSON & XML**: Flattened and exploded to align structure with CSV for better queryability in Spark SQL.
- **Metadata fields**: Added Ingestion Date and Source to track data lineage.
- **No dimensional modelling**: Data remains in its original structure, without Star/Snowflake schema compliance.
- **Schema enforcement** to prevent unwanted changes.
- **Storage in Delta format** (external tables, mta_bronze schema).

### Silver Layer
- **Addition of MD5 surrogate keys** to ensure consistent and unique identification of records.
- **Removal of junk and unnecessary fields** to streamline the dataset and improve efficiency.
- **Application of schema modelling**: Three types of objects were created:
  - **Fact tables** for transactional data
  - **Dimension tables** for descriptive data
  - **Lookup tables** for dictionaries, translating dirty values into clean ones or providing additional metadata
- Application of **multiple data transformations** to further refine and standardize the data.
- Data stored in Delta **managed tables** within the mta_silver schema, providing full control over the dataset.

### Gold Layer
- **Creation of report tables**: Refined datasets are designed to provide the most valuable data for analysis.
- **Application of several data transformations**, e.g.:
  - **Aggregations** and **groupby** operations to summarize data.
  - **Window functions** for calculating additional metrics.
- **Data stored in Delta managed tables** within the mta_gold schema, ensuring full control and consistency.
- **Consumption-ready data**, optimized for fast querying and reporting by business users.

## Project Stucture
- 📂 bronze/ → Raw data ingestion from sources like CSV, JSON, XML
- 📂 silver/ → Data cleaning, deduplication, transformations, dimensional modelling
- 📂 gold/ → Aggregated, analytics-ready tables
- 📂 utils/ → Reusable PySpark functions and helper scripts
- 📂 raw_data/ → a copy of raw data stored in ADLS Gen2
