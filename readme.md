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
- **Azure Data Lake Storage Gen2** (Data storage)
- **Azure Data Factory** (Orchestration)
- **Apache Spark** (Distributed data processing engine)
- **Python and PySpark** (Python API for Spark)
- **Spark SQL** (Limited usage for data lake objects management)
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

## Load Strategy

The data loading process follows best practices to ensure efficiency and accuracy across different table types:

**Incremental load** for fact tables:

- New records are appended based on the latest ingestion timestamp or unique keys.
- Ensures efficient processing without duplicating historical data.

**Truncate-load** for dimension and lookup tables:
- Old data is completely replaced with a fresh dataset.
- Guarantees up-to-date reference data, preventing inconsistencies caused by slowly changing dimensions (although SCD type II is very likely to be implemented in the next version of the project).

## Orchestration

Data workflows are orchestrated using **Azure Data Factory** (ADF) to automate and manage data movement across the Medallion architecture. ADF handles:
- **Ingestion**: Fetching raw data from ADLS Gen2 into the Bronze layer.
- **Transformation Execution**: Triggering Databricks notebooks to process data into Silver and Gold layers.
- **Load Management**:
  - Incremental loading for fact tables.
  - Truncate-load for dimension and lookup tables.
- **Scheduling & Monitoring**: Automating pipeline execution and tracking data flow.

ADF ensures **efficient, scalable, and automated** data processing across the entire pipeline.

## Project Stucture
- 📂 bronze/ → Raw data ingestion from sources like CSV, JSON, XML
- 📂 silver/ → Data cleaning, deduplication, transformations, dimensional modelling
- 📂 gold/ → Aggregated, analytics-ready tables
- 📂 utils/ → Reusable PySpark functions and helper scripts
- 📂 raw_data/ → a copy of raw data stored in ADLS Gen2
