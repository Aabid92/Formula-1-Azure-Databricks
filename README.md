# Formula 1 Azure Databricks Data Pipeline

## Formula 1 Overview

### An Overview of Formula 1 racing can be learned from https://f1chronicle.com/a-beginners-guide-to-formula-1/

![overview-f1](https://github.com/Aabid92/Formula-1-Azure-Databricks/assets/40827170/422c978b-e503-49cf-b0db-18fd7b25a40a)

## Data Source ( Ergest Developer API ) 
The Ergast Developer API is an experimental web service which provides a historical record of motor racing data for non-commercial purposes.
### Data Schema
![data-shema](https://github.com/Aabid92/Formula-1-Azure-Databricks/assets/40827170/d5e200db-c601-4100-9a67-eba92ab9760a)

## Data Pipeline Modeling

![data_pipeline](https://github.com/Aabid92/Formula-1-Azure-Databricks/assets/40827170/f8c04f08-1f97-4fe9-b085-d6e648b5ec89)

## Project Structure

1.ingestion:

Purpose: Contains notebooks to ingest data from the raw layer to the ingested layer.
Functions: Handles incremental data loading for various datasets like results, pit stops, lap times, and qualifying.

2.trans:

Purpose: Contains notebooks to transform data from the ingested layer to the presentation layer.
Functions: Performs data transformations to prepare it for analysis.

3.set-up:

Purpose: Contains notebooks for setting up the environment.
Functions: Mounts Azure Data Lake Storage (ADLS) storages for raw, ingested, and presentation layers in Databricks.

4.includes:

Purpose: Contains notebooks with helper functions.
Functions: Provides reusable functions used in the transformation processes.

5.analysis:

Purpose: Contains SQL files for analytical queries.
Functions: Finds dominant drivers and teams and prepares results for visualization.

6.raw:

Purpose: Contains SQL files to create ingested tables.
Functions: Uses Spark SQL to create tables from raw data.

7.utils:

Purpose: Contains utility SQL files.
Functions: Drops all databases to facilitate incremental loading.

8.data:

Purpose: Contains sample raw data.
Functions: Provides sample data from the Ergast API for testing and development.

## Data Processing Pipeline Overview
Raw Data Ingestion:

Import data from the Ergest Developer API into a raw ADLS container on Azure.

Ingested Raw Layer:

Use Databricks Notebooks to process raw data:
Apply schema and store data in Parquet format.
Create partitions and add audit information (e.g., date, data source).

Transformation for Presentation:

Transform ingested data using Databricks Notebooks to prepare it for analytical dashboards.

Scheduling and Monitoring:

Use Azure Data Factory for scheduling and monitoring data workflows.

Delta Lakehouse Architecture:

Transition to Delta Lakehouse to meet GDPR, time travel, and other advanced requirements.
