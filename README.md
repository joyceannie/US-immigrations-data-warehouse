# US Immigrations Data Warehouse

## Overview

The objective of the project is to create a datawarehouse to analyze the trends in US immigration patterns. The idea is to identify a number of disparate data sources, clean the data, and process it through an ETL pipeline to create a useful dataset for analytics. The main dataset for the project is immigration data for US ports. This dataset is enriched using datasets from various sources.  The final data warehouse can be used to perform analytics on the US immigration data to gain insights and find relevant patterns. 

At a high level, data is extracted from the immigration SAS data, partitioned by year, month, and arrival day, and stored in a data lake on Amazon S3 as Parquet files.The partitioned data is loaded into Redshift into staging tables. The staging data is combined with other staged data sources to produce the final fact and dimension records in the Redshift warehouse.

## Datasets

* I94 Immigration Data: 
This data comes from the US National Tourism and Trade Office. The data consists of 12 files containing data for each month. Each file has around 3 million rows and 28 columns. All the fields of the dataset is explained in `docs/I94_SAS_Labels_Descriptions.SAS`. The data is obtained from [here](https://www.trade.gov/national-travel-and-tourism-office). A small sample of the data is in `data/immigration_data_sample.csv`.

* World Temperature Data
This dataset came from [Kaggle](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data). `GlobalLandTemperaturesByCity.csv` and `GlobalLandTemperaturesByCountry.csv` are used from this dataset. These files are present in the `data` folder.

* US city Demographic Data
This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). 


## Data Model

The project uses star schema. There is one fact table with the main immigration data with multiple dimension tables surrounding it. The schema of all the tables are available in the `data/data_dictionary.md` file.

![Data model](/docs/images/data_model.png)


## Tools And Technologies

The main technologies used are:

Python3

Amazon S3 - S3  is used as the data lake storage of the data to be processed. 

Apache Spark - Spark is used to extract, clean, and partition the immigration data. As the immigration data files are too large, it is preprocessed using Spark. The preprocessig scripts are in `spark/extract_immigration_data.py`. In production we would probably add a DAG to Apache Airflow to submit a job to a Spark cluster on a monthly basis or as needed. 

Apache Airflow - Apache Airflow is used as a tool for the primary data pipeline. The pipeline schedules and coordinates the flow of data from the S3 data lake to Amazon Redshift and performs quality checks along the way. Airflow makes it easy to set up the pipeline and make adjustments as requirements change over time.

## Data Pipeline

Airflow uses directed acyclic graphs (DAG's) to describe a pipeline workflow. Each DAG is made up of tasks which are the nodes of the graph. Each task implements an operator of some type to execute code.

Before executing the pipeline, all the data is uploaded to the S3 bucket. As mentioned earlier, the immigration data is preprocessed using Spark before uploading to S3 bucket. The demographics data is preprocessed using pandas. The files in `data_for_s3_upload` are uploaded manually to S3 bucket. This includes countries, ports and demographics data. 

The main data pipeline uses Apache Airflow to process immigration data for single day at a time. It brings in the immigration data from Amazon S3 and combines it with other staging data for ports,countries, city and country temperatures, and city demographics. The immigration data in the S3 bucket is partitioned by year, month and arrival date.

There are 2 DAGs to be executed.


* crete_tables.py - This DAG is responsible for creating the schema of the database and loading some static staging data.

* etl.py -   This DAG loads the immigration data into a staging table in Redshift and then combines it with other staging data to produce the final dimension and fact table entries that are inserted.




