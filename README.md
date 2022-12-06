# US Immigrations Data Warehouse

## Overview

The objective of the project is to create a datawarehouse to analyze the trends in US immigration patterns. The idea is to identify a number of disparate data sources, clean the data, and process it through an ETL pipeline to create a useful dataset for analytics. The main dataset for the project is immigration data for US ports. This dataset is enriched using datasets from various sources.  The final data warehouse can be used to perform analytics on the US immigration data to gain insights and find relevant patterns. 

At a high level, data is extracted from the immigration SAS data, partitioned by year, month, and arrival day, and stored in a data lake on Amazon S3 as Parquet files.The partitioned data is loaded into Redshift into staging tables. The staging data is combined with other staged data sources to produce the final fact and dimension records in the Redshift warehouse.

## Datasets

* I94 Immigration Data  
This data comes from the US National Tourism and Trade Office. The data consists of 12 files containing data for each month. Each file has around 3 million rows and 28 columns. All the fields of the dataset is explained in `docs/I94_SAS_Labels_Descriptions.SAS`. The data is obtained from [here](https://www.trade.gov/national-travel-and-tourism-office). As the dataset is too large, it is not uploaded to the repo. A small sample of the data is in `data/immigration_data_sample.csv`. 

* World Temperature Data  
This dataset came from [Kaggle](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data). `GlobalLandTemperaturesByCity.csv` and `GlobalLandTemperaturesByCountry.csv` are used from this dataset. These files are present in the `data` folder.

* US city Demographic Data  
This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). 


## Data Model

The project uses star schema. There is one fact table with the main immigration data with multiple dimension tables surrounding it. The schema of all the tables are available in the `data/data_dictionary.md` file.

![Data model](/docs/images/data_model.png)


## Tools And Technologies

The main technologies used are:

**Python3**  

**Amazon S3** - S3  is used as the data lake storage of the data to be processed.  

**Apache Spark** - Spark is used to extract, clean, and partition the immigration data. As the immigration data files are too large, it is preprocessed using Spark. In production we would probably add a DAG to Apache Airflow to submit a job to a Spark cluster on a monthly basis or as needed.  

**Apache Airflow** - Apache Airflow is used as a tool for the primary data pipeline. The pipeline schedules and coordinates the flow of data from the S3 data lake to Amazon Redshift and performs quality checks along the way. Airflow makes it easy to set up the pipeline and make adjustments as requirements change over time.  

**Amazon Redshift** - Redshift is used for the data warehouse. Redshift offers efficient storage as well as high performance query processing.  

## Data Preprocessing
The data is preprocessed before loading to the S3 bucket.  The preprocessig script for the immigration data are in `spark/extract_immigration_data.py`. The preprocessed immigration data is also uploaded to the S3 bucket using this script. All the other datasets are cleaned and uploaded to S3 bucket manually. The `data/data_for_s3_upload` contains all the other cleaned datasets. 

## Data Pipeline

Airflow uses directed acyclic graphs (DAG's) to describe a pipeline workflow. Each DAG is made up of tasks which are the nodes of the graph. Each task implements an operator of some type to execute code.

The main data pipeline uses Apache Airflow to process immigration data for single day at a time. It brings in the immigration data from Amazon S3 and combines it with other staging data for ports,countries, city and country temperatures, and city demographics. The immigration data in the S3 bucket is partitioned by year, month and arrival date.

There are 2 DAGs to be executed.


* crete_tables.py - This DAG is responsible for creating the schema of the database and loading some static staging data.

![create_table Graph view](/docs/images/create_tables.png)


* etl.py -  This DAG loads the immigration data into a staging table in Redshift and then combines it with other staging data to produce the final dimension and fact table entries that are inserted.

![etl DAG Graph view](/docs/images/etl_dag_graph.png)

## How to Run

* Create a new IAM role and S3 bucket. Upload all the files from `data/data_for_s3_upload` to the S3 bucket.
* Create a Redshift cluster, and update `config\capstone.cfg` with all the required values.
* Run `spark/extract_immigration_data.py` to clean and upload the immigration data to the S3 bucket.
* Upload all the other cleaned datasets from `data/data-for-s3-upload` to the S3 bucket.
* Update the DATA_LAKE_PATH variable in both the DAG files to point to the S3 bucket.
* Start the airflow server and create connection variables for AWS and Redshift.
* Run `airflow/dags/create_tables.py` to create the schema and load all the static data.
* Run `airflow/dags/etl.py` to load the immigration data and load data to the fact table and all the dimension tables.
* Run queries on the data warehouse using the `notebooks/analytics.pynb` notebook.
* Make sure to delete the cluster as well as the S3 bucket as this would incur costs. The cluster can be deleted using the `notebooks/deployment/delete_cluster.ipynb` notebook.

## Approach To Other Scenarios

**If the data was increased by 100x.**  
 If the data were increased 100x, then we should be able to easily handle it using Spark and Redshift and scaling the clusters as needed.As of now, some of the data cleaning is done using pandas. We may have to use Spark instead of pandas in this case. 
 
**If the pipelines were run on a daily basis by 7am.**  
The pipeline is already set to run daily. The time taken to process a day is a matter of minutes. We might modify the scheduling to a specific time of day and introduce an SLA in Airflow to ensure jobs are completed in a timely manner and adjust accordingly.

**If the database needed to be accessed by 100+ people**  
Amazon Redshift as a data warehouse should have no issues with this and can be scaled as needed.
