"""Apache Airflow DAG to stage and extract I94 immigration data from data lake to data warehouse"""

from datetime import datetime
from airflow.models import DAG
# import configparser
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# config = configparser.ConfigParser()
# config.read('../../config/capstone.cfg')

DATA_LAKE_PATH = 's3a://capstone-project-us-immigration-bucket/'
DATABASE_CONNECTION_ID = "redshift"
AWS_CREDENTIALS_ID = "aws_credentials"

default_arguments = {
    'owner': 'joyce',
    'description': 'Import immigration data from S3 to various supporting dimension tables and fact table',
    'start_date': datetime(2016, 1, 1),
    'end_date': datetime(2016, 1, 31),

}

# Partitioned path broken down by year, month, and day or arrival
templated_partition_path = \
    "{{execution_date.strftime('s3://capstone-project-us-immigration-bucket/immigration_data/year=%Y/month=%-m/partition_date=%Y-%m-%d/*.parquet')}}"

dag = DAG('etl_redshift',
          default_args=default_arguments,
          schedule_interval='@daily',
          max_active_runs=1,
          concurrency=3)

# Stage I94 immigration data for partition path to database
stage_immigration_data_task = StageToRedshiftOperator(
    task_id="stage_immigration_data",
    dag=dag,
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_path=templated_partition_path,
    table_name="staging_immigration",
    truncate_table=False,
    copy_format="PARQUET"
)

load_immigration_data_task = LoadFactOperator(
    task_id='load_immigration_fact_table',
    dag=dag,
    table='fact_immigration',
    redshift_conn_id=DATABASE_CONNECTION_ID,
    select_sql=SqlQueries.immigration_table_insert
)

load_ports_task = LoadDimensionOperator(
    task_id="load_ports",
    dag=dag,
    table='dim_ports',
    redshift_conn_id=DATABASE_CONNECTION_ID,
    select_sql=SqlQueries.ports_table_insert
)

load_countries_task = LoadDimensionOperator(
    task_id="load_countries",
    dag=dag,
    table='dim_countries',
    redshift_conn_id=DATABASE_CONNECTION_ID,
    select_sql=SqlQueries.countries_table_insert
)

load_time_task = LoadDimensionOperator(
    task_id="load_time",
    dag=dag,
    table='dim_time',
    redshift_conn_id=DATABASE_CONNECTION_ID,
    select_sql=SqlQueries.time_table_insert
) 

load_demographics_task = LoadDimensionOperator(
    task_id="load_demographics",
    dag=dag,
    table='dim_demographics',
    redshift_conn_id=DATABASE_CONNECTION_ID,
    select_sql=SqlQueries.demographics_table_insert
) 

# define a table/column map to check null values
# check_table_column_map = { 'fact_immigration':'immigration_id', 'dim_countries':'country_code', 'dim_ports':'port_code','dim_demographics':'city', 'dim_time':'sas_timestamp'}

check_table_column_map = {'staging_countries':'country_code', 'staging_ports':'port_code','staging_demographics':'city', 'dim_countries':'country_code', 'dim_ports':'port_code','dim_demographics':'city', 'dim_time':'sas_timestamp',         'staging_immigration':'immigration_id', 'fact_immigration':'immigration_id', }

# query template to check null values
# check_nulls_query = "SELECT COUNT(*) FROM {schema}.{table} WHERE {column} IS NULL" 

check_empty_table = "SELECT COUNT(*) FROM {schema}.{table}"

dq_check_input = [
#     {
#         "check_feature": "null value",
#         "check_query": check_nulls_query,
#         "expected_result": 0,
#         "check_condition": "equal"
#     },
    {
        "check_feature": "empty_table",
        "check_query": check_empty_table,
        "expected_result": 0,
        "check_condition": "greater than"
    }
]

data_quality_check_task = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=DATABASE_CONNECTION_ID,
    target_schema="public",
    map_table_column=check_table_column_map,
    check_input = dq_check_input
)

# Setup DAG pipeline
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_immigration_data_task
stage_immigration_data_task >> load_immigration_data_task
load_immigration_data_task >> [load_ports_task, load_countries_task, load_time_task]
# load_ports_task >> [load_demographics_task, load_airports_task]
# [load_demographics_task, load_airports_task, load_countries_task, load_time_task] >> data_quality_check_task
load_ports_task >> load_demographics_task
[load_demographics_task, load_countries_task, load_time_task] >> data_quality_check_task

data_quality_check_task >> end_operator


