"""Apache Airflow DAG to setup database schema and import static staging data"""

import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries

DATA_LAKE_PATH = 's3://capstone-project-us-immigration'
DATABASE_CONNECTION_ID = "redshift"
AWS_CREDENTIALS_ID = "aws_credentials"

default_args = {"owner": "joyce"}

dag = DAG('setup_staging_area',
          default_args=default_args,
          description='Create table schema in database and load static staging data',
          schedule_interval=None,
          start_date=datetime.datetime(2019, 1, 1),
          concurrency=2
          )

# Create staging table for immigration data
create_staging_immigration_table = PostgresOperator(
    task_id="create_staging_immigration_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_staging_immigration_table
)

# Create staging table for country data
create_staging_countries_table = PostgresOperator(
    task_id="create_staging_countries_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_staging_countries_table
)

# Create staging table for port data
create_staging_ports_table = PostgresOperator(
    task_id="create_staging_ports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_staging_ports_table
)

# Create staging table for demographics data
create_staging_demographics_table = PostgresOperator(
    task_id="create_staging_demographics_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_staging_demographics_table
)

# Create city temperatures staging table
create_staging_city_temps_table = PostgresOperator(
    task_id="create_staging_city_temps_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_staging_city_temps_table
)

# Create country temperatures staging table
create_staging_country_temps_table = PostgresOperator(
    task_id="create_staging_country_temps_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_staging_country_temps_table
)

# Create dimension table for country data
create_countries_table = PostgresOperator(
    task_id="create_countries_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_countries_table
)

# Create dimension table for port data
create_ports_table = PostgresOperator(
    task_id="create_ports_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_ports_table
)

# Create dimension table for demographics data
create_demographics_table = PostgresOperator(
    task_id="create_demographics_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_demographics_table
)

# Create dimension table for time data
create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_time_table
)

# Create fact table for immigration data
create_fact_immigration_table = PostgresOperator(
    task_id="create_fact_immigration_table",
    dag=dag,
    postgres_conn_id=DATABASE_CONNECTION_ID,
    sql=SqlQueries.create_fact_immigration_table
)



# Load port data into staging table
stage_port_codes_task = StageToRedshiftOperator(
    task_id="stage_port_codes",
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    dag=dag,
    table_name="public.staging_ports",
    s3_path=f"{DATA_LAKE_PATH}/i94_ports.csv",
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=False
)

# Load country codes into staging table
stage_country_codes_task = StageToRedshiftOperator(
    task_id="stage_country_codes",
    dag=dag,
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table_name='staging_countries',
    s3_path=f'{DATA_LAKE_PATH}/i94_countries.csv',
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=False,
)

# Load demographics data into staging table
stage_demographics_task = StageToRedshiftOperator(
    task_id="stage_demographics",
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    dag=dag,
    table_name="staging_demographics",
    s3_path=f"{DATA_LAKE_PATH}/us-cities-demographics.csv",
    truncate_table=False,
    copy_format='CSV IGNOREHEADER 1'
)

#  Load city temperature data into staging table
stage_city_temperatures_task = StageToRedshiftOperator(
    task_id="stage_city_temperatures",
    dag=dag,
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_path=f"{DATA_LAKE_PATH}/GlobalLandTemperaturesByCity.csv",
    table_name="staging_city_temperatures",
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=False
)

#  Load country temperature data into staging table
stage_country_temperatures_task = StageToRedshiftOperator(
    task_id="stage_country_temperatures",
    dag=dag,
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_path=f"{DATA_LAKE_PATH}/GlobalLandTemperaturesByCountry.csv",
    table_name="staging_country_temperatures",
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=False
)

# Setup DAG pipeline
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_staging_immigration_table
start_operator >> create_staging_countries_table
start_operator >> create_staging_ports_table
start_operator >> create_staging_demographics_table
start_operator >> create_time_table
start_operator >> create_ports_table
start_operator >> create_countries_table
start_operator >> create_staging_city_temps_table
start_operator >> create_staging_country_temps_table

# create_ports_table >> create_airports_table
start_operator >> create_demographics_table

create_staging_countries_table >> stage_country_codes_task
create_staging_ports_table >> stage_port_codes_task
# create_staging_airports_table >> stage_airports_codes_task
create_staging_demographics_table >> stage_demographics_task

create_time_table >> create_fact_immigration_table
create_ports_table >> create_fact_immigration_table
create_countries_table >> create_fact_immigration_table
create_demographics_table >> create_fact_immigration_table

create_staging_city_temps_table >> stage_city_temperatures_task
create_staging_country_temps_table >> stage_country_temperatures_task

create_staging_immigration_table >> end_operator
stage_country_codes_task >> end_operator
stage_port_codes_task >> end_operator
# stage_airports_codes_task >> end_operator
stage_demographics_task >> end_operator
stage_city_temperatures_task >> end_operator
stage_country_temperatures_task >> end_operator
create_fact_immigration_table >> end_operator