"""

Spark job to extract SAS format i94 immigration data and global temperature  data and write it to Amazon S3 in Parquet format.

"""

# Import all the required libraries
import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging 

# Setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read('../config/capstone.cfg')
# Read AWS configurations
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# Read all other configurations
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

# Read input and output paths
I94_INPUT_PATH = config['LOCAL']['I94_INPUT_PATH']
TEMPERATURE_INPUT_PATH = config['LOCAL']['TEMPERATURE_INPUT_PATH']
OUTPUT_PATH = config['S3']['BUCKET_PATH']

def create_spark_session():
    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark

def convert_date(days):
    """
    Converts SAS date stored as days since 1/1/1960 to datetime
    :param days: Days since 1/1/1960
    :return: datetime
    """
    if days is None:
        return None
    return datetime.date(1960, 1, 1) + datetime.timedelta(days=days)

def convert_travel_mode(i94_mode):
    """
    Convert the i94 travel mode from integer to the corresponding value based on the data dictionary
    :param i94_mode: The travel mode in integer type
    :return: String
    """
    if i94_mode == 1:
        return 'Air'
    elif i94_mode == 2:
        return 'Sea'
    elif i94_mode == 3:
        return 'Land'
    else:
        return 'Not reported'
    
def convert_visa(visa):
    """
    Convert visa from integer type to the corresponding value based on the data dictionary
    :param visa: The visa code in integer type
    : return String
    """
    if visa is None:
        return "Not Reported"
    elif visa == 1:
        return "Business"
    elif visa == 2:
        return "Pleasure"
    elif visa == 3:
        return "Student"
    else:
        return "Not Reported"
        
def get_sas_day(days):
    """
    Converts SAS date stored as days since 1/1/1960 to day of month
    :param days: Days since 1/1/1960
    :return: Day of month value as integer
    """
    if days is None:
        return None
    return (datetime.date(1960, 1, 1) + datetime.timedelta(days=days)).day
    
def clean_immigration_data(spark, input_path, output_path):
    """
    Function to clean the immigration data, and write it in parquet format.
    The data is partitioned by year, month, and day.
    :param spark: Spark session
    :param input_path: Input path to SAS data
    :param output_path: Output path for Parquet files
    :return: None
    """
    
    convert_date_udf = F.udf(convert_date, DateType())
    convert_travel_mode_udf = F.udf(convert_travel_mode, StringType())
    convert_visa_udf = F.udf(convert_visa, StringType())
    get_sas_day_udf = F.udf(get_sas_day, StringType())
    
    #months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    months = ['jan']
    for month in months:
        input_data_path = f"{input_path}/i94_{month}16_sub.sas7bdat"
        
        # Processing i94 data for the month {month}
        
        # Read SAS data for month
        df = spark.read.format("com.github.saurfang.sas.spark").load(input_data_path)   
        
        # Select the needed columns with the correct datatype
        df = df.withColumn('country_code', df['i94cit'].cast(IntegerType())) \
            .withColumn('port_code', df['i94port'].cast(StringType())) \
            .withColumn('year', df['i94yr'].cast(IntegerType())) \
            .withColumn('month', df['i94mon'].cast(IntegerType())) \
            .withColumn('arrival_day', get_sas_day_udf(df['arrdate'])) \
            .withColumn('departure_day', get_sas_day_udf(df['depdate'])) \
            .withColumn('age', df['i94bir'].cast(IntegerType())) \
            .withColumn('birth_year', df['biryear'].cast(IntegerType())) \
            .withColumn('travel_mode', convert_travel_mode_udf(df['i94mode'])) \
            .withColumn('visa_category', convert_visa_udf(df['i94visa'])) \
            .withColumn('arrival_date', convert_date_udf(df['arrdate'])) \
            .withColumn('departure_date', convert_date_udf(df['depdate'])) \
            .withColumn('visa_type', df['visatype'])
       
        immigration_data = df.select(['year', 'month', 'arrival_day', 'country_code', 'port_code', 'age', 'travel_mode', 'visa_category', 'visa_type', 'gender', 'birth_year', 'arrival_date', 'departure_day', 'departure_date'])
        
        # Write data in parquet format partitioned by year, month and arrival_day
        print(f"Writing {input_data_path} to output...")
        immigration_data.write.mode("append").partitionBy("year", "month", "arrival_date") \
            .parquet(f"{output_path}/immigration_data")
        print(f"Completed {input_path}.")

def extract_country_temperature_data(spark, input_path, output_path):
    """
    Function to extract the latest temperature data from all the countires, and write the data in parquet format. 
    :param: spark: spark session
    :param: input_path: location of the input file
    :param: output_path: location of the output file
    :return: None
    """
    
    # Read data from the input path
    country_data_path = f"{input_path}/GlobalLandTemperaturesByCountry.csv"
    data = spark.read.option("header", True).option("inferSchema",True).csv(country_data_path)
    
    # Remove rows without temperature data
    data = data.filter(data.AverageTemperature.isNotNull())
    
    # Select latest temperature from all the countries
    data = data.withColumn("temperature_rank", F.dense_rank().over(Window.partitionBy("Country").orderBy(F.desc("dt"))))
    data = data.filter(data['temperature_rank'] == 1).orderBy("Country")
    
    # Write data in parquet format to the output path
    data.write.mode("overwrite").parquet(f"{output_path}/country_temperature_data")
    
def extract_city_temperature_data(spark, input_path, output_path):
    """
    Extract the latest temperature of all US cities, and write the data in parquet format
    :param: spark: spark session
    :param: input_path: location of the input file
    :param: output_path: location of the output file
    :return: None
    """
    
    # Read data from input path
    city_data_path  = f"{input_path}/GlobalLandTemperaturesByCity.csv"
    city_data = spark.read.option("header", True).option("inferSchema",True).csv(city_data_path)
    
    # Remove rows without temperature data
    city_data = city_data.filter(city_data.AverageTemperature.isNotNull())
    
    # Select cities within US
    city_data = city_data.filter(city_data['Country'] == "United States")
    
    # Select latest temperature of all the cities
    city_data = city_data.withColumn("temperature_rank", F.dense_rank().over(Window.partitionBy("City").orderBy(F.desc("dt"))))
    city_data = city_data.filter(city_data['temperature_rank'] == 1).orderBy('City')
    
    # Write data in parquet format to the output path
    city_data.write.mode("overwrite").parquet(f"{output_path}/city_temperature_data")   

def main():
    spark = create_spark_session()  
    clean_immigration_data(spark, I94_INPUT_PATH, OUTPUT_PATH)    
    extract_country_temperature_data(spark, TEMPERATURE_INPUT_PATH, OUTPUT_PATH)
    extract_city_temperature_data(spark, TEMPERATURE_INPUT_PATH, OUTPUT_PATH)
                                         
if __name__ == "__main__":
    main()