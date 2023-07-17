from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from snowflake.helper import sfOptions
import os

os.environ['HADOOP_HOME'] = "C:\hadoop-3.3.0"

def transform_and_output_to_hdfs():
    # Create a SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("DataFrame Transformations Example") \
        .enableHiveSupport() \
        .config('spark.jars.packages',
                'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3')\
        .getOrCreate()



    # Read multiple JSON files into a DataFrame
    df = spark.read.option("multiline","true").json(r"C:\Users\sunil.kn\Downloads\j\AdventureWorksCustomers-210509-235702.json")

    df.show()
    # Output the transformed DataFrame to HDFS in JSON format
    output_path = "hdfs://localhost:9000/rawlayer"
    df.write.mode("overwrite").json(output_path)
    df.write.format("snowflake").options(**sfOptions) \
        .option("dbtable", "{}".format(r"raw_layer")).mode("overwrite").options(header=True).save()

    # Stop the SparkSession
    spark.stop()

# Call the function to perform the transformation and output to HDFS
transform_and_output_to_hdfs()
