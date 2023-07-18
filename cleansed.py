from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from snowflake.helper import sfOptions
import os

os.environ['HADOOP_HOME'] = "C:\hadoop-3.3.0"

def transform_and_save_to_hive_hdfs():
    # Create a SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("DataFrame Transformations Example") \
        .enableHiveSupport() \
        .config('spark.jars.packages','net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3')\
        .getOrCreate()

    # Read the JSON file into a DataFrame
    json_file = "hdfs://localhost:9000/rawlayer_output"
    df = spark.read.json(json_file+"/*.json")

    # Drop unwanted columns
    columns_to_drop = ['FirstName', 'LastName', 'Prefix']
    df = df.drop(*columns_to_drop)

    # Encrypt sensitive columns using hashing techniques
    hashed_df = df.withColumn('FullName', sha2(col('FullName'), 256)) \
                  .withColumn('EmailAddress', sha2(col('EmailAddress'), 256))

    # Change the datatypes of hashed columns to StringType
    hashed_df = hashed_df.withColumn('FullName', col('FullName').cast(StringType())) \
                         .withColumn('EmailAddress', col('EmailAddress').cast(StringType()))\
                          .withColumn("BirthDateFormatted", to_date(col("BirthDate"), "M/d/yyyy").cast(DateType()))

    # Save the transformed DataFrame to Hive and HDFS as JSON format
    hashed_df.coalesce(1).write.mode("overwrite").format("hive").saveAsTable("cleansed_table")
    hdfs_output_path = "hdfs://localhost:9000/cleansed_output"
    hashed_df.coalesce(1).write.mode("overwrite").json(hdfs_output_path)
    hashed_df.write.format("snowflake").options(**sfOptions) \
       .option("dbtable", "{}".format(r"cleansed")).mode("overwrite").options(header=True).save()

    # Stop the SparkSession
    spark.stop()

# Call the function to perform the transformation and save to Hive and HDFS
transform_and_save_to_hive_hdfs()
