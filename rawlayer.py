from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from snowflake.helper import sfOptions
import os

def transform_and_output_to_hdfs():
    # Create a SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("DataFrame Transformations Example") \
        .enableHiveSupport() \
        .config('spark.jars.packages',
                'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3')\
        .getOrCreate()

    raw_path = "hdfs://localhost:9000/rawlayer"
    output_path = "hdfs://localhost:9000/rawlayer_output"

    # Read multiple JSON files into a DataFrame
    df = spark.read.json(raw_path+"/*.json")

    df.show()

    transformed_df = df.fillna({"EmailAddress": ""}) \
        .fillna({"EducationLevel": ""})\
        .withColumn("TotalChildren", col("TotalChildren").cast(IntegerType())) \
        .withColumn("FullName", concat(col("Prefix"), lit(" "), col("FirstName"), lit(" "), col("LastName"))) \
        .withColumn("IncomeCategory",
                    when(col("AnnualIncome") < 50000, "Low")
                    .when(col("AnnualIncome") < 100000, "Medium")
                    .otherwise("High"))

    # Overwrite the original input path with the transformed DataFrame
    transformed_df.coalesce(1).write.mode("overwrite").json(output_path)

    # output to snowflake
    transformed_df.coalesce(1).write.format("snowflake").options(**sfOptions) \
        .option("dbtable", "{}".format(r"raw_layer")).mode("overwrite").options(header=True).save()

    # Save the DataFrame to Hive table
    transformed_df.coalesce(1).write.mode("overwrite").saveAsTable("raw_layer")

    # Stop the SparkSession
    spark.stop()

# Call the function to perform the transformation and output to HDFS
transform_and_output_to_hdfs()
