from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from snowflake.helper import sfOptions
import os

os.environ['HADOOP_HOME'] = "C:\hadoop-3.3.0"

def sales_analysis():
    # Create a SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("Sales Analysis") \
        .enableHiveSupport() \
        .config('spark.jars.packages', 'net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3') \
        .getOrCreate()

    # Read the product dataset
    cust_path = "hdfs://localhost:9000/cleansed_output"
    product_df = spark.read.json(cust_path + "/*.json")

    # Read the sales dataset from local and select required columns
    sales_path = "hdfs://localhost:9000/sales_2015"
    sales_df = spark.read.json(sales_path + "/*.json")

    # Drop unwanted columns from product_df
    columns_to_drop = ['FullName', 'EmailAddress']
    cust_data = product_df.drop(*columns_to_drop)

    # Overwrite the original input path with the transformed DataFrame
    cust_data.coalesce(1).write.mode("overwrite").json("hdfs://localhost:9000/curated_data")

    # output to snowflake
    cust_data.coalesce(1).write.format("snowflake").options(**sfOptions) \
        .option("dbtable", "{}".format(r"curated")).mode("overwrite").options(header=True).save()

    # Save the DataFrame to Hive table
    cust_data.coalesce(1).write.mode("overwrite").saveAsTable("curated_layer")

    # Provide aliases for CustomerKey columns
    sales_df = sales_df.withColumnRenamed("CustomerKey", "sales_CustomerKey")
    cust_data = cust_data.withColumnRenamed("CustomerKey", "cust_CustomerKey")

    joined_df = sales_df.join(cust_data, sales_df["sales_CustomerKey"] == cust_data["cust_CustomerKey"])

    """Average Number of Orders Placed in a Day per Customer:"""

    average_orders_per_day = joined_df.groupBy("OrderDate", "sales_CustomerKey").agg(countDistinct("OrderNumber").alias("order_count_per_day"))
    average_orders_per_day = average_orders_per_day.groupBy("sales_CustomerKey").agg(avg("order_count_per_day").alias("average_orders_per_day"))


    # Overwrite the original input path with the transformed DataFrame
    average_orders_per_day.coalesce(1).write.mode("overwrite").json("hdfs://localhost:9000/avg_per_day")

    # output to snowflake
    average_orders_per_day.coalesce(1).write.format("snowflake").options(**sfOptions) \
        .option("dbtable", "{}".format(r"avg_per_day")).mode("overwrite").options(header=True).save()

    # Save the DataFrame to Hive table
    average_orders_per_day.coalesce(1).write.mode("overwrite").saveAsTable("avg_per_day")

    # Stop the SparkSession
    spark.stop()

# Call the function to perform the sales analysis
sales_analysis()
