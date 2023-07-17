from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from kafka import KafkaProducer
import json
import os

def process_csv_to_hdfs():
    os.environ['HADOOP_HOME'] = "C:\hadoop-3.3.0"
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

    kafka_topic = "CUSTOMERS_DATA"
    kafka_server = "localhost:9092"
    csv_file = r"C:\Users\sunil.kn\Downloads\salesdataset\AdventureWorksSales2017-210509-235702.csv"
    output_path = "hdfs://localhost:9000/r_kafka_output"

    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read the CSV file into a DataFrame
    df = spark.read.format("csv").option("header", "true").load(csv_file)
    df.show()

    # Convert DataFrame to JSON and extract values
    json_data = df.toJSON().map(lambda x: json.loads(x)).collect()

    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_server)

    # Send JSON records to Kafka topic
    for record in json_data:
        message = json.dumps(record).encode('utf-8')
        producer.send(kafka_topic, value=message)

    # Close the producer
    producer.close()

    # Continue with the rest of the code...
    # Read from Kafka topic as a streaming DataFrame
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Process the streaming DataFrame as needed
    processed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), df.schema).alias("data")).select("data.*")

    # Write the processed DataFrame to HDFS as JSON files
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", "hdfs://localhost:9000/checkpoint") \
        .start()

    # Wait for the streaming query to finish
    query.awaitTermination()

    # Stop SparkSession once the query has finished
    spark.stop()

if __name__ == "__main__":
    process_csv_to_hdfs()
