# Read Data and stream to GCS 

from __future__ import annotations
import logging
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType, MapType
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import mean, stddev, max, udf
from pyspark.sql.types import ArrayType, DoubleType
from tables import Column
from typing import List, Optional
import pyspark.sql.functions as F
from pyspark.sql.functions import col, year, month, dayofmonth, hour
import atexit

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

# Load configuration from env variables
project_id = os.getenv('PROJECT_ID')
topic = os.getenv('TOPIC')
subscription_id = os.getenv('SUBSCRIPTION')
bucket_name = os.getenv('BUCKET_NAME')
bucket_path = f"gs://{bucket_name}/data/"
keyfile_path = ''
cluster_manager  = "spark://164.92.85.68:7077"

def get_session() -> SparkSession:
    """Create a spark session for the Spark application.

    Returns:
        SparkSession.
    """
    # Configure Spark
    spark = SparkSession.builder \
    .appName("Read_Pubsub_GCS") \
    .config('spark.master',cluster_manager)\
    .getOrCreate()

    # Configure GCS credentials
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keyfile_path)

    # Set GCS as the default file system
    spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    # Get app id
    app_id = spark.conf.get("spark.app.id")

    logging.info(f"SparkSession started successfully for app: {app_id}")

    return spark
spark = get_session()




def define_schema():
    """
    Define schema for Pub/Sub data.
    # Define schemas for Kafka topics

        Args:
            spark (SparkSession): spark session
            broker_address (str): kafka broker address Ex: localhost:9092
            topic (str): topic from which events needs to consumed
            offset (str, optional): _description_. Defaults to "earliest".

        Returns:
            DataStreamReader: Interface used to load a streaming DataFrame from external storage systems

        Reference:
            https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
    """
    spark = get_session()

    schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])
    return schema



def read_pubsub_data(pubsub_topic, schema: StructType) -> DataFrame:
    """
    Read data from Pub/Sub Function.
    
    """
    
    pubsub_df = spark \
        .readStream \
        .format("pubsub") \
        .option("projectId", project_id) \
        .option("subscriptionId", subscription_id) \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("failOnDataLoss", False) \
        .option("schema", schema.json()) \
        .load()
    
    pubsub_df1 = pubsub_df.selectExpr("CAST(data AS STRING) AS value")
    pubsub_df2 = pubsub_df1.select(F.from_json("value", schema).alias("data")).select("data.*")
    
    return pubsub_df2


schema = define_schema()
# Read data from Pub/Sub
read_pubsub_df = read_pubsub_data('input_topic', schema)

# Print schema
read_pubsub_df.printSchema()



def write_Bucket_stream():
    """
    Starts Streaming data from pubsub into GCS Buckets
    """
    logging.info("Streaming has started.....")
    Read_Data = read_pubsub_data(topic,schema)

    bucket_path = f"gs://{bucket_name}/data/"

     # Write data to GCS
    # query = Read_Data \
    #     .writeStream \
    #     .format("json") \
    #     .option("path", bucket_path) \
    #     .option("checkpointLocation", f"gs://{bucket_name}/temp_location") \
    #     .outputMode("append") \
    #     .start()


    # Stream Data to GCS  
def StreamWriter(input:DataFrame,checkpointFolder,Output):
    return (input.writeStream.format('json')
            .option('checkpointLocation',checkpointFolder)
            .option('path',Output)
            .trigger(processingTime='3 Seconds')
            .start()
            )

query = StreamWriter(read_pubsub_df,
                     f'gs://{bucket_name}/temp_location',
                     f'gs://{bucket_name}/data/'
                     )
query.awaitTermination()
spark.stop()
    
    

        























