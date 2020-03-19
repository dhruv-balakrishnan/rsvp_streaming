from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from kafka import *
from pyspark.sql.functions import UserDefinedFunction
import psycopg2

import os
# setup arguments
submit_args = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --jars /Users/dhruv/PycharmProjects/rsvp_streaming_project/postgresql-42.2.11.jar pyspark-shell'
java8_location = '/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home'

os.environ['JAVA_HOME'] = java8_location
if 'PYSPARK_SUBMIT_ARGS' not in os.environ:
    os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args
else:
    os.environ['PYSPARK_SUBMIT_ARGS'] += submit_args


def update_group_and_country(df, epoch_id):
    """
    Given a DataFrame, takes the data and pushes it to a SQL table.
    Overwrites the data due to constraints.
    :param df: the dataframe
    :param epoch_id: ID representing the batch
    :return: None
    """
    host = 'localhost'
    db = 'rsvp_streaming'

    jdbcUrl = f"jdbc:postgresql://{host}/{db}"
    connectionProperties = {
        "user": 'postgres',
        "password": 'boobiepotato',
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(url = jdbcUrl, table='group_and_country_counts', mode='append', properties=connectionProperties)

def update_group_locations(df, epoch_id):
    """
    Given a DataFrame, takes the data and pushes it to a SQL table.
    Overwrites the data due to constraints.
    :param df: the dataframe
    :param epoch_id: ID representing the batch
    :return: None
    """
    host = 'localhost'
    db = 'rsvp_streaming'

    jdbcUrl = f"jdbc:postgresql://{host}/{db}"
    connectionProperties = {
        "user": 'postgres',
        "password": 'boobiepotato',
        "driver": "org.postgresql.Driver"
    }
    print(df)
    df.write.jdbc(url=jdbcUrl, table='group_locations', mode='append', properties=connectionProperties)

if __name__ == "__main__":

    spark = SparkSession.builder.appName('abc').getOrCreate()

    json_schema = StructType([
            StructField("venue", StructType([
                StructField("venue_name", StringType(), False),
                StructField("venue_id", StringType(), False)
            ])),

            StructField("response", StringType(), False),
            StructField("guests", IntegerType(), False),
            StructField("member", StructType([
                StructField("member_id", StringType(), False)
            ])),

            StructField("rsvp_id", StringType(), False),
            StructField("event", StructType([
                StructField("event_name", StringType(), False),
                StructField("event_id", StringType(), False)
            ])),

            StructField("group", StructType([
                StructField("group_city", StringType(), False),
                StructField("group_country", StringType(), False),
                StructField("group_id", StringType(), False),
                StructField("group_name", StringType(), False),
                StructField("group_lat", StringType(), False),
                StructField("group_lon", StringType(), False)
            ])),

            StructField("timestamp", TimestampType(), False)
        ])

    # Simple function to strip out the 'b' in the raw data to allow proper
    # json -> Struct parsing.
    udf = UserDefinedFunction(lambda x: str(x), StringType())

    # Reading from our raw data stream, so we can clean up the data
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option('failOnDataLoss', 'false') \
        .option("startingOffsets", "earliest")\
        .option("subscribe", "raw") \
        .load()

    # Cleaning raw data.
    json_data = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(F.from_json('value', json_schema).alias('value')) \
        .select(F.to_json('value').alias('value')) \
        .writeStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("checkpointLocation", "/Users/dhruv/Checkpoints") \
        .option("topic", "clean").start()

    # Reading from our cleaned data stream, so we can perform analyses
    clean_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option('failOnDataLoss', 'false') \
        .option("startingOffsets", "earliest") \
        .option("subscribe", "clean") \
        .load()

    avg_guests_per_country = clean_stream.selectExpr("CAST(value as STRING)") \
        .select(F.from_json("value", json_schema).alias("data")) \
        .select('data.*').select('guests', 'group.*') \
        .groupBy('group_country') \
        .avg('guests').alias('avg_guests') \
        .writeStream.format('console') \
        .option('truncate', 'false') \
        .outputMode('complete').start()

    # # Number of RSVP's per country
    common_countries = clean_stream.selectExpr("CAST(value as STRING)", "CAST(timestamp AS TIMESTAMP)") \
        .select(F.from_json("value", json_schema).alias("data")) \
        .select('data.*').select('group.*') \
        .groupBy('group_country') \
        .count().alias('country_counts') \
        .writeStream.format('console') \
        .option('truncate', 'false') \
        .outputMode('complete').start()

    # # RSVP's per group and country
    group_country_counts = clean_stream.selectExpr("CAST(value as STRING)", "CAST(timestamp as STRING)") \
        .select(F.from_json("value", json_schema).alias("data")) \
        .select('data.*').select('group.*', 'timestamp') \
        .withWatermark('timestamp', '1 minute') \
        .groupBy(F.window('timestamp', '1 minute'), 'group_name', 'group_country') \
        .count().alias('group_country_counts') \
        .select('group_name', 'group_country', 'count') \
        .writeStream.format('console') \
        .option('truncate', 'false') \
        .trigger(processingTime="1 minute") \
        .start()

    # # Grab and store the lat and long of each group
    group_locations = clean_stream.selectExpr("CAST(value as STRING)", "CAST(timestamp as STRING)") \
        .select(F.from_json("value", json_schema).alias("data")) \
        .select('data.*').select('group.*', 'timestamp') \
        .select('group_name', 'group_lat', 'group_lon') \
        .writeStream.format('console') \
        .option('truncate', 'false') \
        .start()

    spark.streams.awaitAnyTermination()