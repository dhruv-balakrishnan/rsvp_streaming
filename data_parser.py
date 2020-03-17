from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from kafka import *
from pyspark.sql.functions import UserDefinedFunction

import os
# setup arguments
submit_args = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
if 'PYSPARK_SUBMIT_ARGS' not in os.environ:
    os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args
else:
    os.environ['PYSPARK_SUBMIT_ARGS'] += submit_args

import json

def publish_message(producer, topic, key, value):
    key = bytes(key, encoding='utf-8')
    value = bytes(value, encoding='utf-8')
    producer.send(topic, key, value)
    producer.flush

#def clean_message(message, schema):


def connect_to_producer():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    return producer

def connect_to_consumer(topic="test", offset='latest'):
    """
    Creates a consumer for topic.
    :param topic: the topic to connect to
    :param offset: the offset to use. One of: 'latest', 'earliest' 'none'
    :return:
    """
    consumer = KafkaConsumer(topic, auto_offset_reset=offset, bootstrap_servers=['localhost:9092'])
    return consumer


if __name__ == "__main__":

    java8_location = '/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home'
    os.environ['JAVA_HOME'] = java8_location

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
                StructField("group_name", StringType(), False)
            ]))
        ])

    producer = connect_to_producer()
    consumer = connect_to_consumer('test', 'earliest')

    # Simple function to strip out the 'b' in the raw data to allow proper
    # json -> Struct parsing.
    udf = UserDefinedFunction(lambda x: str(x), StringType())

    # Reading from our raw data stream, so we can clean up the data
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest")\
        .option("subscribe", "test") \
        .load()

    # Reading from our cleaned data stream, so we can perform analyses
    clean_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest") \
        .option("subscribe", "clean") \
        .load()

    # json_data = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
    #     .select(udf('value').alias('data'))\
    #     .select(F.from_json('data', json_schema))\
    #     .writeStream.format("console") \
    #     .option("truncate", "false") \
    #     .start().awaitTermination()

    # Cleaning raw data.
    json_data = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
        .select(F.from_json('value', json_schema).alias('value')) \
        .select(F.to_json('value').alias('value')) \
        .writeStream.format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("checkpointLocation", "/Users/dhruv/Checkpoints")\
        .option("topic", "clean").start()

    avg_guests_per_country = clean_stream.selectExpr("CAST(value as STRING)")\
        .select(F.from_json("value", json_schema).alias("data"))\
        .select('data.*').select('guests', 'group.*')\
        .groupBy('group_country')\
        .avg('guests').alias('avg_guests')\
        .writeStream.format('console')\
        .option('truncate', 'false')\
        .outputMode('complete').start()

    common_countries = clean_stream.selectExpr("CAST(value as STRING)") \
        .select(F.from_json("value", json_schema).alias("data")) \
        .select('data.*').select('group.*') \
        .groupBy('group_country') \
        .count().alias('country_counts') \
        .writeStream.format('console') \
        .option('truncate', 'false') \
        .outputMode('complete').start()





    spark.streams.awaitAnyTermination()



