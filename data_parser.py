import re
import time
import pyspark.sql.functions as F
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from kafka import *
import findspark
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

    java8_location = '/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home'  # Set your own
    os.environ['JAVA_HOME'] = java8_location

    spark = SparkSession.builder.appName('abc').getOrCreate()

    json_schema = StructType([
            StructField("venue", StructType([
                StructField("venue_name", StringType(), False),
                StructField("venue_id", FloatType(), False)
            ]))
            #
            # StructField("response", StringType(), False),
            # StructField("member", StructType([
            #     StructField("member_id", FloatType(), False)
            # ])),
            #
            # StructField("rsvp_id", FloatType(), False),
            # StructField("event", StructType([
            #     StructField("event_name", StringType(), False),
            #     StructField("event_id", FloatType(), False),
            # ])),

            # StructField("group_city", StringType(), False),
            # StructField("group_country", StringType(), False),
            # StructField("group_id", FloatType(), False),
            # StructField("group_name", StringType(), False)
        ])

    producer = connect_to_producer()
    consumer = connect_to_consumer('test', 'earliest')

    udf = UserDefinedFunction(lambda x: str(x)[1:-1], StringType())

    stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest")\
        .option("subscribe", "test") \
        .load()

    json_data = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
        .select(udf('value').alias('data')).select(F.from_json('data', json_schema)).writeStream.format("console") \
        .option("truncate", "false") \
        .start()

    json_data.awaitTermination()

    # json_data = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
    #     .select(from_json('value', json_schema))\
    #     .alias('meeting')\
    #     .select('meeting.*')\
    #     .writeStream.format("kafka")\
    #     .option("kafka.bootstrap.servers", "localhost:9092")\
    #     .option("topic", "clean").start()



