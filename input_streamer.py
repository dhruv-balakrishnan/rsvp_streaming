import requests
from kafka.producer import *
import time

"""
A basic kafka producer that reads a stream and pushes messages onto 
a kafka topic
"""


def get_producer(server='localhost:2181'):
    producer = KafkaProducer(bootstrap_servers=[server])
    return producer


def publish(producer, topic="test", key="raw", value=""):
    key = bytes(key, encoding='utf-8')
    #value = bytes(value, encoding='utf-8')
    producer.send(topic, value, key)
    producer.flush


if __name__ == "__main__":

    stream_url = "http://stream.meetup.com/2/rsvps"
    server = 'localhost:9092'
    raw_topic = 'test'
    raw_key = 'raw'

    request = requests.get(stream_url, stream=True)
    producer = get_producer(server)

    print("Gathering data..")
    count=0

    for line in request.iter_lines():
        time.sleep(5)
        publish(producer, raw_topic, raw_key, line)
