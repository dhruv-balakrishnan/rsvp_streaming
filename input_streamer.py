import requests
from kafka.producer import *
import datetime
import time
import json

"""
A basic kafka producer that reads a stream and pushes messages onto 
a kafka topic
"""


def get_producer(server='localhost:2181'):
    producer = KafkaProducer(bootstrap_servers=[server], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    return producer

def publish(producer, topic, key, value):
    key = bytes(key, encoding='utf-8')
    #value = bytes(value, encoding='utf-8')
    producer.send(topic, value, key)
    producer.flush()


if __name__ == "__main__":

    stream_url = "http://stream.meetup.com/2/rsvps"
    server = 'localhost:9092'
    raw_topic = 'raw'
    raw_key = 'raw'

    request = requests.get(stream_url, stream=True)
    producer = get_producer(server)

    print("Gathering data..")

    for line in request.iter_lines():
        time.sleep(3) # So my system doesn't burn.
        try:
            j_object = json.loads(str(line)[2:-1])
            j_object['timestamp'] = str(datetime.datetime.now())
            publish(producer, raw_topic, raw_key, j_object)

        except ValueError as V:
            # Error rate seems to be fairly high.
            # Wonder what the cause of that is.
            pass
