#!/usr/bin/env python

from kafka import KafkaConsumer
from data_common.config.configurer import get_conf

conf = get_conf()

namespaces = conf.namespaces

KAFKA_TOPIC = next(iter(namespaces.items()))[0]

KAFKA_BROKERS = "127.0.0.1:9092"

# This works with load balancers
# KAFKA_BROKERS = '35.205.200.161:32400,35.195.84.126:32401'

# from inside the cluster in a different namespace
# KAFKA_BROKERS = 'bootstrap.kafka.svc.cluster.local:9092'

print('shyo')

consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKERS)


for message in consumer:
    print(f"message is of type: {type(message)}")
    print(message)

print('yo')
consumer.subscribe([KAFKA_TOPIC])
