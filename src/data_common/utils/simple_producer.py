#!/usr/bin/env python
import json
from kafka import KafkaProducer

from data_common.config.configurer import get_conf


conf = get_conf()

namespaces = conf.namespaces

KAFKA_BROKERS = "127.0.0.1:9092"

# This works with load balancers
# KAFKA_BROKERS = '35.205.200.161:32400,35.195.84.126:32401'

# from inside the cluster in a different namespace
# KAFKA_BROKERS = 'bootstrap.kafka.svc.cluster.local:9092'

print('KAFKA_BROKERS: ' + KAFKA_BROKERS)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    linger_ms=1,
    acks='all',
    api_version='0.9'
)


some_number = "16629"


for namespace, v in namespaces.items():

    payload = {
        "Wielder": "cool",
        "namespace": namespace
    }

    value = json.dumps(payload)

    topic = "activities." + namespace
    key = namespace + '-' + some_number

    print(f"sending to topic: {topic} key: {key} value: {value}")

    producer.send(
        topic=topic,
        key=str.encode(key),
        value=str.encode(value)
    )

producer.flush()


