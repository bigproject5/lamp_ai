import os, json
from kafka import KafkaProducer
from settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_RESULT_TOPIC

class VehicleAuditProducer:
    def __init__(self, topic: str = None, bootstrap_servers: str = None):
        self.topic = topic or KAFKA_RESULT_TOPIC
        bs = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.producer = KafkaProducer(
            bootstrap_servers=bs,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
            linger_ms=5
        )

    def send(self, value: dict, key=None):
        self.producer.send(self.topic, key=key, value=value)
        self.producer.flush(1.0)
