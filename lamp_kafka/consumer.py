import json
import threading
import logging
from kafka import KafkaConsumer
from settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_SOURCE_TOPIC, KAFKA_GROUP_ID

log = logging.getLogger("lamp_kafka.consumer")

class VehicleAuditConsumer:
    def __init__(self, topic: str = None, bootstrap_servers: str = None, group_id: str = None):
        self.topic = topic or KAFKA_SOURCE_TOPIC
        bs = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        gid = group_id or KAFKA_GROUP_ID
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=bs,
            group_id=gid,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            consumer_timeout_ms=0
        )
        self._thread = None
        self._stop = threading.Event()

    def run_forever(self, handler):
        log.info(f"[lamp-ai] Kafka consume start: topic={self.topic}")
        for msg in self.consumer:
            if self._stop.is_set():
                break
            try:
                handler(msg.value)
            except Exception as e:
                log.exception("handler error: %s", e)
        log.info("[lamp-ai] Kafka consume stopped.")

    def start(self, handler):
        self._thread = threading.Thread(target=self.run_forever, args=(handler,), daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        try:
            self.consumer.close(timeout=1.0)
        except Exception:
            pass
