import os
from kafka import KafkaProducer
import json, atexit

class VehicleAuditProducer:
    def __init__(self, topic=None, bootstrap_servers=None):
        self.topic = topic or os.getenv("KAFKA_TOPIC", "ai-diagnosis-completed")
        bs = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        # 문자열/리스트 둘 다 허용
        if isinstance(bs, str):
            bs = [bs]

        self.producer = KafkaProducer(
            bootstrap_servers=bs,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            linger_ms=10,
        )
        atexit.register(self.close)

    def close(self):
        try:
            self.producer.flush(5)
            self.producer.close()
        except Exception:
            pass

    def send_ai_diagnosis_completed(self, payload: dict):
        key_val = payload.get("inspectionId")
        key_bytes = (str(key_val).encode() if key_val is not None else None)

        self.producer.send(
            self.topic,
            key=key_bytes,
            value=payload,
            headers=[
                ("__TypeId__", b"aivle.project.vehicleAudit.event.AiDiagnosisCompletedEventDTO"),
                ("contentType", b"application/json"),
            ],
        )
