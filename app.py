import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# --- DTO 정의 (이전과 동일) ---
class TestStartedEventDTO(BaseModel):
    traceId: Optional[str] = None
    auditId: int
    inspectionId: int
    inspectionType: str
    model: Optional[str] = None
    lineCode: Optional[str] = None
    collectDataPath: str
    requestedAt: Optional[str] = None

class WorkerTaskCompletedEventDTO(BaseModel):
    traceId: str
    auditId: int
    inspectionId: int
    inspectionType: str
    workerId: str
    workerName: str
    resolve: str
    startedAt: str
    endedAt: str
    durationSec: Optional[int] = None

class DiagnosisResult(BaseModel):
    traceId: str
    auditId: int
    inspectionId: int
    inspectionType: str
    status: str  # COMPLETED | FAILED
    model: str
    result: dict
    message: str
    completedAt: str

# --- Kafka 설정 ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TEST_STARTED_TOPIC = "test-started"
WORKER_COMPLETED_TOPIC = "worker-task-completed"
DIAGNOSIS_RESULT_TOPIC = "ai-diagnosis-completed"

app = FastAPI()
producer = None

# --- Kafka 프로듀서/컨슈머 생명주기 관리 ---
@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    await producer.start()
    asyncio.create_task(consume_test_started())
    asyncio.create_task(consume_worker_completed())

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()

# --- 결과 전송 함수 (Kafka) ---
async def send_diagnosis_result(result_dto: DiagnosisResult):
    print(f"Sending DiagnosisResult to Kafka topic '{DIAGNOSIS_RESULT_TOPIC}': {result_dto.json()}")
    await producer.send_and_wait(DIAGNOSIS_RESULT_TOPIC, result_dto.dict())

# --- 모델 추론 시뮬레이션 및 결과 전송 ---
async def simulate_inference_and_send_result(evt: TestStartedEventDTO):
    print(f"Simulating inference for inspectionId: {evt.inspectionId}")
    await asyncio.sleep(3)  # 3초간 추론 시뮬레이션

    # AI 추론 결과 시뮬레이션
    if not evt.collectDataPath or "invalid" in evt.collectDataPath:
        is_defect = True
        result_label = "N/A"
        result_score = 0.0
        model_used = "torch:heuristic"
        message = "Invalid collectDataPath or file access failed."
    else:
        is_defect = True
        result_label = "headlight_on"
        result_score = 0.982
        model_used = "torch:best.pt"
        message = "ok"

    # vehicleAudit의 DTO 형식에 맞춘 결과 데이터 (String으로 변환)
    diagnosis_result_content = {
        "label": result_label,
        "score": result_score,
        "model": model_used,
        "message": message,
        "extra": {"brightness": 187.4}
    }

    # Kafka로 전송할 최종 페이로드 (AiDiagnosisCompletedEventDTO 와 일치)
    payload = {
        "auditId": evt.auditId,
        "inspectionId": evt.inspectionId,
        "inspectionType": evt.inspectionType,
        "isDefect": is_defect,
        "collectDataPath": evt.collectDataPath,
        "resultDataPath": f"s3://aivle-5/results/{evt.inspectionId}/result.jpg",
        "diagnosisResult": json.dumps(diagnosis_result_content)
    }

    # Spring Kafka Deserializer가 타입을 인식할 수 있도록 __TypeId__ 헤더 추가
    headers = [
        ('__TypeId__', b'aivle.project.vehicleAudit.event.AiDiagnosisCompletedEventDTO')
    ]

    print(f"Sending data to Kafka topic '{DIAGNOSIS_RESULT_TOPIC}': {json.dumps(payload)}")
    # producer.send_and_wait() 호출 시 value와 headers를 명시적으로 전달
    await producer.send_and_wait(DIAGNOSIS_RESULT_TOPIC, value=payload, headers=headers)


# --- Kafka 컨슈머 ---
async def consume_test_started():
    consumer = AIOKafkaConsumer(
        TEST_STARTED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="lamp-ai-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"Consumed from {msg.topic}: {msg.value}")
            try:
                evt = TestStartedEventDTO(**msg.value)
                asyncio.create_task(simulate_inference_and_send_result(evt))
            except Exception as e:
                print(f"Error processing message from {TEST_STARTED_TOPIC}: {e}")
    finally:
        await consumer.stop()

async def consume_worker_completed():
    consumer = AIOKafkaConsumer(
        WORKER_COMPLETED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="lamp-ai-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"Consumed from {msg.topic}: {msg.value}")
            try:
                evt = WorkerTaskCompletedEventDTO(**msg.value)
                # 메타데이터 병합 저장 또는 로그 기록
                print(f"Worker {evt.workerName} completed task for inspection {evt.inspectionId}. Resolve: {evt.resolve}")
            except Exception as e:
                print(f"Error processing message from {WORKER_COMPLETED_TOPIC}: {e}")
    finally:
        await consumer.stop()

# --- (선택) 상태 확인용 엔드포인트 ---
@app.get("/")
def read_root():
    return {"status": "lamp_ai_patched is running with Kafka consumers"}
