import asyncio
import json
import io
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# --- AI Model & Image Processing Imports ---
from ultralytics import YOLO
from PIL import Image
import requests

# --- DTO 정의 ---
class TestStartedEventDTO(BaseModel):
    auditId: int
    inspectionId: int
    inspectionType: str
    collectDataPath: str
    traceId: Optional[str] = None
    model: Optional[str] = None
    lineCode: Optional[str] = None
    requestedAt: Optional[str] = None

# --- Kafka 설정 ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TEST_STARTED_TOPIC = "test-started"
DIAGNOSIS_RESULT_TOPIC = "ai-diagnosis-completed"

app = FastAPI()
producer = None

# --- AI 모델 관련 전역 변수 ---
model = None

# --- FastAPI 생명주기 관리 ---
@app.on_event("startup")
async def startup_event():
    global producer, model
    print("Starting up...")
    
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    await producer.start()
    
    asyncio.create_task(consume_test_started())

    try:
        model = YOLO('models/best.pt')
        print("Ultralytics YOLO model 'models/best.pt' loaded successfully.")
    except Exception as e:
        print(f"Error loading YOLO model: {e}")
        model = None

@app.on_event("shutdown")
async def shutdown_event():
    print("Shutting down...")
    await producer.stop()

# --- 실제 AI 추론 및 결과 전송 함수 ---
async def run_inference_and_send_result(evt: TestStartedEventDTO):
    if model is None:
        print("Model is not loaded. Skipping inference.")
        return

    print(f"Running inference for inspectionId: {evt.inspectionId} on image: {evt.collectDataPath}")

    try:
        response = requests.get(evt.collectDataPath)
        response.raise_for_status()
        image = Image.open(io.BytesIO(response.content)).convert("RGB")

        results = model(image)
        names = model.names

        # BUG FIX: Detection 모델의 결과를 올바르게 해석
        # 탐지된 객체의 개수(len(results[0].boxes))를 확인 (기존의 results[0].probs는 Classification 모델용)
        if len(results[0].boxes) == 0:
            print(f"Warning: Model returned no detections for inspectionId {evt.inspectionId}.")
            is_defect = True  # 판단 불가 -> 비정상으로 간주
            result_label = "undetermined"
            result_score = 0.0
            message = "Model could not detect any lamp object."
        else:
            # 하나라도 'off' 상태인 램프가 감지되면 결함으로 판단
            is_defect = False
            highest_off_score = 0
            final_label = ""

            for box in results[0].boxes:
                label = names[int(box.cls)]
                score = float(box.conf)
                if 'off' in label.lower():
                    is_defect = True
                    # 가장 확신도 높은 'off' 결과를 최종 결과로 선택
                    if score > highest_off_score:
                        highest_off_score = score
                        final_label = label
            
            # 결함('off')이 발견되지 않았다면, 가장 확신도 높은 'on' 결과를 선택
            if not is_defect:
                best_box = results[0].boxes[0] # 가장 확신도 높은 객체가 맨 앞에 옴
                result_label = names[int(best_box.cls)]
                result_score = float(best_box.conf)
            else:
                result_label = final_label
                result_score = highest_off_score
            
            message = "ok"

        model_used = "yolo:models/best.pt"

    except Exception as e:
        print(f"Error during inference for inspectionId {evt.inspectionId}: {e}")
        is_defect = True
        result_label = "N/A"
        result_score = 0.0
        model_used = "yolo:heuristic"
        message = f"Inference failed: {e}"

    diagnosis_result_content = {
        "label": result_label,
        "score": result_score,
        "model": model_used,
        "message": message,
    }

    payload = {
        "auditId": evt.auditId,
        "inspectionId": evt.inspectionId,
        "inspectionType": evt.inspectionType,
        "isDefect": is_defect,
        "collectDataPath": evt.collectDataPath,
        "resultDataPath": f"s3://aivle-5/results/{evt.inspectionId}/result.jpg",
        "diagnosisResult": json.dumps(diagnosis_result_content)
    }
    
    headers = [('__TypeId__', b'aivle.project.vehicleAudit.event.AiDiagnosisCompletedEventDTO')]

    print(f"Sending data to Kafka topic '{DIAGNOSIS_RESULT_TOPIC}': {json.dumps(payload)}")
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
                asyncio.create_task(run_inference_and_send_result(evt))
            except Exception as e:
                print(f"Error processing message from {TEST_STARTED_TOPIC}: {e}")
    finally:
        await consumer.stop()

# --- 상태 확인용 엔드포인트 ---
@app.get("/")
def read_root():
    return {"status": "lamp_ai is running with a real YOLO model"}
