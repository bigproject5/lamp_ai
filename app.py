import asyncio
import json
import io
import os
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
# 로컬에서 uvicorn으로 실행 시 환경변수가 없으면 localhost:9092를 사용합니다.
# Docker로 실행 시 docker-compose.yml의 환경변수(kafka:9092)가 이 값을 덮어씁니다.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TEST_STARTED_TOPIC = os.getenv("KAFKA_SOURCE_TOPIC", "test-started")
DIAGNOSIS_RESULT_TOPIC = os.getenv("KAFKA_RESULT_TOPIC", "ai-diagnosis-completed")

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

    # [DB 제약조건 해결] diagnosisResult를 DB 컬럼(VARCHAR(255))에 맞게 줄입니다.
    # diagnosis_result_content가 JSON 문자열로 변환될 때 255자를 넘지 않도록 보장합니다.
    DB_COLUMN_MAX_SIZE = 255

    # 'message' 필드를 제외한 나머지 JSON 구조의 길이를 계산합니다.
    temp_content = diagnosis_result_content.copy()
    temp_content["message"] = ""
    # 한글 등 non-ASCII 문자 길이를 정확히 계산하기 위해 ensure_ascii=False 사용
    overhead_length = len(json.dumps(temp_content, ensure_ascii=False))

    # 'message' 필드에 할당 가능한 최대 길이를 계산합니다. (말줄임표 "..." 3자리 확보)
    max_message_length = DB_COLUMN_MAX_SIZE - overhead_length - 3

    # 'message'가 최대 길이를 초과하면 자르고 말줄임표를 추가합니다.
    original_message = diagnosis_result_content.get("message", "")
    if len(original_message) > max_message_length and max_message_length > 0:
        diagnosis_result_content["message"] = original_message[:max_message_length] + "..."


    payload = {
        "auditId": evt.auditId,
        "inspectionId": evt.inspectionId,
        "inspectionType": evt.inspectionType,
        "isDefect": is_defect,
        "collectDataPath": evt.collectDataPath,
        "resultDataPath": f"s3://{os.getenv('S3_BUCKET', 'aivle-5')}/results/{evt.inspectionId}/result.jpg",
        "diagnosisResult": diagnosis_result_content
    }
    
    headers = [('__TypeId__', b'aivle.project.vehicleAudit.event.AiDiagnosisCompletedEventDTO')]

    print(f"Sending data to Kafka topic '{DIAGNOSIS_RESULT_TOPIC}': {json.dumps(payload)}")
    await producer.send_and_wait(DIAGNOSIS_RESULT_TOPIC, value=payload, headers=headers)

# --- Kafka 컨슈머 ---
async def consume_test_started():
    consumer = AIOKafkaConsumer(
        TEST_STARTED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=os.getenv("KAFKA_GROUP_ID", "lamp-ai-group"),
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
