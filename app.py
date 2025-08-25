import asyncio
import json
import io
import os
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import numpy as np

# --- AI Model & Image Processing Imports ---
from ultralytics import YOLO
from PIL import Image
import requests

# --- Analysis Module Imports ---
from analysis.brightness_analyzer import TechnicalBrightnessAnalyzer
from analysis.diagnostic_classifier import DiagnosticClassifier

# --- 진단 결과를 한 문장으로 변환하는 함수 ---
def generate_diagnosis_sentence(detailed_diagnosis, technical_details, result_label, result_score):
    """진단 결과를 사람이 이해하기 쉬운 한 문장으로 변환"""

    # 램프 타입 결정
    lamp_type = "헤드라이트" if "headlight" in result_label.lower() else "테일라이트"

    # 진단별 문장 템플릿
    templates = {
        'NORMAL': f"{lamp_type}가 정상적으로 점등되어 있습니다",
        'OFF': f"{lamp_type}가 완전히 꺼져 있어 즉시 점검이 필요합니다",
        'DIM_OVERALL': f"{lamp_type}가 점등되어 있으나 전체적으로 어두워 전구 교체를 권장합니다",
        'DIM_PARTIAL': f"{lamp_type}가 점등되어 있으나 일부가 어두워 내부 점검이 필요합니다",
        'UNEVEN_BRIGHTNESS': f"{lamp_type} 밝기가 불균일하여 렌즈 청소나 반사판 점검이 필요합니다",
        'HOTSPOT': f"{lamp_type}에 과도하게 밝은 부분이 있어 렌즈나 반사판 손상을 확인하세요",
        'FLICKERING': f"{lamp_type}에서 깜빡임이 감지되어 전기 연결부나 전구를 점검하세요",
        'UNKNOWN': f"{lamp_type} 상태를 판단할 수 없어 수동 점검이 필요합니다"
    }

    # 기본 문장 생성
    base_sentence = templates.get(detailed_diagnosis['diagnosis'], f"{lamp_type} 상태에 문제가 있습니다")

    # 추가 정보 생성
    additional_info = []

    # 밝기 정보 (UNKNOWN이 아닌 경우)
    if detailed_diagnosis['diagnosis'] != 'UNKNOWN' and technical_details:
        brightness = technical_details.get('mean_brightness')
        if brightness:
            additional_info.append(f"평균밝기: {brightness:.0f}")

        # 품질 등급
        grade = technical_details.get('cv_grade', '').replace('급 (보통)', '급').replace('급 (우수)', '급').replace('급 (양호)', '급').replace('급 (불량)', '급')
        if grade and '급' in grade:
            additional_info.append(f"품질: {grade[:2]}")

    # 신뢰도 (80% 이상일 때만)
    if result_score >= 0.8:
        additional_info.append(f"확신도: {result_score*100:.0f}%")

    # 최종 문장 조합
    if additional_info:
        final_sentence = f"{base_sentence} ({', '.join(additional_info)})"
    else:
        final_sentence = base_sentence

    # 255자 제한 적용
    if len(final_sentence) > 255:
        # 추가 정보를 하나씩 제거하면서 길이 조정
        while len(final_sentence) > 255 and additional_info:
            additional_info.pop()
            if additional_info:
                final_sentence = f"{base_sentence} ({', '.join(additional_info)})"
            else:
                final_sentence = base_sentence

        # 그래도 길면 기본 문장도 자르기
        if len(final_sentence) > 255:
            final_sentence = final_sentence[:252] + "..."

    return final_sentence

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

# --- Analysis 객체 초기화 ---
brightness_analyzer = TechnicalBrightnessAnalyzer()
diagnostic_classifier = DiagnosticClassifier()

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

        # PIL Image를 numpy array로 변환 (analysis 모듈에서 사용)
        image_array = np.array(image)

        results = model(image)
        names = model.names

        # YOLO 결과 분석 및 ROI 추출
        if len(results[0].boxes) == 0:
            print(f"Warning: Model returned no detections for inspectionId {evt.inspectionId}.")
            is_defect = True
            result_label = "undetermined"
            result_score = 0.0
            model_used = "yolo:models/best.pt"
            detailed_diagnosis = {
                'diagnosis': 'UNKNOWN',
                'diagnosis_kr': '판단 불가',
                'confidence': 0.0,
                'severity': 'HIGH',
                'technical_details': {'reason': 'NO_DETECTION'},
                'brightness_analysis': None
            }
            message = "모델이 램프 객체를 감지하지 못해 판단할 수 없습니다."
        else:
            # 가장 확신도 높은 박스 선택
            best_box = results[0].boxes[0]
            result_label = names[int(best_box.cls)]
            result_score = float(best_box.conf)
            model_used = "yolo:models/best.pt"

            # ROI 추출 (YOLO 박스 좌표 사용)
            box_coords = best_box.xyxy[0].cpu().numpy()  # [x1, y1, x2, y2]
            x1, y1, x2, y2 = map(int, box_coords)
            roi = image_array[y1:y2, x1:x2]

            # YOLO 결과를 analysis 모듈 형식으로 변환
            yolo_result = {
                'class_name': result_label,
                'confidence': result_score
            }

            # 램프 타입 결정 (헤드라이트 vs 테일라이트)
            light_type = "headlight_on" if "headlight" in result_label.lower() else "taillight_on"

            # Analysis 모듈을 사용한 상세 진단
            detailed_diagnosis = diagnostic_classifier.classify_lamp_condition(roi, light_type, yolo_result)

            # 최종 결함 여부 판단 (analysis 결과 기반)
            is_defect = detailed_diagnosis['diagnosis'] != 'NORMAL'

            # 메시지 생성
            if detailed_diagnosis['diagnosis'] == 'NORMAL':
                message = f"정상: {detailed_diagnosis['diagnosis_kr']}"
            else:
                message = f"결함: {detailed_diagnosis['diagnosis_kr']} (심각도: {detailed_diagnosis['severity']})"

    except Exception as e:
        print(f"Error during inference for inspectionId {evt.inspectionId}: {e}")
        is_defect = True
        result_label = "error"
        result_score = 0.0
        model_used = "yolo:heuristic"
        detailed_diagnosis = {
            'diagnosis': 'UNKNOWN',
            'diagnosis_kr': '오류 발생',
            'confidence': 0.0,
            'severity': 'HIGH',
            'technical_details': {'reason': 'PROCESSING_ERROR', 'error': str(e)},
            'brightness_analysis': None
        }
        message = f"추론 중 오류 발생: {str(e)}"

    # diagnosisResult 구조 확장 (기존 필드 + 상세 분석 정보)
    diagnosis_result_content = {
        "label": result_label,
        "score": result_score,
        "model": model_used,
        "message": message,
        "detailed_diagnosis": detailed_diagnosis['diagnosis'],
        "diagnosis_kr": detailed_diagnosis['diagnosis_kr'],
        "confidence": detailed_diagnosis['confidence'],
        "severity": detailed_diagnosis['severity'],
        "technical_details": detailed_diagnosis['technical_details']
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

    # diagnosisResult를 사람이 이해하기 쉬운 한 문장으로 생성
    diagnosis_sentence = generate_diagnosis_sentence(
        detailed_diagnosis,
        detailed_diagnosis.get('technical_details', {}),
        result_label,
        result_score
    )

    payload = {
        "auditId": evt.auditId,
        "inspectionId": evt.inspectionId,
        "inspectionType": evt.inspectionType,
        "isDefect": is_defect,
        "collectDataPath": evt.collectDataPath,
        "resultDataPath": f"s3://{os.getenv('S3_BUCKET', 'aivle-5')}/results/{evt.inspectionId}/result.jpg",
        "diagnosisResult": diagnosis_sentence
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
