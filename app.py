import asyncio
import json
import io
import os
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import numpy as np
import cv2
from datetime import datetime

# --- AI Model & Image Processing Imports ---
from ultralytics import YOLO
from PIL import Image, ImageDraw, ImageFont
import requests

# --- Analysis Module Imports ---
from analysis.brightness_analyzer import TechnicalBrightnessAnalyzer
from analysis.diagnostic_classifier import DiagnosticClassifier

# --- S3 Utils ---
from utils.s3 import upload_bytes_to_s3, s3_uri_to_https_url

# --- 진단 결과를 한 문장으로 변환하는 함수 ---
def generate_diagnosis_sentence(detailed_diagnosis, technical_details, result_label, result_score):
    """진단 결과를 사람이 이해하기 쉬운 한 문장으로 변환"""

    # 램프 타입 결정
    lamp_type = "헤드라이트" if "headlight" in result_label.lower() else "테일라이트"

    # YOLO 기본 분류 결과 설명
    yolo_classification = generate_yolo_classification_text(result_label, lamp_type, result_score)

    # Analysis 모듈 결과와 결합하여 최종 문장 생성
    if detailed_diagnosis['diagnosis'] == 'UNKNOWN':
        return f"모델이 {lamp_type} 객체를 감지하지 못해 상태를 판단할 수 없습니다"

    # YOLO 결과와 Analysis 결과 결합
    analysis_result = generate_analysis_result_text(detailed_diagnosis, technical_details)

    # 최종 문장 조합
    if detailed_diagnosis['diagnosis'] == 'OFF':
        final_sentence = f"모델이 {lamp_type}를 소등 상태로 분류하여 결함 판별됨"
    elif detailed_diagnosis['diagnosis'] == 'NORMAL':
        final_sentence = f"모델이 {lamp_type}를 점등 상태로 분류하였고, {analysis_result}"
    else:
        final_sentence = f"모델이 {lamp_type}를 점등 상태로 분류했으나, {analysis_result}"

    # 추가 정보 생성 (우선순위 순서로 배치)
    additional_info = []

    # 밝기 정보
    if technical_details:
        brightness = technical_details.get('mean_brightness')
        if brightness:
            additional_info.append(f"평균밝기: {brightness:.0f}")

        # 균일도 상세 수치들
        cv_score = technical_details.get('cv_score')
        if cv_score is not None:
            additional_info.append(f"CV: {cv_score:.2f}")

        spatial_score = technical_details.get('spatial_score')
        if spatial_score is not None:
            spatial_percent = int(spatial_score * 100)
            additional_info.append(f"공간균일도: {spatial_percent}%")

        # 극값 비율 (문제가 있는 경우만)
        percentile_ratio = technical_details.get('percentile_ratio')
        if percentile_ratio is not None and percentile_ratio > 3.0:
            additional_info.append(f"극값비: {percentile_ratio:.1f}")

        # 품질 등급
        grade = technical_details.get('cv_grade', '').replace('급 (보통)', '급').replace('급 (우수)', '급').replace('급 (양호)', '급').replace('급 (불량)', '급')
        if grade and '급' in grade:
            additional_info.append(f"품질: {grade[:2]}")

    # 신뢰도 (80% 이상일 때만)
    if result_score >= 0.8:
        additional_info.append(f"확신도: {result_score*100:.0f}%")

    # 최종 문장 조합
    if additional_info:
        final_sentence = f"{final_sentence} ({', '.join(additional_info)})"

    # 255자 제한 적용
    if len(final_sentence) > 255:
        # 추가 정보를 하나씩 제거하면서 길이 조정
        while len(final_sentence) > 255 and additional_info:
            additional_info.pop()
            if additional_info:
                final_sentence = f"{final_sentence.split(' (')[0]} ({', '.join(additional_info)})"
            else:
                final_sentence = final_sentence.split(' (')[0]

        # 그래도 길면 기본 문장도 자르기
        if len(final_sentence) > 255:
            final_sentence = final_sentence[:252] + "..."

    return final_sentence

def generate_yolo_classification_text(result_label, lamp_type, result_score):
    """YOLO 분류 결과를 텍스트로 변환"""
    if 'off' in result_label.lower():
        return f"모델이 {lamp_type}를 소등 상태로 분류"
    elif 'on' in result_label.lower():
        return f"모델이 {lamp_type}를 점등 상태로 분류"
    else:
        return f"모델이 {lamp_type}를 {result_label}로 분류"

def generate_analysis_result_text(detailed_diagnosis, technical_details):
    """Analysis 모듈 결과를 텍스트로 변환"""
    if detailed_diagnosis['diagnosis'] == 'NORMAL':
        uniformity_detail = generate_uniformity_details(technical_details, detailed_diagnosis['diagnosis'])
        if uniformity_detail:
            return f"균일도 분석에서도 {uniformity_detail}"
        else:
            return "균일도 분석에서도 정상으로 판별됨"

    # 문제가 있는 경우
    diagnosis_kr = detailed_diagnosis.get('diagnosis_kr', '문제')
    uniformity_detail = generate_uniformity_details(technical_details, detailed_diagnosis['diagnosis'])

    if uniformity_detail:
        return f"{uniformity_detail}으로 {diagnosis_kr} 판별됨"
    else:
        return f"분석 결과 {diagnosis_kr}로 판별됨"

def generate_uniformity_details(technical_details, diagnosis):
    """균일도에 대한 자연스러운 설명 생성"""
    if not technical_details:
        return None

    # 균일도 관련 문제가 있는 경우에만 설명 제공
    if diagnosis not in ['UNEVEN_BRIGHTNESS', 'DIM_PARTIAL', 'HOTSPOT', 'NORMAL']:
        return None

    explanation_parts = []

    # 변동계수 분석
    cv_score = technical_details.get('cv_score')
    if cv_score is not None:
        if cv_score > 0.35:
            explanation_parts.append("변동계수가 크게 기준을 초과하여")
        elif cv_score > 0.25:
            explanation_parts.append("변동계수가 기준을 초과하여")
        elif cv_score <= 0.15:
            explanation_parts.append("변동계수가 우수한 수준으로")

    # 공간적 균일도 분석
    spatial_score = technical_details.get('spatial_score')
    if spatial_score is not None:
        problem_percent = int((1 - spatial_score) * 100)
        if problem_percent > 30:
            explanation_parts.append(f"다수 영역에서 밝기 편차가 발견되어")
        elif problem_percent > 10:
            explanation_parts.append(f"일부 영역에서 밝기 편차가 발견되어")
        elif problem_percent <= 5:
            explanation_parts.append("공간적으로 균일하여")

    # 극값 비율 분석 (심각한 경우만)
    percentile_ratio = technical_details.get('percentile_ratio')
    if percentile_ratio is not None and percentile_ratio > 4.0:
        explanation_parts.append("밝기 극값 차이가 과도하여")

    # 설명 조합
    if explanation_parts:
        if diagnosis == 'NORMAL':
            return " ".join(explanation_parts) + " 균일함"
        else:
            return " ".join(explanation_parts) + " 불균일로 판별됨"

    # 기본 설명
    if diagnosis == 'NORMAL':
        return "균일도 기준 내로 양호함"
    else:
        return "균일도 기준 미달로 문제 있음"

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

    # 'message'에 할당 가능한 최대 길이를 계산합니다. (말줄임표 "..." 3자리 확보)
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

    # --- 결과 이미지 생성 및 업로드 함수 ---
    def create_result_image(image, results, detailed_diagnosis, result_label, result_score):
        """YOLO 결과와 진단 정보를 시각화한 결과 이미지 생성"""
        # PIL Image를 OpenCV 형식으로 변환
        img_cv = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)

        # 감지된 박스 그리기
        if len(results[0].boxes) > 0:
            for box in results[0].boxes:
                # 박스 좌표 추출
                x1, y1, x2, y2 = map(int, box.xyxy[0].cpu().numpy())
                confidence = float(box.conf)
                class_id = int(box.cls)
                class_name = results[0].names[class_id]

                # 박스 색상 결정 (정상: 녹색, 결함: 빨간색)
                color = (0, 255, 0) if detailed_diagnosis['diagnosis'] == 'NORMAL' else (0, 0, 255)

                # 박스 그리기
                cv2.rectangle(img_cv, (x1, y1), (x2, y2), color, 2)

                # 라벨 텍스트 준비
                label_text = f"{class_name}: {confidence:.2f}"

                # 라벨 배경 그리기
                (text_width, text_height), baseline = cv2.getTextSize(label_text, cv2.FONT_HERSHEY_SIMPLEX, 0.6, 2)
                cv2.rectangle(img_cv, (x1, y1 - text_height - 10), (x1 + text_width, y1), color, -1)

                # 라벨 텍스트 그리기
                cv2.putText(img_cv, label_text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)

        # 진단 결과 텍스트 추가
        diagnosis_text = f"진단: {detailed_diagnosis['diagnosis_kr']} ({detailed_diagnosis['severity']})"
        cv2.putText(img_cv, diagnosis_text, (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2)

        # 타임스탬프 추가
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cv2.putText(img_cv, timestamp, (10, img_cv.shape[0] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)

        # OpenCV 이미지를 PIL Image로 변환
        result_image = Image.fromarray(cv2.cvtColor(img_cv, cv2.COLOR_BGR2RGB))

        return result_image

    async def upload_result_image(result_image, inspection_id):
        """결과 이미지를 S3에 업로드"""
        try:
            # 이미지를 바이트로 변환
            img_byte_arr = io.BytesIO()
            result_image.save(img_byte_arr, format='JPEG', quality=90)
            img_byte_arr = img_byte_arr.getvalue()

            # S3 경로 생성
            s3_path = f"s3://{os.getenv('S3_BUCKET', 'aivle-5')}/results/{inspection_id}/result.jpg"

            # S3에 업로드 (이제 HTTPS URL 반환)
            uploaded_path = upload_bytes_to_s3(img_byte_arr, s3_path, content_type="image/jpeg")
            print(f"Result image uploaded to: {uploaded_path}")

            return uploaded_path
        except Exception as e:
            print(f"Failed to upload result image: {e}")
            # 업로드 실패 시에도 HTTPS URL 형식으로 기본 경로 반환
            s3_fallback_path = f"s3://{os.getenv('S3_BUCKET', 'aivle-5')}/results/{inspection_id}/result.jpg"
            return s3_uri_to_https_url(s3_fallback_path)

    # 결과 이미지 생성
    result_image = create_result_image(image, results, detailed_diagnosis, result_label, result_score)

    # 결과 이미지 S3 업로드
    result_data_path = await upload_result_image(result_image, evt.inspectionId)

    payload = {
        "auditId": evt.auditId,
        "inspectionId": evt.inspectionId,
        "inspectionType": evt.inspectionType,
        "isDefect": is_defect,
        "collectDataPath": evt.collectDataPath,
        "resultDataPath": result_data_path,
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
