
import json
from fastapi import FastAPI, UploadFile, File, Form
from pydantic import BaseModel

from lamp_kafka.producer import VehicleAuditProducer

# Kafka Producer 초기화
# 이 코드는 앱 실행 시 단 한 번만 실행되어 카프카 서버와 연결을 맺습니다.
# 실제 운영 환경에서는 카프카 서버 주소를 설정 파일이나 환경 변수에서 가져오는 것이 좋습니다.
producer = VehicleAuditProducer(topic='ai-diagnosis-completed', bootstrap_servers='localhost:9092')

app = FastAPI()


class S3InferenceRequest(BaseModel):
    s3_uri: str
    auditId: int
    inspectionId: int
    inspectionType: str


@app.post("/inference/lamp")
def inference_s3(request: S3InferenceRequest):
    """
    S3 URI를 받아 추론을 수행하고, 결과를 카프카로 전송합니다.
    """
    # 1. 실제 모델 추론 로직 (현재는 더미 데이터 사용)
    # TODO: 전달받은 s3_uri를 사용하여 이미지를 다운로드하고 모델 추론을 수행해야 합니다.
    inference_result = {
        "model": "lamp_v1_s3",
        "label": "headlight_on",
        "prob": 0.98
    }

    # 2. vehicleAudit의 DTO 형식에 맞는 카프카 메시지 생성
    kafka_message = {
        "auditId": request.auditId,
        "inspectionId": request.inspectionId,
        "inspectionType": request.inspectionType,
        "isDefect": inference_result.get("label") != "headlight_on",  # '정상'이 아니면 결함으로 판단
        "collectDataPath": request.s3_uri,  # 원본 데이터 경로
        "resultDataPath": None,  # 결과 데이터 경로 (필요시 생성)
        "diagnosisResult": json.dumps(inference_result)  # AI의 상세 결과는 JSON 문자열로 저장
    }

    # 3. 카프카로 메시지 발행 전, 콘솔에 출력하여 확인
    print("Sending message to Kafka:", kafka_message)
    # producer.send_message(kafka_message)

    return {"message": "Inference completed. Kafka is disabled.", "kafka_message": kafka_message}


@app.post("/inference/lamp/upload")
def inference_upload(
        image_file: UploadFile = File(...),
        auditId: int = Form(...),
        inspectionId: int = Form(...),
        inspectionType: str = Form(...)
):
    """
    이미지 파일을 직접 업로드받아 추론을 수행하고, 결과를 카프카로 전송합니다.
    """
    # 1. 실제 모델 추론 로직 (현재는 더미 데이터 사용)
    # TODO: 전달받은 image_file.file 객체를 사용하여 모델 추론을 수행해야 합니다.
    inference_result = {
        "model": "lamp_v1_upload",
        "label": "headlight_off",
        "prob": 0.95
    }

    # 2. vehicleAudit의 DTO 형식에 맞는 카프카 메시지 생성
    kafka_message = {
        "auditId": auditId,
        "inspectionId": inspectionId,
        "inspectionType": inspectionType,
        "isDefect": inference_result.get("label") != "headlight_on",
        "collectDataPath": image_file.filename,  # 원본 데이터 경로로 파일명 사용
        "resultDataPath": None,
        "diagnosisResult": json.dumps(inference_result)
    }

    # 3. 카프카로 메시지 발행 전, 콘솔에 출력하여 확인
    print("Sending message to Kafka:", kafka_message)
    producer.send_ai_diagnosis_completed(kafka_message)
    return {"message": "Request processed successfully."}
    #return {"message": "Inference completed. Kafka is disabled.", "kafka_message": kafka_message}
