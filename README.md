# lamp_ai

차량 검사 공정에서 촬영된 전조등/후미등 이미지를 분석해 점등 상태와 밝기 이상을 자동으로 진단하는 AI 서비스입니다. YOLO 기반 점등 여부 탐지와 밝기 균일도 분석을 결합해 단순 on/off 판정을 넘어 불균일, 핫스팟 등 8가지 세부 진단명과 조치 권장사항을 제공합니다.

## 주요 기능

- YOLO 모델 기반 전조등/후미등 점등 여부 탐지
- 밝기 균일도 분석(변동계수, 백분위수, 공간 분포)을 통한 세분화된 진단
- 8가지 진단 카테고리와 심각도(HIGH/MEDIUM/LOW)별 조치 권장사항 산출
- Kafka 이벤트 기반 비동기 검사 파이프라인 연동
- 검사 이미지 URL을 받아 다운로드하고 분석 결과 이미지를 S3에 업로드
- FastAPI 기반 상태 확인 API 제공

## 시스템 아키텍처

검사 시스템이 Kafka `test-started` 토픽에 검사 요청 이벤트를 발행하면, lamp_ai가 이를 소비해 이벤트에 담긴 이미지 URL로 검사 이미지를 내려받고 YOLO 추론과 밝기 분석을 수행합니다. 진단 결과는 `ai-diagnosis-completed` 토픽으로 발행되며, 결과 이미지는 S3에 업로드됩니다.

```
검사 시스템 → Kafka(test-started) → lamp_ai(이미지 다운로드 → YOLO 추론 → 밝기 분석) → Kafka(ai-diagnosis-completed)
                                                                    ↓
                                                            S3 (결과 이미지 업로드)
```

## 기술 스택

- Backend: FastAPI, Uvicorn
- AI/Vision: Ultralytics YOLO, OpenCV, Pillow
- Messaging: Apache Kafka (aiokafka)
- Storage: AWS S3 (boto3)
- Infra: Docker, Docker Compose, Kubernetes

## 폴더 구조

```
lamp_ai/
├── analysis/                    # 밝기 균일도 분석, 진단 분류 로직 (app.py에서 사용)
│   ├── brightness_analyzer.py
│   └── diagnostic_classifier.py
├── inference/                   # YOLO 모델 로더/추론 모듈 (현재 app.py 미사용)
│   ├── model_loader.py
│   └── predict.py
├── lamp_kafka/                  # Kafka consumer/producer 모듈 (현재 app.py 미사용)
├── models/                      # 학습된 YOLO 모델(.pt)
├── utils/                       # S3 업로드 등 공통 유틸리티 (app.py에서 사용)
├── kubernetes/                  # K8s 배포 매니페스트
├── app.py                       # FastAPI 앱, YOLO 로딩과 Kafka 이벤트 처리를 직접 구현
├── settings.py                  # 환경 변수 로드 (현재 app.py 미사용)
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

YOLO 모델 로딩과 Kafka consumer/producer 로직은 `app.py`에 직접 구현되어 있으며, `inference/`, `lamp_kafka/`, `settings.py`는 아직 `app.py`에 연결되지 않은 별도 모듈입니다.

## 설치 및 실행

### 1. 프로젝트 클론

```bash
git clone https://github.com/bigproject5/lamp_ai.git
cd lamp_ai
```

### 2. 환경 설정 파일 생성

macOS / Linux

```bash
cp .env.example .env
```

Windows

```bash
copy .env.example .env
```

### 3. 환경 변수 입력

`.env` 파일을 열어 AWS 자격 증명을 채워 넣습니다. 나머지 값은 로컬 개발 기준으로 기본값이 설정되어 있어 그대로 사용해도 됩니다.

```
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
AWS_REGION=YOUR_AWS_REGION
S3_BUCKET=YOUR_S3_BUCKET_NAME
```

### 4. 실행

```bash
docker-compose up
```

Zookeeper, Kafka, lamp_ai 컨테이너가 함께 기동되며 API 서버는 `http://localhost:8000`에서 접근할 수 있습니다.

## 동작 확인

```bash
# 서비스 상태 확인
curl http://localhost:8000/
```

Kafka `test-started` 토픽에 아래 형식의 메시지를 발행하면 진단 파이프라인이 동작합니다.

```json
{
  "auditId": 12345,
  "inspectionId": 67890,
  "inspectionType": "LAMP_INSPECTION",
  "collectDataPath": "https://your-image-url.jpg",
  "traceId": "test-001"
}
```

진단이 완료되면 `ai-diagnosis-completed` 토픽으로 아래와 같은 결과가 발행됩니다.

```json
{
  "label": "headlight_on_uneven",
  "diagnosis": "UNEVEN_BRIGHTNESS",
  "diagnosis_kr": "밝기 불균일",
  "severity": "LOW",
  "recommendation": "렌즈 청소나 반사판 점검이 필요합니다.",
  "technical_details": {
    "mean_brightness": 165.8,
    "cv_score": 0.34,
    "spatial_score": 0.6
  }
}
```

## 문제 해결

모델 로드에 실패하면 `models/` 디렉터리에 `best.pt` 파일이 있는지 확인합니다. Kafka 연결이 되지 않으면 `docker-compose logs kafka`로 컨테이너 상태를 확인합니다. OpenCV 관련 오류가 발생하면 `libgl1-mesa-glx`, `libglib2.0-0` 시스템 패키지 설치가 필요할 수 있습니다.
