# 🚀 Enhanced lamp_AI 실행 가이드

## 🎯 주요 개선사항

### ✅ 추가된 기능
- **밝기 균일도 분석**: CV, 백분위수, 공간 균일도 측정
- **세분화된 진단**: 8가지 상세 진단명 (정상, 어두움, 불균일, 핫스팟 등)
- **기술적 상세정보**: 각 진단에 대한 구체적 수치와 권장사항

## 🔧 시스템 구성

```
lamp_ai_/
├── analysis/                    # 🧠 분석 모듈
│   ├── brightness_analyzer.py   # 밝기 균일도 분석기
│   └── diagnostic_classifier.py # 진단 분류기
├── inference/                  # 🔍 AI 추론
│   ├── model_loader.py         # YOLO 모델 로더
│   └── predict.py              # 향상된 예측 로직
├── lamp_kafka/                 # 📨 Kafka 연동
├── utils/                      # 🛠️ 유틸리티
├── models/                     # 🤖 AI 모델
└── app.py                      # 🚀 메인 애플리케이션
```

### 1단계: 의존성 설치
```bash
# 가상환경 활성화 (이미 있다면 생략)
# python -m venv .venv
# .venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# 패키지 설치
pip install -r requirements.txt
```

### 2단계: 환경 설정
```bash
# .env 파일 생성 (.env.example 참고)
cp .env.example .env

# 필요시 환경변수 수정
# KAFKA_BOOTSTRAP_SERVERS=localhost:29092
# S3_BUCKET=your-bucket-name
```

### 3단계: 시스템 시작
```bash
# Docker로 Kafka + lamp_AI 실행
docker-compose up -d

# 또는 개발 모드 (Python 직접 실행)
python app.py
```

### 4단계: 시스템 테스트
```bash
# 테스트 스크립트 실행
python test_enhanced_system.py
```

## 📊 기능 테스트

### 상태 확인
```bash
# 시스템 상태
curl http://localhost:8000/health

# 기능 목록
curl http://localhost:8000/
```

### Kafka 메시지 테스트
```json
{
  "auditId": 12345,
  "inspectionId": 67890,
  "inspectionType": "LAMP_INSPECTION", 
  "collectDataPath": "https://your-image-url.jpg",
  "traceId": "test-001"
}
```

## 🎯 진단 결과 예시

### 정상 케이스
```json
{
  "label": "headlight_on",
  "diagnosis": "NORMAL",
  "diagnosis_kr": "정상",
  "severity": "NONE",
  "technical_details": {
    "mean_brightness": 185.2,
    "cv_score": 0.18,
    "weighted_score": 0.92
  }
}
```

### 불량 케이스 
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
    "spatial_score": 0.6,
    "issues": ["변동계수 과대", "공간적 불균일"]
  }
}
```

## 🚨 알림 시스템

### Kafka 알림 토픽
- **토픽명**: `lamp-alerts`
- **메시지 형식**: JSON
- **심각도**: HIGH/MEDIUM/LOW

### 알림 예시
```json
{
  "alert_id": "LAMP_20250825_143022_67890",
  "severity": "MEDIUM",
  "diagnosis_kr": "밝기 불균일",
  "vehicle_info": "InspectionID_67890", 
  "recommendation": "렌즈 청소나 반사판 점검이 필요합니다.",
  "requires_immediate_action": true
}
```

## 🔧 커스터마이징

### 진단 기준 조정
```python
# analysis/brightness_analyzer.py
self.brightness_standards = {
    'headlight_on': {
        'target_brightness': 200,  # 목표 밝기
        'min_brightness': 150,     # 최소 밝기
        'max_brightness': 250,     # 최대 밝기
    }
}
```

### 알림 설정 변경
```python
# notification/alert_manager.py
self.severity_config = {
    'HIGH': {
        'immediate_alert': True,
        'notification_methods': ['kafka', 'console'],
        'escalation_time': 0
    }
}
```

## 🐛 문제 해결

### 모델 로드 실패
```bash
# models/best.pt 파일 확인
ls -la models/

# 권한 문제 해결
chmod 644 models/best.pt
```

### Kafka 연결 실패
```bash
# Kafka 컨테이너 상태 확인
docker-compose ps

# 로그 확인
docker-compose logs kafka
```

### OpenCV 오류
```bash
# 시스템 패키지 설치 (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install libgl1-mesa-glx libglib2.0-0

# 또는 headless 버전 재설치
pip uninstall opencv-python-headless
pip install opencv-python-headless
```

## 📈 성능 모니터링

### 로그 확인
```bash
# Docker 로그
docker-compose logs -f lamp_ai

# 특정 시간대 로그
docker-compose logs --since "2025-08-25T14:00:00" lamp_ai
```

### 메트릭 확인
- 처리 시간: 로그에서 inference 시간 확인
- 메모리 사용량: `docker stats lamp_ai`
- Kafka 처리량: 토픽 메시지 수 확인

## 🎉 구현 완료 체크리스트

- [x] YOLO 기반 on/off 감지
- [x] 밝기 균일도 분석 (CV, 백분위수, 공간)
- [x] 세분화된 진단명 (8가지 카테고리)
- [x] 실시간 작업자 알림 시스템
- [x] Kafka 연동 및 메시지 처리
- [x] S3 이미지 다운로드 지원
- [x] Docker 컨테이너화
- [x] 상태 모니터링 API
- [x] 예외 처리 및 안정성
- [x] 한글 진단명 및 권장사항
