@echo off
echo 🚀 lamp_AI 빠른 도커 빌드 시작...

echo 🧹 기존 컨테이너 정리 중...
docker-compose down --remove-orphans
docker system prune -f

echo 🔨 이미지 빌드 중... (3-5분 소요 예정)
docker-compose build --no-cache lamp_ai

echo 🚀 서비스 시작 중...
docker-compose up -d

echo ✅ 빌드 완료! 서비스 상태 확인 중...
timeout 10
docker-compose ps

echo 🔍 API 테스트...
curl http://localhost:8000/health

echo 🎉 lamp_AI 도커 빌드 완료!
echo 📝 브라우저에서 http://localhost:8000/docs 확인하세요.
pause