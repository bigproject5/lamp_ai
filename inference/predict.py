# inference/predict.py
import os, tempfile
from PIL import Image
from utils.s3 import download_s3_to_path  # 이미 있다면 그대로 사용

# 👉 실제 추론은 "로컬 경로"만 받도록 묶기
def _infer_on_path(local_path: str) -> dict:
    # TODO: 여기에 기존 추론 로직 사용
    # ex) img = Image.open(local_path); model.predict(img) ...
    # 임시 더미 반환 형태(프론트/백 연동용 스키마 유지)
    with Image.open(local_path) as im:
        im.load()
    return {"model": "heuristic", "label": "headlight_on", "prob": 0.98}

# 👉 외부에 노출되는 API: s3도, 로컬도 모두 지원
def run_inference(src: str) -> dict:
    # src가 s3://면 다운로드 후 로컬 경로로 추론
    if src.startswith("s3://"):
        suffix = os.path.splitext(src)[1] or ".jpg"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.close()
        download_s3_to_path(src, tmp.name)
        try:
            return _infer_on_path(tmp.name)
        finally:
            os.unlink(tmp.name)
    else:
        # 로컬 파일 경로 그대로 처리 (업로드 엔드포인트가 여기로 옴)
        return _infer_on_path(src)
