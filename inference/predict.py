# inference/predict.py
import os, tempfile
import numpy as np
from PIL import Image
from utils.s3 import download_s3_to_path

MODE = os.getenv("LAMP_MODE", "dummy")  # dummy | brightness

def run_inference(src: str) -> dict:
    # s3/로컬 분기 그대로 유지
    if src.startswith("s3://"):
        suffix = os.path.splitext(src)[1] or ".jpg"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix); tmp.close()
        download_s3_to_path(src, tmp.name)
        try:
            return _infer_on_path(tmp.name)
        finally:
            os.unlink(tmp.name)
    else:
        return _infer_on_path(src)

def _infer_on_path(local_path: str) -> dict:
    if MODE == "dummy":
        # 완전 고정 더미
        return {"model": "dummy_v1", "label": "headlight_on", "prob": 0.98}

    if MODE == "brightness":
        # 간단한 밝기 휴리스틱(밝으면 on, 어두우면 off)
        im = Image.open(local_path).convert("L")
        mean = float(np.array(im).mean()) / 255.0
        label = "headlight_on" if mean > 0.5 else "headlight_off"
        # 확률은 밝기와 임계값(0.5) 거리로 가볍게 계산
        prob = max(0.5, min(0.99, 0.5 + abs(mean - 0.5)))
        return {"model": "brightness_v0", "label": label, "prob": prob}

    # 기본 fallback
    return {"model": "dummy_v1", "label": "headlight_on", "prob": 0.98}
