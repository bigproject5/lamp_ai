import os
import numpy as np
from PIL import Image

# Check if ultralytics is available
try:
    from ultralytics import YOLO
    ULTRALYTICS_AVAILABLE = True
except ImportError:
    ULTRALYTICS_AVAILABLE = False

class LampModel:
    def __init__(self, weight_path: str | None = "models/best.pt"):
        self.mode = "heuristic" # Default mode is heuristic
        self.model = None

        # Check if the library is installed and the weight file exists
        if ULTRALYTICS_AVAILABLE and weight_path and os.path.exists(weight_path):
            try:
                # Load the YOLO model from the checkpoint file
                self.model = YOLO(weight_path)
                self.mode = "yolo"
                print(f"[LampModel] YOLO model loaded successfully from {weight_path}")
            except Exception as e:
                print(f"[LampModel] YOLO weight load failed -> heuristic fallback: {e}")
        else:
            if not ULTRALYTICS_AVAILABLE:
                print("[LampModel] ultralytics library not found. Falling back to heuristic.")
            if not (weight_path and os.path.exists(weight_path)):
                print(f"[LampModel] Weight file not found at {weight_path}. Falling back to heuristic.")

    def predict(self, img: Image.Image):
        # If the YOLO model is loaded, use it for prediction
        if self.mode == "yolo" and self.model is not None:
            try:
                # Perform inference
                results = self.model.predict(img, verbose=False)
                
                detections = results[0].boxes
                if len(detections) > 0:
                    # Lamp is ON
                    label = "NORMAL"
                    # Use the confidence of the highest-scoring detection as the score
                    score = float(detections.conf.max())
                else:
                    # Lamp is OFF
                    label = "ABNORMAL"
                    score = 1.0 # Confidence is high that it's off
                
                return {"label": label, "score": round(score, 4), "model": "yolo_v8"}
            except Exception as e:
                print(f"[LampModel] YOLO prediction failed: {e}")
                # Fallback to a default error state if prediction fails
                return {"label": "ABNORMAL", "score": 0.0, "model": "yolo_error"}

        # Fallback to the brightness heuristic if the YOLO model isn't available
        gray = np.asarray(img.convert("L")).astype("float32") / 255.0
        brightness = float(gray.mean())
        label = "NORMAL" if brightness >= 0.6 else "ABNORMAL"
        score = brightness if label == "NORMAL" else round(1.0 - brightness, 4)
        return {"label": label, "score": round(float(score), 4), "model": "heuristic"}
