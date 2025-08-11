import os, numpy as np
from PIL import Image

try:
    import torch  # type: ignore
    TORCH_AVAILABLE = True
except Exception:
    TORCH_AVAILABLE = False

class LampModel:
    def __init__(self, weight_path: str | None = "models/lamp_v1.pt"):
        self.mode = "heuristic"
        self.model = None
        if TORCH_AVAILABLE and weight_path and os.path.exists(weight_path):
            try:
                self.model = torch.jit.load(weight_path, map_location="cpu").eval()
                self.mode = "torch"
            except Exception as e:
                print(f"[LampModel] weight load failed -> heuristic fallback: {e}")

    def _preprocess_torch(self, img: Image.Image):
        import torch  # noqa
        arr = np.asarray(img.resize((224, 224))).astype("float32") / 255.0
        t = torch.from_numpy(arr).permute(2, 0, 1)  # C,H,W
        return t

    def predict(self, img: Image.Image):
        if self.mode == "torch" and self.model is not None:
            import torch  # noqa
            with torch.no_grad():
                t = self._preprocess_torch(img)
                out = self.model(t.unsqueeze(0))
                prob = torch.softmax(out, dim=1)[0]
                score, pred = torch.max(prob, dim=0)
                label = "NORMAL" if int(pred.item()) == 0 else "ABNORMAL"
                return {"label": label, "score": round(float(score), 4), "model": "torch"}
        # 휴리스틱(밝기 기준)
        gray = np.asarray(img.convert("L")).astype("float32") / 255.0
        brightness = float(gray.mean())
        label = "NORMAL" if brightness >= 0.6 else "ABNORMAL"
        score = brightness if label == "NORMAL" else round(1.0 - brightness, 4)
        return {"label": label, "score": round(float(score), 4), "model": "heuristic"}
