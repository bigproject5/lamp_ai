# inference/predict.py
import os
import tempfile
from PIL import Image
from utils.s3 import download_s3_to_path
from inference.model_loader import LampModel

# Load the model once when the module is imported
# Explicitly point to the best.pt model
model = LampModel(weight_path="models/best.pt")

def run_inference(src: str) -> dict:
    """
    Performs inference on an image from a local path or S3 URI.
    It uses the loaded PyTorch model (best.pt).
    """
    # s3/local branching logic remains the same
    if src.startswith("s3://"):
        # Create a temporary file to download the S3 object
        suffix = os.path.splitext(src)[1] or ".jpg"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.close()
        download_s3_to_path(src, tmp.name)
        try:
            # Perform inference on the downloaded file
            return _infer_on_path(tmp.name)
        finally:
            # Clean up the temporary file
            os.unlink(tmp.name)
    else:
        # Perform inference on the local file directly
        return _infer_on_path(src)

def _infer_on_path(local_path: str) -> dict:
    """
    Private function to run inference on a local image file.
    """
    try:
        # Open the image
        img = Image.open(local_path).convert("RGB")
        # Get prediction from the loaded model
        prediction = model.predict(img)

        # The model's output label ("NORMAL"/"ABNORMAL") needs to be mapped 
        # to the format expected by app.py ("headlight_on"/"headlight_off").
        original_label = prediction.get("label", "ABNORMAL")
        final_label = "headlight_on" if original_label == "NORMAL" else "headlight_off"
        
        return {
            "model": f"torch:{prediction.get('model', 'unknown')}",
            "label": final_label,
            "prob": prediction.get("score", 0.0)
        }
    except Exception as e:
        print(f"Error during inference on {local_path}: {e}")
        # Fallback in case of an error
        return {"model": "error", "label": "unknown", "prob": 0.0}
