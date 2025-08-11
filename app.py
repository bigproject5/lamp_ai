from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from pydantic import BaseModel
from inference.predict import run_inference
import time, tempfile, shutil, os, traceback
from PIL import Image

# 1) app 먼저 만든다
app = FastAPI(title="lamp_ai", version="1.0.0")

# 2) Pydantic 모델
class InferenceRequest(BaseModel):
    auditId: int | None = None
    inspectionId: int | None = None
    s3Uri: str

# 3) 라우트들
@app.get("/health")
def health():
    return {"status": "ok"}

# S3 URI 방식
@app.post("/inference/lamp")
def inference_lamp(req: InferenceRequest):
    t0 = time.time()
    result = run_inference(req.s3Uri)
    return {
        "status": "COMPLETED",
        "result": result,
        "meta": {"model": result.get("model", "heuristic"),
                 "latencyMs": int((time.time() - t0) * 1000)}
    }

# 파일 업로드(로컬) 방식
@app.post("/inference/lamp/upload")
async def inference_lamp_upload(
        auditId: int = Query(...),
        inspectionId: int = Query(...),
        file: UploadFile = File(...)
):
    t0 = time.time()
    suffix = os.path.splitext(file.filename)[1] or ".bin"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    try:
        # 포맷 검증은 선택
        try:
            with Image.open(tmp_path) as im:
                im.verify()
        except Exception:
            pass

        result = run_inference(tmp_path)  # run_inference가 로컬 경로 처리하도록
        return {
            "status": "COMPLETED",
            "auditId": auditId,
            "inspectionId": inspectionId,
            "result": result,
            "meta": {"latencyMs": int((time.time() - t0) * 1000)}
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.unlink(tmp_path)
