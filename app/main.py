from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import os

app = FastAPI(title="Split Engine API")

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Health check
@app.get("/healthz")
async def healthz():
    return {"ok": True}

# Upload endpoint
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    try:
        with open(file_path, "wb") as f:
            f.write(await file.read())
        size = os.path.getsize(file_path)
        return {"filename": file.filename, "size_bytes": size}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Split endpoint (simple placeholder for now)
@app.get("/split")
async def split_file():
    return JSONResponse({"status": "Split endpoint ready"})
