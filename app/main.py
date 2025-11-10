from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import os
import uuid
import shutil
import zipfile
from typing import List, Dict
from docx import Document
import srt
import webvtt

app = FastAPI(title="Split Engine", version="0.1")

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Utility functions ---

def save_upload_file(upload_file: UploadFile, destination: str):
    with open(destination, "wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)
    return destination


def read_file_content(file_path: str, file_type: str) -> str:
    if file_type == "txt":
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    elif file_type == "docx":
        doc = Document(file_path)
        return "\n".join([p.text for p in doc.paragraphs])
    elif file_type == "srt":
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            subs = list(srt.parse(f.read()))
        return "\n".join([sub.content for sub in subs])
    elif file_type == "vtt":
        return "\n".join([caption.text for caption in webvtt.read(file_path)])
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type")


def split_by_lines(text: str, max_lines: int) -> List[str]:
    lines = text.splitlines()
    return [
        "\n".join(lines[i:i + max_lines])
        for i in range(0, len(lines), max_lines)
    ]


def split_by_size(text: str, max_bytes: int) -> List[str]:
    parts = []
    current = ""
    for line in text.splitlines(True):
        if len(current.encode("utf-8")) + len(line.encode("utf-8")) > max_bytes:
            parts.append(current)
            current = ""
        current += line
    if current:
        parts.append(current)
    return parts


def zip_output_files(files: List[str], zip_path: str):
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for f in files:
            zipf.write(f, os.path.basename(f))
    return zip_path

# --- Routes ---

@app.get("/healthz")
def health_check():
    return {"ok": True}


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    ext = file.filename.split(".")[-1].lower()
    if ext not in ["txt", "docx", "srt", "vtt"]:
        raise HTTPException(status_code=400, detail="Unsupported file type")

    limits = {"txt": 10, "srt": 10, "vtt": 10, "docx": 25}
    max_mb = limits.get(ext, 10)
    max_bytes = max_mb * 1024 * 1024

    contents = await file.read()
    if len(contents) > max_bytes:
        raise HTTPException(status_code=400, detail=f"File too large (>{max_mb}MB)")

    file_id = str(uuid.uuid4())
    path = os.path.join(UPLOAD_DIR, f"{file_id}.{ext}")

    with open(path, "wb") as f:
        f.write(contents)

    return {"file_id": file_id, "filename": file.filename, "size": len(contents), "type": ext}


@app.post("/split")
def split_file(
    file_id: str,
    mode: str,
    lines: int = 250,
    size: int = 200000
):
    # locate file
    file_path = None
    for f in os.listdir(UPLOAD_DIR):
        if f.startswith(file_id):
            file_path = os.path.join(UPLOAD_DIR, f)
            ext = f.split(".")[-1]
            break
    if not file_path:
        raise HTTPException(status_code=404, detail="File not found")

    text = read_file_content(file_path, ext)

    # skip if small
    if len(text) < 200:
        return {"skipped": True, "reason": "File too small to split"}

    if mode == "lines":
        parts = split_by_lines(text, lines)
    elif mode == "size":
        parts = split_by_size(text, size)
    else:
        raise HTTPException(status_code=400, detail="Invalid mode")

    output_files = []
    for i, chunk in enumerate(parts, start=1):
        out_path = os.path.join(OUTPUT_DIR, f"{file_id}_part{i}.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(chunk)
        output_files.append(out_path)

    zip_path = os.path.join(OUTPUT_DIR, f"{file_id}_split.zip")
    zip_output_files(output_files, zip_path)

    return FileResponse(zip_path, media_type="application/zip", filename=f"{file_id}_split.zip")
