from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from typing import Dict, Any, List
from datetime import datetime, timezone
import os, io, zipfile, hashlib, json, tempfile

# Optional parsers (installed via requirements.txt)
from docx import Document
import srt
import webvtt

app = FastAPI(title="Split Engine", version="0.1")

# In-memory registry (v0.1). Later we can move to temp files/S3.
FILES: Dict[str, Dict[str, Any]] = {}

# Per-type caps (bytes)
CAPS = {
    ".txt": 10 * 1024 * 1024,
    ".srt": 10 * 1024 * 1024,
    ".vtt": 10 * 1024 * 1024,
    ".docx": 25 * 1024 * 1024,
    ".pdf": 40 * 1024 * 1024,  # reserved for v0.2
}
SUPPORTED_V01 = {".txt", ".docx", ".srt", ".vtt"}

# Defaults & thresholds
DEFAULT_LINES = 250
DEFAULT_BYTES = 200_000  # 200 KB
MIN_MULTIPLIER = 2  # skip split if file smaller than 2x defaults

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def ext_of(name: str) -> str:
    return os.path.splitext(name)[1].lower()

def enforce_caps(ext: str, size_bytes: int):
    cap = CAPS.get(ext)
    if cap is None:
        raise HTTPException(415, f"Unsupported file type: {ext}")
    if ext not in SUPPORTED_V01:
        raise HTTPException(415, f"{ext} not supported in v0.1 (use .txt/.docx/.srt/.vtt)")
    if size_bytes > cap:
        mb = cap // (1024*1024)
        raise HTTPException(413, f"File too large for {ext} (> {mb} MB)")

def load_text(name: str, data: bytes) -> str:
    ext = ext_of(name)
    if ext == ".txt":
        return data.decode("utf-8", errors="replace")
    if ext == ".docx":
        try:
            doc = Document(io.BytesIO(data))
            return "\n".join(p.text for p in doc.paragraphs)
        except Exception:
            raise HTTPException(400, "Failed to parse .docx")
    if ext == ".srt":
        try:
            text = data.decode("utf-8", errors="replace")
            subs = list(srt.parse(text))
            return "\n\n".join(s.content for s in subs)
        except Exception:
            raise HTTPException(400, "Failed to parse .srt")
    if ext == ".vtt":
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".vtt") as f:
                f.write(data); f.flush(); v = webvtt.read(f.name)
            try: os.unlink(f.name)
            except Exception: pass
            return "\n\n".join(c.text for c in v)
        except Exception:
            raise HTTPException(400, "Failed to parse .vtt")
    raise HTTPException(415, f"Unsupported extension: {ext}")

def split_by_lines(text: str, n: int) -> List[str]:
    lines = text.splitlines(True)
    return ["".join(lines[i:i+n]) for i in range(0, len(lines), n)]

def split_by_size(text: str, sz: int) -> List[str]:
    b = text.encode("utf-8")
    out, start = [], 0
    while start < len(b):
        end = min(start + sz, len(b))
        out.append(b[start:end].decode("utf-8", errors="replace"))
        start = end
    return out

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    name = file.filename or "upload"
    ext = ext_of(name)
    data = await file.read()
    if not data:
        raise HTTPException(400, "Empty file")
    enforce_caps(ext, len(data))
    text = load_text(name, data)
    fid = sha256_bytes(data)[:16]
    FILES[fid] = {
        "name": name, "ext": ext, "sha256": sha256_bytes(data),
        "text": text, "length_chars": len(text),
        "uploaded_at": datetime.now(timezone.utc).isoformat()
    }
    return {"file_id": fid, "name": name, "ext": ext, "length_chars": len(text)}

@app.post("/split")
async def split(payload: Dict[str, Any]):
    fid = payload.get("file_id")
    mode = payload.get("mode")
    params = payload.get("params", {})
    if not fid or fid not in FILES:
        raise HTTPException(404, "file_id not found")
    if mode not in {"lines", "size"}:
        raise HTTPException(400, "mode must be 'lines' or 'size'")

    meta = FILES[fid]
    text = meta["text"]

    # Minimum-threshold rule
    total_lines = text.count("\n") + 1
    total_bytes = len(text.encode("utf-8"))
    lines_default = int(params.get("default_lines", DEFAULT_LINES))
    bytes_default = int(params.get("default_bytes", DEFAULT_BYTES))
    too_small_for_lines = total_lines < (MIN_MULTIPLIER * lines_default)
    too_small_for_size  = total_bytes < (MIN_MULTIPLIER * bytes_default)

    if (mode == "lines" and too_small_for_lines) or (mode == "size" and too_small_for_size):
        manifest = {
            "source": {"filename": meta["name"], "sha256": meta["sha256"], "length_chars": len(text)},
            "created_at": datetime.now(timezone.utc).isoformat(),
            "mode": mode, "params": params,
            "skipped_reason": "file below minimum threshold for selected mode",
            "pieces": [{"id": "0001", "length_chars": len(text)}],
        }
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("piece_0001.txt", text)
            z.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        buf.seek(0)
        return StreamingResponse(buf, media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="split-pack.zip"'})

    # Perform split
    if mode == "lines":
        n = int(params.get("lines", DEFAULT_LINES))
        if n <= 0: raise HTTPException(400, "lines must be > 0")
        pieces = split_by_lines(text, n)
    else:
        sz = int(params.get("bytes", DEFAULT_BYTES))
        if sz <= 0: raise HTTPException(400, "bytes must be > 0")
        pieces = split_by_size(text, sz)

    manifest = {
        "source": {"filename": meta["name"], "sha256": meta["sha256"], "length_chars": len(text)},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode, "params": params,
        "pieces": [{"id": f"{i+1:04d}", "length_chars": len(p)} for i, p in enumerate(pieces)]
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for i, p in enumerate(pieces, 1):
            z.writestr(f"piece_{i:04d}.txt", p)
        z.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="split-pack.zip"'})
