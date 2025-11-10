# Directory: app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Dict, Any, Tuple
from datetime import datetime, timezone
import io, zipfile, hashlib, json, os

# Optional parsers
try:
    import docx  # python-docx
except Exception:
    docx = None

try:
    import srt as srtlib
except Exception:
    srtlib = None

try:
    import webvtt
except Exception:
    webvtt = None

app = FastAPI(title="Split Engine", version="0.1")
app.state.files: Dict[str, Dict[str, Any]] = {}

# Per-type caps (bytes)
CAPS = {
    ".txt": 10 * 1024 * 1024,
    ".srt": 10 * 1024 * 1024,
    ".vtt": 10 * 1024 * 1024,
    ".docx": 25 * 1024 * 1024,
    ".pdf": 40 * 1024 * 1024,  # For v0.2 (not enabled here)
}

# Defaults and thresholds
DEFAULT_LINES = 250
DEFAULT_BYTES = 200_000  # 200 KB
MIN_MULTIPLIER = 2  # skip split if file smaller than 2x defaults
SUPPORTED_V01 = {".txt", ".docx", ".srt", ".vtt"}


def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()


def ext_of(filename: str) -> str:
    e = os.path.splitext(filename)[1].lower()
    return e


def enforce_caps(ext: str, size_bytes: int):
    cap = CAPS.get(ext)
    if cap is None:
        raise HTTPException(415, f"Unsupported file type: {ext}")
    if ext not in SUPPORTED_V01:
        raise HTTPException(415, f"{ext} not supported in v0.1. Supported: .txt, .docx, .srt, .vtt")
    if size_bytes > cap:
        mb = cap // (1024*1024)
        raise HTTPException(413, f"File too large for {ext} (> {mb} MB)")


def load_text_from_upload(filename: str, data: bytes) -> str:
    ext = ext_of(filename)
    if ext == ".txt":
        return data.decode("utf-8", errors="replace")
    if ext == ".docx":
        if docx is None:
            raise HTTPException(500, "DOCX support not installed (python-docx)")
        try:
            d = docx.Document(io.BytesIO(data))
            return "
".join(p.text for p in d.paragraphs)
        except Exception:
            raise HTTPException(400, "Failed to parse .docx file")
    if ext == ".srt":
        if srtlib is None:
            raise HTTPException(500, "SRT support not installed (srt)")
        try:
            text = data.decode("utf-8", errors="replace")
            subs = list(srtlib.parse(text))
            return "

".join(s.content for s in subs)
        except Exception:
            raise HTTPException(400, "Failed to parse .srt file")
    if ext == ".vtt":
        if webvtt is None:
            raise HTTPException(500, "VTT support not installed (webvtt-py)")
        try:
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix=".vtt") as f:
                f.write(data)
                f.flush()
                v = webvtt.read(f.name)
            try:
                os.unlink(f.name)
            except Exception:
                pass
            return "

".join(c.text for c in v)
        except Exception:
            raise HTTPException(400, "Failed to parse .vtt file")
    raise HTTPException(415, f"Unsupported extension: {ext}")


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/upload")
async def upload(file: UploadFile = File(...)) -> Dict[str, Any]:
    name = file.filename or "uploaded"
    ext = ext_of(name)
    data = await file.read()
    if not data:
        raise HTTPException(400, "Empty file")
    enforce_caps(ext, len(data))
    text = load_text_from_upload(name, data)
    fid = sha256_bytes(data)[:16]
    app.state.files[fid] = {
        "name": name,
        "ext": ext,
        "text": text,
        "sha256": sha256_bytes(data),
        "length_chars": len(text),
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }
    return {"file_id": fid, "name": name, "ext": ext, "length_chars": len(text)}


def split_by_lines(text: str, lines_per_piece: int):
    lines = text.splitlines(True)
    pieces = []
    for i in range(0, len(lines), lines_per_piece):
        pieces.append("".join(lines[i:i+lines_per_piece]))
    return pieces


def split_by_size(text: str, bytes_per_piece: int):
    b = text.encode("utf-8")
    pieces = []
    start = 0
    while start < len(b):
        end = min(start + bytes_per_piece, len(b))
        pieces.append(b[start:end].decode("utf-8", errors="replace"))
        start = end
    return pieces


@app.post("/split")
async def split(payload: Dict[str, Any]):
    fid = payload.get("file_id")
    mode = payload.get("mode")
    params = payload.get("params", {})
    if not fid or fid not in app.state.files:
        raise HTTPException(404, "file_id not found")
    if mode not in {"lines", "size"}:
        raise HTTPException(400, "Only 'lines' and 'size' modes are supported in v0.1")

    meta = app.state.files[fid]
    text = meta["text"]

    # Minimum-threshold logic (Option A: simple)
    total_lines = text.count("
") + 1
    total_bytes = len(text.encode("utf-8"))

    lines_default = int(params.get("default_lines", DEFAULT_LINES))
    bytes_default = int(params.get("default_bytes", DEFAULT_BYTES))

    too_small_for_lines = total_lines < (MIN_MULTIPLIER * lines_default)
    too_small_for_size = total_bytes < (MIN_MULTIPLIER * bytes_default)

    if (mode == "lines" and too_small_for_lines) or (mode == "size" and too_small_for_size):
        manifest = {
            "source": {"filename": meta["name"], "sha256": meta["sha256"], "length_chars": len(text)},
            "created_at": datetime.now(timezone.utc).isoformat(),
            "mode": mode,
            "params": params,
            "skipped_reason": "file below minimum threshold for selected mode",
            "pieces": [{"id": "0001", "length_chars": len(text)}],
        }
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("piece_0001.txt", text)
            z.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        buf.seek(0)
        return StreamingResponse(buf, media_type="application/zip", headers={"Content-Disposition": 'attachment; filename="split-pack.zip"'})

    if mode == "lines":
        n = int(params.get("lines", DEFAULT_LINES))
        if n <= 0: raise HTTPException(400, "lines must be > 0")
        pieces = split_by_lines(text, n)
    else:  # size
        sz = int(params.get("bytes", DEFAULT_BYTES))
        if sz <= 0: raise HTTPException(400, "bytes must be > 0")
        pieces = split_by_size(text, sz)

    manifest = {
        "source": {"filename": meta["name"], "sha256": meta["sha256"], "length_chars": len(text)},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "params": params,
        "pieces": [{"id": f"{i+1:04d}", "length_chars": len(p)} for i, p in enumerate(pieces)]
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for i, p in enumerate(pieces, 1):
            z.writestr(f"piece_{i:04d}.txt", p)
        z.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/zip", headers={"Content-Disposition": 'attachment; filename="split-pack.zip"'})

# End of file

