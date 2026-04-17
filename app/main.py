"""Deudores FSCU · DecideChile — FastAPI server (static-only, no PII en runtime)."""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

app = FastAPI(title="Deudores FSCU · DecideChile", version="0.1.0")

STATIC = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

@app.get("/")
def index():
    return FileResponse(str(STATIC / "index.html"))

@app.get("/healthz")
def healthz():
    return {"status": "ok"}
