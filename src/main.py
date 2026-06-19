"""
Py-Data-Pipeline: Data transformation and routing pipeline
"""
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Py-Data-Pipeline", version="3.0.0")

class Payload(BaseModel):
    source: str
    data: dict

@app.post("/api/v1/transform")
def transform(p: Payload):
    transformed = {k: str(v).upper() for k, v in p.data.items()}
    return {"source": p.source, "transformed": transformed}


@app.get("/health")
def health():
    return {"status": "healthy", "service": "Py-Data-Pipeline", "timestamp": int(time.time())}
