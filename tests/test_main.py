from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_transform():
    r = client.post("/api/v1/transform", json={"source": "web", "data": {"user": "admin"}})
    assert r.status_code == 200
    assert r.json()["transformed"]["user"] == "ADMIN"

