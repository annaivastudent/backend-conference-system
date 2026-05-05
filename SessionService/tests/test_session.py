from fastapi.testclient import TestClient
from unittest.mock import patch
from SessionService.main import app

client = TestClient(app)


def test_time_validation():
    response = client.post("/api/session/upsert", json={
        "conferenceId": 1,
        "title": "Test",
        "speaker": "Me",
        "startTime": "2026-05-01 12:00",
        "endTime": "2026-05-01 11:00"
    })
    assert response.status_code == 400


@patch("main.get_db")
def test_create_session(mock_db):
    mock_conn = mock_db.return_value.__enter__.return_value
    mock_cursor = mock_conn.cursor.return_value

    mock_cursor.fetchone.return_value = [55]

    response = client.post("/api/session/upsert", json={
        "conferenceId": 1,
        "title": "AI Talk",
        "speaker": "Anna",
        "startTime": "2026-05-01 10:00",
        "endTime": "2026-05-01 11:00",
        "room": "A1"
    })

    assert response.status_code == 200
    assert response.json()["status"] == "created"
    assert response.json()["sessionId"] == 55
