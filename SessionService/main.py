import os
import pyodbc
from fastapi import FastAPI, HTTPException, Path, Body
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="SessionService")

# -----------------------------
# Database connection
# -----------------------------
def get_db():
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={server},1433;"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(conn_str)


# ============================================================
# SESSION SERVICE
# ============================================================

class SessionIn(BaseModel):
    sessionId: Optional[int] = None
    conferenceId: int
    title: str
    speaker: str
    startTime: str
    endTime: str
    room: Optional[str] = None


class SessionOut(SessionIn):
    sessionId: int


@app.get("/api/sessions", response_model=List[SessionOut])
def get_sessions():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT sessionId, conferenceId, title, speaker, startTime, endTime, room
            FROM HannaIvanova.Session
        """)

        rows = cursor.fetchall()

        return [
            SessionOut(
                sessionId=row[0],
                conferenceId=row[1],
                title=row[2],
                speaker=row[3],
                startTime=row[4],
                endTime=row[5],
                room=row[6]
            )
            for row in rows
        ]


@app.get("/api/sessions/{sessionId}", response_model=SessionOut)
def get_session(sessionId: int = Path(...)):
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT sessionId, conferenceId, title, speaker, startTime, endTime, room
            FROM HannaIvanova.Session
            WHERE sessionId = ?
        """, sessionId)

        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Session not found")

        return SessionOut(
            sessionId=row[0],
            conferenceId=row[1],
            title=row[2],
            speaker=row[3],
            startTime=row[4],
            endTime=row[5],
            room=row[6]
        )


@app.post("/api/session/upsert")
def upsert_session(data: SessionIn = Body(...)):
    with get_db() as conn:
        cursor = conn.cursor()

        if data.startTime >= data.endTime:
            raise HTTPException(400, "startTime must be < endTime")

        if data.sessionId:
            cursor.execute(
                "SELECT sessionId FROM HannaIvanova.Session WHERE sessionId = ?",
                data.sessionId
            )
            if not cursor.fetchone():
                raise HTTPException(404, "Session not found")

            cursor.execute("""
                UPDATE HannaIvanova.Session
                SET conferenceId=?, title=?, speaker=?, startTime=?, endTime=?, room=?
                WHERE sessionId=?
            """, (
                data.conferenceId,
                data.title,
                data.speaker,
                data.startTime,
                data.endTime,
                data.room,
                data.sessionId
            ))

            conn.commit()
            return {"status": "updated", "sessionId": data.sessionId}

        cursor.execute("""
            INSERT INTO HannaIvanova.Session (conferenceId, title, speaker, startTime, endTime, room)
            OUTPUT INSERTED.sessionId
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            data.conferenceId,
            data.title,
            data.speaker,
            data.startTime,
            data.endTime,
            data.room
        ))

        new_id = cursor.fetchone()[0]
        conn.commit()

        return {"status": "created", "sessionId": new_id}


@app.get("/api/session/init")
def init_session_table():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'HannaIvanova')
            EXEC('CREATE SCHEMA HannaIvanova')
        """)

        cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'HannaIvanova' AND TABLE_NAME = 'Session'
        )
        CREATE TABLE HannaIvanova.Session (
            sessionId INT IDENTITY(1,1) PRIMARY KEY,
            conferenceId INT NOT NULL,
            title NVARCHAR(255) NOT NULL,
            speaker NVARCHAR(255) NOT NULL,
            startTime DATETIME NOT NULL,
            endTime DATETIME NOT NULL,
            room NVARCHAR(255),
            FOREIGN KEY (conferenceId) REFERENCES HannaIvanova.Conference(conferenceId)
        )
        """)

        conn.commit()
        return {"status": "OK", "message": "Session table created"}


@app.get("/api/session/seed")
def seed_sessions():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO HannaIvanova.Session (conferenceId, title, speaker, startTime, endTime, room)
            VALUES
            (1, 'Opening Keynote', 'Dr. Smith', '2026-05-01 10:00', '2026-05-01 11:00', 'Hall A'),
            (1, 'AI Trends 2026', 'Anna Petrova', '2026-05-01 12:00', '2026-05-01 13:00', 'Hall B'),
            (2, 'Machine Learning Workshop', 'John Lee', '2026-06-10 09:00', '2026-06-10 12:00', 'Room 101'),
            (3, 'Cybersecurity Panel', 'Maria Gomez', '2026-07-20 14:00', '2026-07-20 15:30', 'Hall C'),
            (4, 'Cloud Architecture', 'David Chen', '2026-08-15 11:00', '2026-08-15 12:30', 'Hall A')
        """)

        conn.commit()
        return {"status": "seeded"}
