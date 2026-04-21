from azure.servicebus import ServiceBusClient
import threading
import time
import os
import pyodbc
from fastapi import FastAPI, HTTPException, Path, Body
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="ConferenceService")


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


# -----------------------------
# Pydantic models
# -----------------------------
class ConferenceIn(BaseModel):
    conferenceId: Optional[int] = None
    title: str
    startDate: str
    endDate: str
    isActive: bool = True
    description: Optional[str] = None


class ConferenceOut(ConferenceIn):
    conferenceId: int


# -----------------------------
# GET all conferences
# -----------------------------
@app.get("/api/conferences", response_model=List[ConferenceOut])
def get_conferences(isActive: Optional[bool] = None):
    with get_db() as conn:
        cursor = conn.cursor()

        query = """
            SELECT conferenceId, title, startDate, endDate, isActive, description
            FROM ConferenceSystem.Conference
        """
        params = []

        if isActive is not None:
            query += " WHERE isActive = ?"
            params.append(1 if isActive else 0)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        return [
            ConferenceOut(
                conferenceId=row[0],
                title=row[1],
                startDate=row[2],
                endDate=row[3],
                isActive=row[4],
                description=row[5]
            )
            for row in rows
        ]


# -----------------------------
# GET one conference
# -----------------------------
@app.get("/api/conferences/{conferenceId}", response_model=ConferenceOut)
def get_conference(conferenceId: int = Path(...)):
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT conferenceId, title, startDate, endDate, isActive, description
            FROM ConferenceSystem.Conference
            WHERE conferenceId = ?
        """, conferenceId)

        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Conference not found")

        return ConferenceOut(
            conferenceId=row[0],
            title=row[1],
            startDate=row[2],
            endDate=row[3],
            isActive=row[4],
            description=row[5]
        )


# -----------------------------
# UPSERT conference
# -----------------------------
@app.post("/api/conference/upsert")
def upsert_conference(data: ConferenceIn = Body(...)):
    with get_db() as conn:
        cursor = conn.cursor()

        if data.startDate >= data.endDate:
            raise HTTPException(400, "startDate must be < endDate")

        # UPDATE
        if data.conferenceId:
            cursor.execute(
                "SELECT conferenceId FROM ConferenceSystem.Conference WHERE conferenceId = ?",
                data.conferenceId
            )
            if not cursor.fetchone():
                raise HTTPException(404, "Conference not found")

            cursor.execute("""
                UPDATE ConferenceSystem.Conference
                SET title=?, startDate=?, endDate=?, isActive=?, description=?
                WHERE conferenceId=?
            """, (
                data.title,
                data.startDate,
                data.endDate,
                data.isActive,
                data.description,
                data.conferenceId
            ))

            conn.commit()
            return {"status": "updated", "conferenceId": data.conferenceId}

        # CREATE
        cursor.execute("""
            INSERT INTO ConferenceSystem.Conference (title, startDate, endDate, isActive, description)
            OUTPUT INSERTED.conferenceId
            VALUES (?, ?, ?, ?, ?)
        """, (
            data.title,
            data.startDate,
            data.endDate,
            data.isActive,
            data.description
        ))

        new_id = cursor.fetchone()[0]
        conn.commit()

        return {"status": "created", "conferenceId": new_id}


# -----------------------------
# INIT: create schema + table
# -----------------------------
@app.get("/api/conference/init")
def init_conference_table():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ConferenceSystem')
            EXEC('CREATE SCHEMA ConferenceSystem')
        """)

        cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'ConferenceSystem' AND TABLE_NAME = 'Conference'
        )
        CREATE TABLE ConferenceSystem.Conference (
            conferenceId INT IDENTITY(1,1) PRIMARY KEY,
            title NVARCHAR(255) NOT NULL,
            startDate DATE NOT NULL,
            endDate DATE NOT NULL,
            isActive BIT NOT NULL DEFAULT 1,
            description NVARCHAR(MAX)
        )
        """)

        conn.commit()
        return {"status": "OK", "message": "Schema ConferenceSystem and table Conference created"}


# -----------------------------
# SEED: insert test data
# -----------------------------
@app.get("/api/conference/seed")
def seed_conferences():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO ConferenceSystem.Conference (title, startDate, endDate, isActive, description)
            VALUES
            ('Tech Summit 2026', '2026-05-01', '2026-05-03', 1, 'Technology and innovation conference'),
            ('AI Expo Europe', '2026-06-10', '2026-06-12', 1, 'Artificial Intelligence exhibition'),
            ('Cybersecurity Forum', '2026-07-20', '2026-07-21', 0, 'Security and privacy discussions'),
            ('Cloud World Congress', '2026-08-15', '2026-08-17', 1, 'Cloud computing and DevOps'),
            ('Data Science Week', '2026-09-05', '2026-09-09', 1, 'Data analytics and ML workshops')
        """)

        conn.commit()
        return {"status": "seeded"}


def background_queue_reader():
    conn_str = os.getenv("SERVICEBUS_LISTEN")
    queue_name = os.getenv("QUEUE_NAME")

    if not conn_str or not queue_name:
        print("Service Bus listen config is missing, background reader not started")
        return

    while True:
        with ServiceBusClient.from_connection_string(conn_str) as client:
            receiver = client.get_queue_receiver(queue_name)
            with receiver:
                for msg in receiver.receive_messages(max_wait_time=5):
                    print("Received message from queue:", str(msg))
                    receiver.complete_message(msg)

        time.sleep(10)  # интервал между циклами чтения


threading.Thread(target=background_queue_reader, daemon=True).start()
