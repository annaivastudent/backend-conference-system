import os
import pyodbc
from fastapi import FastAPI, HTTPException, Path, Body
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv
from azure.servicebus import ServiceBusClient, ServiceBusMessage

load_dotenv()

app = FastAPI(title="FeedbackService")


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
# Service Bus sender
# -----------------------------
def send_message_to_queue(message: str):
    conn_str = os.getenv("SERVICEBUS_SEND")
    queue_name = os.getenv("QUEUE_NAME")

    if not conn_str or not queue_name:
        # В учебном проекте можно просто залогировать,
        # чтобы не падать, если переменные не заданы
        print("Service Bus config is missing, message not sent:", message)
        return

    with ServiceBusClient.from_connection_string(conn_str) as client:
        sender = client.get_queue_sender(queue_name)
        with sender:
            sender.send_messages(ServiceBusMessage(message))


# ============================================================
# MODELS
# ============================================================
class FeedbackIn(BaseModel):
    feedbackId: Optional[int] = None
    conferenceId: int
    rating: int
    comment: Optional[str] = None


class FeedbackOut(FeedbackIn):
    feedbackId: int
    createdAt: str


# ============================================================
# INIT (должен быть выше динамических маршрутов)
# ============================================================
@app.get("/api/feedback/init")
def init_feedback_table():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ConferenceSystem')
            EXEC('CREATE SCHEMA ConferenceSystem')
        """)

        cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'ConferenceSystem' AND TABLE_NAME = 'Feedback'
        )
        CREATE TABLE ConferenceSystem.Feedback (
            feedbackId INT IDENTITY(1,1) PRIMARY KEY,
            conferenceId INT NOT NULL,
            rating INT NOT NULL,
            comment NVARCHAR(MAX),
            createdAt DATETIME NOT NULL DEFAULT GETDATE()
        )
        """)

        conn.commit()
        return {"status": "OK", "message": "Feedback table created"}


# ============================================================
# GET ALL
# ============================================================
@app.get("/api/feedback", response_model=List[FeedbackOut])
def get_feedback():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT feedbackId, conferenceId, rating, comment, createdAt
            FROM ConferenceSystem.Feedback
        """)

        rows = cursor.fetchall()

        return [
            FeedbackOut(
                feedbackId=row[0],
                conferenceId=row[1],
                rating=row[2],
                comment=row[3],
                createdAt=str(row[4])
            )
            for row in rows
        ]


# ============================================================
# GET ONE (динамический — должен быть ниже статических)
# ============================================================
@app.get("/api/feedback/{feedbackId}", response_model=FeedbackOut)
def get_feedback_item(feedbackId: int = Path(...)):
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT feedbackId, conferenceId, rating, comment, createdAt
            FROM ConferenceSystem.Feedback
            WHERE feedbackId = ?
        """, feedbackId)

        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Feedback not found")

        return FeedbackOut(
            feedbackId=row[0],
            conferenceId=row[1],
            rating=row[2],
            comment=row[3],
            createdAt=str(row[4])
        )


# ============================================================
# UPSERT
# ============================================================
@app.post("/api/feedback/upsert")
def upsert_feedback(data: FeedbackIn = Body(...)):
    with get_db() as conn:
        cursor = conn.cursor()

        if not (1 <= data.rating <= 5):
            raise HTTPException(400, "rating must be between 1 and 5")

        # UPDATE
        if data.feedbackId:
            cursor.execute(
                "SELECT feedbackId FROM ConferenceSystem.Feedback WHERE feedbackId = ?",
                data.feedbackId
            )
            if not cursor.fetchone():
                raise HTTPException(404, "Feedback not found")

            cursor.execute("""
                UPDATE ConferenceSystem.Feedback
                SET conferenceId=?, rating=?, comment=?
                WHERE feedbackId=?
            """, (
                data.conferenceId,
                data.rating,
                data.comment,
                data.feedbackId
            ))

            conn.commit()
            return {"status": "updated", "feedbackId": data.feedbackId}

        # CREATE
        cursor.execute("""
            INSERT INTO ConferenceSystem.Feedback (conferenceId, rating, comment, createdAt)
            OUTPUT INSERTED.feedbackId
            VALUES (?, ?, ?, GETDATE())
        """, (
            data.conferenceId,
            data.rating,
            data.comment
        ))

        new_id = cursor.fetchone()[0]
        conn.commit()

        # отправка сообщения в очередь по триггеру создания
        send_message_to_queue(f"New feedback created: {new_id}")

        return {"status": "created", "feedbackId": new_id}
