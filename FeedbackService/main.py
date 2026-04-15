import os
import pyodbc
from fastapi import FastAPI, HTTPException, Path, Body
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv

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

        # Создаём схему, если её нет
        cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM sys.schemas WHERE name = 'HannaIvanova'
        )
        EXEC('CREATE SCHEMA HannaIvanova')
        """)

        # Создаём таблицу Feedback
        cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='HannaIvanova' AND TABLE_NAME='Feedback'
        )
        CREATE TABLE HannaIvanova.Feedback (
            feedbackId INT IDENTITY(1,1) PRIMARY KEY,
            conferenceId INT NOT NULL,
            rating INT NOT NULL,
            comment NVARCHAR(MAX),
            createdAt DATETIME NOT NULL DEFAULT GETDATE(),
            FOREIGN KEY (conferenceId) REFERENCES HannaIvanova.Conference(conferenceId)
        )
        """)

        conn.commit()
        return {"status": "OK", "message": "Feedback table created"}

# ============================================================
# SEED
# ============================================================

@app.get("/api/feedback/seed")
def seed_feedback():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO HannaIvanova.Feedback (conferenceId, rating, comment)
            VALUES
            (1, 5, 'Amazing keynote!'),
            (1, 4, 'Very informative'),
            (2, 3, 'Good but long'),
            (3, 5, 'Excellent!')
        """)

        conn.commit()
        return {"status": "seeded"}

# ============================================================
# GET ALL
# ============================================================

@app.get("/api/feedback", response_model=List[FeedbackOut])
def get_feedback():
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT feedbackId, conferenceId, rating, comment, createdAt
            FROM HannaIvanova.Feedback
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
            FROM HannaIvanova.Feedback
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
                "SELECT feedbackId FROM HannaIvanova.Feedback WHERE feedbackId = ?",
                data.feedbackId
            )
            if not cursor.fetchone():
                raise HTTPException(404, "Feedback not found")

            cursor.execute("""
                UPDATE HannaIvanova.Feedback
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
            INSERT INTO HannaIvanova.Feedback (conferenceId, rating, comment, createdAt)
            OUTPUT INSERTED.feedbackId
            VALUES (?, ?, ?, GETDATE())
        """, (
            data.conferenceId,
            data.rating,
            data.comment
        ))

        new_id = cursor.fetchone()[0]
        conn.commit()

        return {"status": "created", "feedbackId": new_id}
