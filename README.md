# Backend Conference System

## Overview

Backend Conference System --- учебный проект, реализованный с
использованием микросервисной архитектуры.\
Каждый сервис изолирован в отдельной ветке репозитория.

Проект состоит из трёх микросервисов:

-   ConferenceService --- управление конференциями\
-   SessionService --- управление сессиями\
-   FeedbackService --- обработка отзывов и работа с очередью

------------------------------------------------------------------------

## Architecture

  -------------------------------------------------------------------------
  Service             Branch        Description
  ------------------- ------------- ---------------------------------------
  ConferenceService   conference    CRUD для конференций

  SessionService      session       Управление сессиями

  FeedbackService     feedback      Отзывы + Azure Service Bus
  -------------------------------------------------------------------------

------------------------------------------------------------------------

## Branch Structure

main → документация\
conference → ConferenceService\
session → SessionService\
feedback → FeedbackService

------------------------------------------------------------------------

## How to Run

### Install dependencies

pip install -r requirements.txt

### Run services

FeedbackService: cd FeedbackService\
py -m uvicorn main:app --reload --port 8000

ConferenceService: cd ConferenceService\
py -m uvicorn main:app --reload --port 8001

SessionService: cd SessionService\
py -m uvicorn main:app --reload --port 8002

------------------------------------------------------------------------

## Database Init

ConferenceService: GET /api/conference/init

FeedbackService: GET /api/feedback/init

------------------------------------------------------------------------

## Feedback Flow

POST /api/feedback/upsert

{ "conferenceId": 1, "rating": 5, "comment": "Test message" }

→ отправка в очередь\
→ обработка ConferenceService

------------------------------------------------------------------------

## Azure Service Bus

Queue: hannaivanova

ENV: SERVICEBUS_SEND\
SERVICEBUS_LISTEN

------------------------------------------------------------------------

## Testing

1.  Создать конференцию\
2.  Отправить отзыв\
3.  Проверить консоль

Ожидаемо: Received message from queue: New feedback created: X
