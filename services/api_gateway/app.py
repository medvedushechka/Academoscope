from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI
from pydantic import BaseModel
from sqlalchemy.orm import Session

from common.db import Base, engine, get_db
from common.models import Course, Lesson, Student, Event, EventTypeEnum


# Создаём таблицы, если их ещё нет.
Base.metadata.create_all(bind=engine)


app = FastAPI(title="Academoscope API Gateway", version="0.1.0")


class EventIn(BaseModel):
    """Входная модель события от платформы курсов.

    Пока используется для тестовых/симулированных событий.
    Позже сюда можно будет прокинуть данные от Teachable/Thinkific.
    """

    course_external_id: str
    course_title: Optional[str] = None
    lesson_external_id: Optional[str] = None
    lesson_title: Optional[str] = None
    student_external_id: str
    student_email: Optional[str] = None
    event_type: EventTypeEnum
    occurred_at: Optional[datetime] = None
    payload: Optional[dict] = None


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.post("/events")
async def ingest_event(event_in: EventIn, db: Session = Depends(get_db)) -> dict:
    """Приём события об активности студента.

    - Идентифицирует/создаёт курс, урок и студента по external_id.
    - Записывает событие в таблицу `events`.
    """

    # Курс
    course = (
        db.query(Course)
        .filter(Course.external_id == event_in.course_external_id)
        .one_or_none()
    )
    if course is None:
        course = Course(
            external_id=event_in.course_external_id,
            title=event_in.course_title or event_in.course_external_id,
        )
        db.add(course)
        db.flush()

    # Урок (может отсутствовать, например для события записи на курс)
    lesson = None
    if event_in.lesson_external_id is not None:
        lesson = (
            db.query(Lesson)
            .filter(
                Lesson.course_id == course.id,
                Lesson.external_id == event_in.lesson_external_id,
            )
            .one_or_none()
        )
        if lesson is None:
            lesson = Lesson(
                course_id=course.id,
                external_id=event_in.lesson_external_id,
                title=event_in.lesson_title or event_in.lesson_external_id,
            )
            db.add(lesson)
            db.flush()

    # Студент
    student = (
        db.query(Student)
        .filter(Student.external_id == event_in.student_external_id)
        .one_or_none()
    )
    if student is None:
        student = Student(
            external_id=event_in.student_external_id,
            email=event_in.student_email,
        )
        db.add(student)
        db.flush()
    else:
        # Обновляем email, если он появился или изменился
        if event_in.student_email and student.email != event_in.student_email:
            student.email = event_in.student_email

    occurred_at = event_in.occurred_at or datetime.utcnow()

    db_event = Event(
        course_id=course.id,
        student_id=student.id,
        lesson_id=lesson.id if lesson else None,
        event_type=event_in.event_type.value,
        occurred_at=occurred_at,
        payload=event_in.payload,
    )

    db.add(db_event)
    db.commit()
    db.refresh(db_event)

    return {"id": db_event.id}
