import logging
import os
import time

from sqlalchemy import distinct, func
from sqlalchemy.orm import Session

from common.db import Base, SessionLocal, engine
from common.models import (
    Course,
    CourseMetrics,
    Event,
    EventTypeEnum,
    Lesson,
    LessonMetrics,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Создаём таблицы (на случай отдельного запуска воркера).
Base.metadata.create_all(bind=engine)


def calculate_course_metrics(db: Session) -> None:
    courses = db.query(Course).all()
    for course in courses:
        total_students = (
            db.query(func.count(distinct(Event.student_id)))
            .filter(
                Event.course_id == course.id,
                Event.event_type == EventTypeEnum.ENROLLED.value,
            )
            .scalar()
            or 0
        )

        completed_students = (
            db.query(func.count(distinct(Event.student_id)))
            .filter(
                Event.course_id == course.id,
                Event.event_type == EventTypeEnum.COURSE_COMPLETED.value,
            )
            .scalar()
            or 0
        )

        completion_rate = (
            int(completed_students / total_students * 100) if total_students else 0
        )

        metrics = (
            db.query(CourseMetrics)
            .filter(CourseMetrics.course_id == course.id)
            .one_or_none()
        )

        if metrics is None:
            metrics = CourseMetrics(
                course_id=course.id,
                total_students=total_students,
                completed_students=completed_students,
                completion_rate=completion_rate,
            )
            db.add(metrics)
        else:
            metrics.total_students = total_students
            metrics.completed_students = completed_students
            metrics.completion_rate = completion_rate

    db.commit()


def calculate_lesson_metrics(db: Session) -> None:
    lessons = db.query(Lesson).all()
    for lesson in lessons:
        started_students = (
            db.query(func.count(distinct(Event.student_id)))
            .filter(
                Event.lesson_id == lesson.id,
                Event.event_type == EventTypeEnum.LESSON_STARTED.value,
            )
            .scalar()
            or 0
        )

        completed_students = (
            db.query(func.count(distinct(Event.student_id)))
            .filter(
                Event.lesson_id == lesson.id,
                Event.event_type == EventTypeEnum.LESSON_COMPLETED.value,
            )
            .scalar()
            or 0
        )

        drop_off_rate = (
            int((started_students - completed_students) / started_students * 100)
            if started_students
            else 0
        )

        metrics = (
            db.query(LessonMetrics)
            .filter(LessonMetrics.lesson_id == lesson.id)
            .one_or_none()
        )

        if metrics is None:
            metrics = LessonMetrics(
                lesson_id=lesson.id,
                started_students=started_students,
                completed_students=completed_students,
                drop_off_rate=drop_off_rate,
            )
            db.add(metrics)
        else:
            metrics.started_students = started_students
            metrics.completed_students = completed_students
            metrics.drop_off_rate = drop_off_rate

    db.commit()


def run_once() -> None:
    db = SessionLocal()
    try:
        logger.info("Recalculating metrics...")
        calculate_course_metrics(db)
        calculate_lesson_metrics(db)
        logger.info("Metrics recalculated")
    finally:
        db.close()


def main() -> None:
    interval = int(os.getenv("METRICS_INTERVAL_SECONDS", "60"))
    logger.info("Starting metrics worker, interval=%s seconds", interval)

    while True:
        run_once()
        time.sleep(interval)


if __name__ == "__main__":
    main()
