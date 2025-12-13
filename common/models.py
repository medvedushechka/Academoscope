from datetime import datetime
from typing import Optional
from enum import Enum

from sqlalchemy import JSON, Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

from .db import Base


class Course(Base):
    __tablename__ = "courses"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String(255), unique=True, index=True, nullable=False)
    title = Column(String(255), nullable=False)

    lessons = relationship("Lesson", back_populates="course")
    events = relationship("Event", back_populates="course")
    metrics = relationship("CourseMetrics", back_populates="course", uselist=False)


class Lesson(Base):
    __tablename__ = "lessons"

    id = Column(Integer, primary_key=True, index=True)
    course_id = Column(Integer, ForeignKey("courses.id"), nullable=False, index=True)
    external_id = Column(String(255), nullable=False)
    title = Column(String(255), nullable=False)
    position = Column(Integer, nullable=True)  # Порядок урока в курсе

    course = relationship("Course", back_populates="lessons")
    events = relationship("Event", back_populates="lesson")
    metrics = relationship("LessonMetrics", back_populates="lesson", uselist=False)

    __table_args__ = (
        UniqueConstraint("course_id", "external_id", name="uq_lesson_course_external"),
    )


class Student(Base):
    __tablename__ = "students"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String(255), unique=True, index=True, nullable=False)
    email = Column(String(255), nullable=True)

    events = relationship("Event", back_populates="student")


class EventTypeEnum(str, Enum):  # type: ignore[misc]
    ENROLLED = "enrolled"
    LESSON_STARTED = "lesson_started"
    LESSON_COMPLETED = "lesson_completed"
    COURSE_COMPLETED = "course_completed"


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, index=True)
    course_id = Column(Integer, ForeignKey("courses.id"), nullable=False, index=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False, index=True)
    lesson_id = Column(Integer, ForeignKey("lessons.id"), nullable=True, index=True)
    event_type = Column(String(64), nullable=False, index=True)
    occurred_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    payload = Column(JSON, nullable=True)

    course = relationship("Course", back_populates="events")
    student = relationship("Student", back_populates="events")
    lesson = relationship("Lesson", back_populates="events")


class CourseMetrics(Base):
    __tablename__ = "course_metrics"

    id = Column(Integer, primary_key=True)
    course_id = Column(Integer, ForeignKey("courses.id"), unique=True, nullable=False)
    total_students = Column(Integer, nullable=False, default=0)
    completed_students = Column(Integer, nullable=False, default=0)
    completion_rate = Column(Integer, nullable=False, default=0)  # в процентах, 0-100

    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    course = relationship("Course", back_populates="metrics")


class LessonMetrics(Base):
    __tablename__ = "lesson_metrics"

    id = Column(Integer, primary_key=True)
    lesson_id = Column(Integer, ForeignKey("lessons.id"), unique=True, nullable=False)
    started_students = Column(Integer, nullable=False, default=0)
    completed_students = Column(Integer, nullable=False, default=0)
    drop_off_rate = Column(Integer, nullable=False, default=0)  # в процентах, 0-100

    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    lesson = relationship("Lesson", back_populates="metrics")
