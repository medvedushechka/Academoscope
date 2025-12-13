import json
import os
from datetime import datetime, timedelta
from types import SimpleNamespace
from urllib import error as urlerror
from urllib import request as urlrequest

from flask import Flask, abort, jsonify, render_template, request
from sqlalchemy import distinct, func
from sqlalchemy.orm import scoped_session, sessionmaker

from common.db import Base, engine
from common.models import (
    Course,
    CourseMetrics,
    Event,
    EventTypeEnum,
    Lesson,
    LessonMetrics,
    Student,
)


# Создаём таблицы (на случай, если дашборд запускается отдельно от API).
Base.metadata.create_all(bind=engine)

SessionLocal = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)

app = Flask(__name__)

AI_USE_RECOMMENDER = os.getenv("AI_USE_RECOMMENDER", "false").lower() == "true"
AI_PROVIDER = os.getenv("AI_PROVIDER", "gemini").lower()
YANDEX_GPT_API_KEY = os.getenv("YANDEX_GPT_API_KEY")
YANDEX_GPT_FOLDER_ID = os.getenv("YANDEX_GPT_FOLDER_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")


def _mask_email(email: str | None) -> str | None:
    if not email:
        return email
    if "@" not in email:
        return email
    local, domain = email.split("@", 1)
    if not local:
        return "***@" + domain
    if len(local) == 1:
        masked_local = local[0] + "***"
    elif len(local) == 2:
        masked_local = local[0] + "***"
    else:
        masked_local = local[0] + "***"
    return f"{masked_local}@{domain}"


def _get_student_status(last_seen_at: datetime | None) -> tuple[str, str, str]:
    status_label = "Нет данных"
    status_code = "no_data"
    status_badge_color = "secondary"

    if last_seen_at is not None:
        days_inactive = (datetime.utcnow() - last_seen_at).days
        if days_inactive <= 7:
            status_code = "active"
            status_label = "Активен"
            status_badge_color = "success"
        elif days_inactive <= 30:
            status_code = "inactive"
            status_label = "Неактивен"
            status_badge_color = "warning"
        else:
            status_code = "risk"
            status_label = "В группе риска"
            status_badge_color = "danger"

    return status_label, status_code, status_badge_color


def _format_dt_short(value: datetime | None) -> str | None:
    if value is None:
        return None
    text = value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return text[:-4]


@app.teardown_appcontext
def remove_session(exception=None):  # type: ignore[override]
    SessionLocal.remove()


@app.route("/")
def index():
    db = SessionLocal()
    period, start_at, period_label = _get_period()

    courses = []
    total_students_all = 0
    completed_students_all = 0

    for course in db.query(Course).order_by(Course.title).all():
        total_students, completed_students, completion_rate = _calculate_course_metrics_for_period(
            db, course.id, start_at
        )

        courses.append(
            {
                "id": course.id,
                "title": course.title,
                "total_students": total_students,
                "completed_students": completed_students,
                "completion_rate": completion_rate,
            }
        )

        total_students_all += total_students
        completed_students_all += completed_students

    overall_completion_rate = (
        int(completed_students_all / total_students_all * 100)
        if total_students_all
        else 0
    )
    courses_count = len(courses)

    course_titles = [course["title"] for course in courses]
    course_completion_rates = [course["completion_rate"] for course in courses]
    not_completed_students_all = max(total_students_all - completed_students_all, 0)

    # Общий охват: уникальные студенты за всё время и активные за последние 30 дней
    total_unique_students = (
        db.query(func.count(distinct(Event.student_id))).scalar() or 0
    )
    active_students_30d = (
        db.query(func.count(distinct(Event.student_id)))
        .filter(Event.occurred_at >= datetime.utcnow() - timedelta(days=30))
        .scalar()
        or 0
    )

    # Студенты группы риска на основе последнего визита
    risk_rows = (
        db.query(
            Student.id,
            Student.external_id,
            Student.email,
            func.max(Event.occurred_at),
        )
        .join(Event, Event.student_id == Student.id)
        .group_by(Student.id, Student.external_id, Student.email)
        .all()
    )

    risk_students: list[dict] = []
    for student_id, external_id, email, last_seen in risk_rows:
        status_label, status_code, status_badge_color = _get_student_status(last_seen)
        if status_code != "risk":
            continue

        display_name = external_id or _mask_email(email) or str(student_id)

        risk_students.append(
            {
                "id": student_id,
                "display_name": display_name,
                "email": email,
                "last_seen_at": _format_dt_short(last_seen),
                "status_label": status_label,
                "status_code": status_code,
                "status_badge_color": status_badge_color,
            }
        )

    risk_students.sort(
        key=lambda s: (s["last_seen_at"] or ""),
    )
    risk_students = risk_students[:5]

    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.args.get(
        "format"
    ) == "json":
        return jsonify(
            period=period,
            period_label=period_label,
            summary={
                "courses_count": courses_count,
                "total_students_all": total_students_all,
                "completed_students_all": completed_students_all,
                "not_completed_students_all": not_completed_students_all,
                "overall_completion_rate": overall_completion_rate,
            },
            courses=courses,
            charts={
                "course_titles": course_titles,
                "course_completion_rates": course_completion_rates,
            },
        )

    # Поведенческая аналитика: сложные и популярные уроки, распределение прогресса по курсам
    behavior_difficult_lessons: list[dict] = []
    behavior_popular_lessons: list[dict] = []

    lesson_rows = (
        db.query(
            Lesson.id,
            Lesson.title,
            Course.title.label("course_title"),
        )
        .join(Course, Lesson.course_id == Course.id)
        .all()
    )

    for lesson_id, lesson_title, course_title in lesson_rows:
        started_students, completed_lesson_students = _calculate_lesson_metrics_for_period(
            db, lesson_id, start_at
        )
        if not started_students:
            continue

        drop_off_rate = int(
            (started_students - completed_lesson_students) / started_students * 100
        )

        behavior_difficult_lessons.append(
            {
                "lesson_id": lesson_id,
                "course_title": course_title,
                "lesson_title": lesson_title,
                "started_students": started_students,
                "completed_students": completed_lesson_students,
                "drop_off_rate": drop_off_rate,
            }
        )

        behavior_popular_lessons.append(
            {
                "lesson_id": lesson_id,
                "course_title": course_title,
                "lesson_title": lesson_title,
                "started_students": started_students,
            }
        )

    behavior_difficult_lessons.sort(key=lambda x: x["drop_off_rate"], reverse=True)
    behavior_difficult_lessons = behavior_difficult_lessons[:5]

    behavior_popular_lessons.sort(key=lambda x: x["started_students"], reverse=True)
    behavior_popular_lessons = behavior_popular_lessons[:5]

    progress_distribution = {
        "0_25": 0,
        "25_50": 0,
        "50_75": 0,
        "75_100": 0,
    }
    for course in courses:
        rate = course.get("completion_rate") or 0
        if rate < 25:
            progress_distribution["0_25"] += 1
        elif rate < 50:
            progress_distribution["25_50"] += 1
        elif rate < 75:
            progress_distribution["50_75"] += 1
        else:
            progress_distribution["75_100"] += 1

    ai_recommendations: list[str] = []

    return render_template(
        "index.html",
        courses=courses,
        courses_count=courses_count,
        total_students_all=total_students_all,
        completed_students_all=completed_students_all,
        overall_completion_rate=overall_completion_rate,
        course_titles=course_titles,
        course_completion_rates=course_completion_rates,
        not_completed_students_all=not_completed_students_all,
        reach_total_students=total_unique_students,
        reach_active_students_30d=active_students_30d,
        ai_recommendations=ai_recommendations,
        ai_enabled=AI_USE_RECOMMENDER,
        risk_students=risk_students,
        behavior_difficult_lessons=behavior_difficult_lessons,
        behavior_popular_lessons=behavior_popular_lessons,
        progress_distribution=progress_distribution,
        period=period,
        period_label=period_label,
    )


@app.route("/courses/<int:course_id>")
def course_detail(course_id: int):
    db = SessionLocal()

    course = db.query(Course).filter(Course.id == course_id).one_or_none()
    if course is None:
        abort(404)
    period, start_at, period_label = _get_period()

    total_students, completed_students, completion_rate = _calculate_course_metrics_for_period(
        db, course.id, start_at
    )

    metrics = SimpleNamespace(
        total_students=total_students,
        completed_students=completed_students,
        completion_rate=completion_rate,
    )

    lessons = []
    for lesson in (
        db.query(Lesson)
        .filter(Lesson.course_id == course.id)
        .order_by(Lesson.position.nulls_last(), Lesson.id)
        .all()
    ):
        started_students, completed_lesson_students = _calculate_lesson_metrics_for_period(
            db, lesson.id, start_at
        )
        drop_off_rate = (
            int((started_students - completed_lesson_students) / started_students * 100)
            if started_students
            else 0
        )

        lessons.append(
            {
                "id": lesson.id,
                "title": lesson.title,
                "started_students": started_students,
                "completed_students": completed_lesson_students,
                "drop_off_rate": drop_off_rate,
            }
        )

    lesson_labels = [lesson["title"] for lesson in lessons]
    drop_off_values = [lesson["drop_off_rate"] for lesson in lessons]

    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.args.get(
        "format"
    ) == "json":
        return jsonify(
            period=period,
            period_label=period_label,
            metrics={
                "total_students": metrics.total_students,
                "completed_students": metrics.completed_students,
                "completion_rate": metrics.completion_rate,
            },
            lessons=lessons,
            chart={
                "lesson_labels": lesson_labels,
                "drop_off_values": drop_off_values,
            },
        )

    return render_template(
        "course_detail.html",
        course=course,
        metrics=metrics,
        lessons=lessons,
        lesson_labels=lesson_labels,
        drop_off_values=drop_off_values,
        period=period,
        period_label=period_label,
    )


@app.route("/students/<int:student_id>")
def student_detail(student_id: int):
    db = SessionLocal()

    student = db.query(Student).filter(Student.id == student_id).one_or_none()
    if student is None:
        abort(404)

    period, start_at, period_label = _get_period()

    event_filters = [Event.student_id == student.id]
    if start_at is not None:
        event_filters.append(Event.occurred_at >= start_at)

    first_seen_at = (
        db.query(func.min(Event.occurred_at)).filter(*event_filters).scalar()
    )
    last_seen_at = (
        db.query(func.max(Event.occurred_at)).filter(*event_filters).scalar()
    )

    status_label, status_code, status_badge_color = _get_student_status(last_seen_at)

    first_seen_at_str = _format_dt_short(first_seen_at)
    last_seen_at_str = _format_dt_short(last_seen_at)

    student_courses: list[dict] = []
    progress_values: list[int] = []

    course_rows = (
        db.query(Course)
        .join(Event, Event.course_id == Course.id)
        .filter(*event_filters)
        .distinct()
        .order_by(Course.title)
        .all()
    )

    for course in course_rows:
        course_filters = event_filters + [Event.course_id == course.id]

        last_course_visit = (
            db.query(func.max(Event.occurred_at)).filter(*course_filters).scalar()
        )

        total_lessons = (
            db.query(func.count(distinct(Lesson.id)))
            .filter(Lesson.course_id == course.id)
            .scalar()
            or 0
        )

        period_filter = []
        if start_at is not None:
            period_filter = [Event.occurred_at >= start_at]

        completed_lessons = (
            db.query(func.count(distinct(Event.lesson_id)))
            .filter(
                Event.student_id == student.id,
                Event.course_id == course.id,
                Event.event_type == EventTypeEnum.LESSON_COMPLETED.value,
                Event.lesson_id.isnot(None),
                *period_filter,
            )
            .scalar()
            or 0
        )

        started_lessons = (
            db.query(func.count(distinct(Event.lesson_id)))
            .filter(
                Event.student_id == student.id,
                Event.course_id == course.id,
                Event.event_type == EventTypeEnum.LESSON_STARTED.value,
                Event.lesson_id.isnot(None),
                *period_filter,
            )
            .scalar()
            or 0
        )

        has_course_completed = (
            db.query(Event.id)
            .filter(
                Event.student_id == student.id,
                Event.course_id == course.id,
                Event.event_type == EventTypeEnum.COURSE_COMPLETED.value,
                *period_filter,
            )
            .first()
            is not None
        )

        if total_lessons:
            progress = int(completed_lessons / total_lessons * 100)
        else:
            progress = 100 if has_course_completed else 0

        if has_course_completed and progress < 100:
            progress = 100

        progress_values.append(progress)

        student_courses.append(
            {
                "course": course,
                "progress": progress,
                "total_lessons": total_lessons,
                "completed_lessons": completed_lessons,
                "started_lessons": started_lessons,
                "last_visit": last_course_visit,
                "last_visit_str": _format_dt_short(last_course_visit),
            }
        )

    overall_progress = (
        int(sum(progress_values) / len(progress_values)) if progress_values else 0
    )

    activity_rows = (
        db.query(func.date(Event.occurred_at), func.count())
        .filter(*event_filters)
        .group_by(func.date(Event.occurred_at))
        .order_by(func.date(Event.occurred_at))
        .all()
    )
    activity_timeline = [
        {"date": str(row_date), "events_count": count}
        for row_date, count in activity_rows
    ]

    auto_tags: list[str] = []
    if status_code == "active":
        auto_tags.append("Активный участник")
    if status_code == "risk":
        auto_tags.append("В группе риска (давно не заходил)")
    if overall_progress >= 80:
        auto_tags.append("Высокая успеваемость")
    if overall_progress <= 30 and student_courses:
        auto_tags.append("Застрял на начальных уроках")
    if not auto_tags:
        auto_tags.append("Данных пока мало, чтобы выделить паттерны")

    display_name = student.external_id or _mask_email(student.email) or str(student.id)

    days_since_last_visit: int | None = None
    if last_seen_at is not None:
        days_since_last_visit = (datetime.utcnow() - last_seen_at).days

    student_ai_context = {
        "student_id": student.id,
        "display_name": display_name,
        "status_code": status_code,
        "overall_progress": overall_progress,
        "days_since_last_visit": days_since_last_visit,
        "courses": [
            {
                "title": item["course"].title,
                "progress": item["progress"],
                "total_lessons": item["total_lessons"],
                "completed_lessons": item["completed_lessons"],
                "started_lessons": item["started_lessons"],
            }
            for item in student_courses
        ],
    }

    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.args.get(
        "format"
    ) == "json":
        return jsonify(
            period=period,
            period_label=period_label,
            student={
                "id": student.id,
                "external_id": student.external_id,
                "email": student.email,
                "display_name": display_name,
                "status": status_code,
                "status_label": status_label,
                "overall_progress": overall_progress,
                "first_seen_at": first_seen_at.isoformat() if first_seen_at else None,
                "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
            },
            courses=[
                {
                    "id": item["course"].id,
                    "title": item["course"].title,
                    "progress": item["progress"],
                    "total_lessons": item["total_lessons"],
                    "completed_lessons": item["completed_lessons"],
                    "started_lessons": item["started_lessons"],
                    "last_visit": item["last_visit"].isoformat()
                    if item["last_visit"]
                    else None,
                }
                for item in student_courses
            ],
            activity_timeline=activity_timeline,
            auto_tags=auto_tags,
        )

    return render_template(
        "student_detail.html",
        student=student,
        student_display_name=display_name,
        status_label=status_label,
        status_code=status_code,
        status_badge_color=status_badge_color,
        first_seen_at=first_seen_at_str,
        last_seen_at=last_seen_at_str,
        overall_progress=overall_progress,
        courses=student_courses,
        activity_timeline=activity_timeline,
        auto_tags=auto_tags,
        ai_enabled=AI_USE_RECOMMENDER,
        student_ai_context=student_ai_context,
        period=period,
        period_label=period_label,
    )


@app.route("/students")
def students_list():
    db = SessionLocal()

    period, start_at, period_label = _get_period()

    filters: list = []
    if start_at is not None:
        filters.append(Event.occurred_at >= start_at)

    rows = (
        db.query(
            Student.id,
            Student.external_id,
            Student.email,
            func.min(Event.occurred_at),
            func.max(Event.occurred_at),
            func.count(distinct(Event.course_id)),
        )
        .join(Event, Event.student_id == Student.id)
        .filter(*filters)
        .group_by(Student.id, Student.external_id, Student.email)
        .order_by(Student.id)
        .all()
    )

    students: list[dict] = []

    for (
        student_id,
        external_id,
        email,
        first_seen_at,
        last_seen_at,
        courses_count,
    ) in rows:
        status_label, status_code, status_badge_color = _get_student_status(last_seen_at)

        # Для списка студентов также используем external_id как отображаемое имя
        display_name = external_id

        students.append(
            {
                "id": student_id,
                "external_id": external_id,
                "email": email,
                "display_name": display_name,
                "status_label": status_label,
                "status_code": status_code,
                "status_badge_color": status_badge_color,
                "courses_count": courses_count or 0,
                "first_seen_at": _format_dt_short(first_seen_at),
                "last_seen_at": _format_dt_short(last_seen_at),
            }
        )

    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.args.get(
        "format"
    ) == "json":
        return jsonify(
            period=period,
            period_label=period_label,
            students=[
                {
                    "id": s["id"],
                    "external_id": s["external_id"],
                    "email": s["email"],
                    "display_name": s["display_name"],
                    "status": s["status_code"],
                    "status_label": s["status_label"],
                    "courses_count": s["courses_count"],
                    "first_seen_at": s["first_seen_at"],
                    "last_seen_at": s["last_seen_at"],
                }
                for s in students
            ],
        )

    return render_template(
        "students_list.html",
        students=students,
        period=period,
        period_label=period_label,
    )


@app.route("/behavior")
def behavior_overview():
    db = SessionLocal()

    period, start_at, period_label = _get_period()

    # Метрики по курсам для распределения прогресса
    courses: list[dict] = []
    for course in db.query(Course).order_by(Course.title).all():
        total_students, completed_students, completion_rate = _calculate_course_metrics_for_period(
            db, course.id, start_at
        )

        courses.append(
            {
                "id": course.id,
                "title": course.title,
                "total_students": total_students,
                "completed_students": completed_students,
                "completion_rate": completion_rate,
            }
        )

    progress_distribution = {
        "0_25": 0,
        "25_50": 0,
        "50_75": 0,
        "75_100": 0,
    }
    for course in courses:
        rate = course.get("completion_rate") or 0
        if rate < 25:
            progress_distribution["0_25"] += 1
        elif rate < 50:
            progress_distribution["25_50"] += 1
        elif rate < 75:
            progress_distribution["50_75"] += 1
        else:
            progress_distribution["75_100"] += 1

    # Подробная аналитика по урокам
    lesson_rows = (
        db.query(
            Lesson.id,
            Lesson.title,
            Course.title.label("course_title"),
        )
        .join(Course, Lesson.course_id == Course.id)
        .order_by(Course.title, Lesson.position.nulls_last(), Lesson.id)
        .all()
    )

    lessons_all: list[dict] = []
    for lesson_id, lesson_title, course_title in lesson_rows:
        started_students, completed_lesson_students = _calculate_lesson_metrics_for_period(
            db, lesson_id, start_at
        )

        if not started_students and not completed_lesson_students:
            continue

        drop_off_rate = (
            int(
                (started_students - completed_lesson_students)
                / started_students
                * 100
            )
            if started_students
            else 0
        )

        lessons_all.append(
            {
                "lesson_id": lesson_id,
                "course_title": course_title,
                "lesson_title": lesson_title,
                "started_students": started_students,
                "completed_students": completed_lesson_students,
                "drop_off_rate": drop_off_rate,
            }
        )

    popular_lessons = sorted(
        lessons_all, key=lambda x: x["started_students"], reverse=True
    )
    difficult_lessons = sorted(
        lessons_all, key=lambda x: x["drop_off_rate"], reverse=True
    )

    return render_template(
        "behavior.html",
        period=period,
        period_label=period_label,
        lessons_all=lessons_all,
        popular_lessons=popular_lessons,
        difficult_lessons=difficult_lessons,
        courses_for_progress=courses,
        progress_distribution=progress_distribution,
    )


@app.route("/api/ai-recommendations")
def api_ai_recommendations():
    if not AI_USE_RECOMMENDER:
        return jsonify(status="disabled", items=[])

    db = SessionLocal()
    period, start_at, _ = _get_period()

    courses: list[dict] = []
    total_students_all = 0
    completed_students_all = 0

    for course in db.query(Course).order_by(Course.title).all():
        total_students, completed_students, completion_rate = _calculate_course_metrics_for_period(
            db, course.id, start_at
        )

        courses.append(
            {
                "id": course.id,
                "title": course.title,
                "total_students": total_students,
                "completed_students": completed_students,
                "completion_rate": completion_rate,
            }
        )

        total_students_all += total_students
        completed_students_all += completed_students

    overall_completion_rate = (
        int(completed_students_all / total_students_all * 100)
        if total_students_all
        else 0
    )

    if not courses or total_students_all == 0:
        return jsonify(status="no_data", items=[])

    items = _get_index_ai_recommendations(
        period=period,
        courses=courses,
        total_students_all=total_students_all,
        completed_students_all=completed_students_all,
        overall_completion_rate=overall_completion_rate,
    )

    status = "ok" if items else "empty"
    return jsonify(status=status, items=items)


@app.route("/api/student-ai-insights", methods=["POST"])
def api_student_ai_insights():
    if not AI_USE_RECOMMENDER:
        return jsonify(status="disabled", items=[])

    data = request.get_json(silent=True) or {}

    display_name = str(data.get("display_name") or "Студент")
    status_code = str(data.get("status_code") or "unknown")
    try:
        overall_progress = int(data.get("overall_progress") or 0)
    except (TypeError, ValueError):
        overall_progress = 0

    days_since_last_visit_value = data.get("days_since_last_visit")
    days_since_last_visit: int | None
    try:
        days_since_last_visit = (
            int(days_since_last_visit_value)
            if days_since_last_visit_value is not None
            else None
        )
    except (TypeError, ValueError):
        days_since_last_visit = None

    courses = data.get("courses")
    if not isinstance(courses, list):
        courses = []

    items = _get_student_ai_insights(
        display_name=display_name,
        status_code=status_code,
        overall_progress=overall_progress,
        days_since_last_visit=days_since_last_visit,
        courses=courses,
    )

    status = "ok" if items else "empty"
    return jsonify(status=status, items=items)


def _get_period() -> tuple[str, datetime | None, str]:
    period = request.args.get("period", "all")
    if period not in {"all", "7d", "30d"}:
        period = "all"

    start_at: datetime | None = None
    if period == "7d":
        start_at = datetime.utcnow() - timedelta(days=7)
    elif period == "30d":
        start_at = datetime.utcnow() - timedelta(days=30)

    period_label = {
        "all": "за всё время",
        "7d": "за последние 7 дней",
        "30d": "за последние 30 дней",
    }[period]

    return period, start_at, period_label


def _calculate_course_metrics_for_period(
    db: SessionLocal, course_id: int, start_at: datetime | None
) -> tuple[int, int, int]:
    filters = [Event.course_id == course_id]
    if start_at is not None:
        filters.append(Event.occurred_at >= start_at)

    total_students = (
        db.query(func.count(distinct(Event.student_id)))
        .filter(Event.event_type == EventTypeEnum.ENROLLED.value, *filters)
        .scalar()
        or 0
    )

    completed_students = (
        db.query(func.count(distinct(Event.student_id)))
        .filter(Event.event_type == EventTypeEnum.COURSE_COMPLETED.value, *filters)
        .scalar()
        or 0
    )

    completion_rate = (
        int(completed_students / total_students * 100) if total_students else 0
    )

    return total_students, completed_students, completion_rate


def _calculate_lesson_metrics_for_period(
    db: SessionLocal, lesson_id: int, start_at: datetime | None
) -> tuple[int, int]:
    filters = [Event.lesson_id == lesson_id]
    if start_at is not None:
        filters.append(Event.occurred_at >= start_at)

    started_students = (
        db.query(func.count(distinct(Event.student_id)))
        .filter(Event.event_type == EventTypeEnum.LESSON_STARTED.value, *filters)
        .scalar()
        or 0
    )

    completed_students = (
        db.query(func.count(distinct(Event.student_id)))
        .filter(Event.event_type == EventTypeEnum.LESSON_COMPLETED.value, *filters)
        .scalar()
        or 0
    )

    return started_students, completed_students


def _get_index_ai_recommendations(
    *,
    period: str,
    courses: list[dict],
    total_students_all: int,
    completed_students_all: int,
    overall_completion_rate: int,
) -> list[str]:
    provider = AI_PROVIDER
    if provider == "yandexgpt":
        if not YANDEX_GPT_API_KEY or not YANDEX_GPT_FOLDER_ID:
            return []
    elif provider == "gemini":
        if not GEMINI_API_KEY:
            return []
    else:
        return []

    lines: list[str] = []
    lines.append(f"Период: {period}")
    lines.append(
        f"Всего студентов по всем курсам: {total_students_all}, завершили: {completed_students_all}, средний процент завершения: {overall_completion_rate}%"
    )
    lines.append("Курсы и их метрики:")
    for course in courses:
        lines.append(
            f"- {course['title']}: студентов {course['total_students']}, завершили {course['completed_students']}, процент завершения {course['completion_rate']}%"
        )

    prompt = "\n".join(lines)

    system_text = (
        "Ты опытный продуктовый аналитик онлайн-курсов. На основе переданных метрик по всем курсам предложи 3–5 конкретных рекомендаций по улучшению программ обучения и структуры курсов. Пиши кратко, на русском языке, в формате списка без лишних пояснений."
    )

    if provider == "yandexgpt":
        text = _call_yandex_gpt(system_text=system_text, user_text=prompt)
    elif provider == "gemini":
        text = _call_gemini(system_text=system_text, user_text=prompt)
    else:
        return []
    if not text:
        return []

    recommendations: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped[0] in "-•*":
            stripped = stripped.lstrip("-•* ")
        if stripped[:2].isdigit() and stripped[1] in ".)":
            stripped = stripped[2:].lstrip()
        recommendations.append(stripped)

    return recommendations


def _get_student_ai_insights(
    *,
    display_name: str,
    status_code: str,
    overall_progress: int,
    days_since_last_visit: int | None,
    courses: list[dict],
) -> list[str]:
    provider = AI_PROVIDER
    if provider == "yandexgpt":
        if not YANDEX_GPT_API_KEY or not YANDEX_GPT_FOLDER_ID:
            return []
    elif provider == "gemini":
        if not GEMINI_API_KEY:
            return []
    else:
        return []

    lines: list[str] = []
    lines.append(f"Студент: {display_name}")
    lines.append(f"Текущий статус: {status_code}")
    lines.append(f"Средний прогресс по курсам: {overall_progress}%")
    if days_since_last_visit is not None:
        lines.append(f"Дней с последнего визита: {days_since_last_visit}")
    lines.append("Курсы и прогресс студента:")
    for course in courses:
        title = course.get("title") or "?"
        progress = course.get("progress") or 0
        completed_lessons = course.get("completed_lessons") or 0
        total_lessons = course.get("total_lessons") or 0
        started_lessons = course.get("started_lessons") or 0
        lines.append(
            f"- {title}: прогресс {progress}%, завершено уроков {completed_lessons} из {total_lessons}, начато уроков {started_lessons}"
        )

    prompt = "\n".join(lines)

    system_text = (
        "Ты методист онлайн-курсов. По данным об одном студенте оцени риск недозавершения обучения и предложи 3–5 конкретных рекомендаций, как куратор может помочь этому студенту (что написать, какие материалы предложить, где упростить). Пиши кратко, на русском языке, в формате списка без лишних пояснений."
    )

    if provider == "yandexgpt":
        text = _call_yandex_gpt(system_text=system_text, user_text=prompt)
    elif provider == "gemini":
        text = _call_gemini(system_text=system_text, user_text=prompt)
    else:
        return []
    if not text:
        return []

    recommendations: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped[0] in "-•*":
            stripped = stripped.lstrip("-•* ")
        if stripped[:2].isdigit() and stripped[1] in ".)":
            stripped = stripped[2:].lstrip()
        recommendations.append(stripped)

    return recommendations


def _call_yandex_gpt(*, system_text: str, user_text: str) -> str | None:
    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Api-Key {YANDEX_GPT_API_KEY}",
    }

    body = {
        "modelUri": f"gpt://{YANDEX_GPT_FOLDER_ID}/yandexgpt-lite",
        "completionOptions": {
            "stream": False,
            "temperature": 0.2,
            "maxTokens": "800",
        },
        "messages": [
            {"role": "system", "text": system_text},
            {"role": "user", "text": user_text},
        ],
    }

    data = json.dumps(body).encode("utf-8")

    request_obj = urlrequest.Request(url=url, data=data, headers=headers, method="POST")
    try:
        with urlrequest.urlopen(request_obj, timeout=15) as response:
            response_text = response.read().decode("utf-8")
    except urlerror.URLError:
        return None

    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError:
        return None

    result = payload.get("result")
    if not isinstance(result, dict):
        return None

    alternatives = result.get("alternatives")
    if not isinstance(alternatives, list) or not alternatives:
        return None

    first = alternatives[0]
    if not isinstance(first, dict):
        return None

    message = first.get("message")
    if not isinstance(message, dict):
        return None

    text = message.get("text")
    if not isinstance(text, str):
        return None

    return text


def _call_gemini(*, system_text: str, user_text: str) -> str | None:
    if not GEMINI_API_KEY:
        return None

    model = GEMINI_MODEL or "gemini-1.5-flash"
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={GEMINI_API_KEY}"
    )

    prompt = f"{system_text}\n\nДанные:\n{user_text}"

    body = {
        "contents": [
            {
                "parts": [
                    {"text": prompt},
                ]
            }
        ]
    }

    data = json.dumps(body).encode("utf-8")

    request_obj = urlrequest.Request(url=url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlrequest.urlopen(request_obj, timeout=5) as response:
            response_text = response.read().decode("utf-8")
    except urlerror.URLError:
        return None

    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError:
        return None

    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        return None

    first = candidates[0]
    if not isinstance(first, dict):
        return None

    content = first.get("content")
    if not isinstance(content, dict):
        return None

    parts = content.get("parts")
    if not isinstance(parts, list) or not parts:
        return None

    first_part = parts[0]
    if not isinstance(first_part, dict):
        return None

    text = first_part.get("text")
    if not isinstance(text, str):
        return None

    return text
