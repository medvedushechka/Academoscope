import csv
import io
import json
import os
from datetime import datetime, timedelta
from types import SimpleNamespace
from urllib import error as urlerror
from urllib import request as urlrequest

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from sqlalchemy import case, distinct, func
from sqlalchemy.orm import scoped_session, sessionmaker

from common.db import Base, engine
from common.models import (
    Course,
    CourseMetrics,
    Event,
    EventTypeEnum,
    Lesson,
    LessonMetrics,
    ScheduleSlot,
    Student,
    Teacher,
)


# Создаём таблицы (на случай, если дашборд запускается отдельно от API).
Base.metadata.create_all(bind=engine)

SessionLocal = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)

app = Flask(__name__)

AI_USE_RECOMMENDER = os.getenv("AI_USE_RECOMMENDER", "false").lower() == "true"
# На данный момент поддерживается только Gemini как AI-провайдер.
AI_PROVIDER = os.getenv("AI_PROVIDER", "gemini").lower()
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
    # Округляем отображение до секунд, без миллисекунд
    return value.strftime("%Y-%m-%d %H:%M:%S")


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

        rate = completion_rate or 0

        courses.append(
            {
                "id": course.id,
                "title": course.title,
                "total_students": total_students,
                "completed_students": completed_students,
                "completion_rate": completion_rate,
                "problem_course": rate < 50,
                "inactive_30d": False,
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

        days_since_last_visit: int | None = None
        if last_seen is not None:
            days_since_last_visit = (datetime.utcnow() - last_seen).days

        risk_students.append(
            {
                "id": student_id,
                "display_name": display_name,
                "email": email,
                "last_seen_at": _format_dt_short(last_seen),
                "days_since_last_visit": days_since_last_visit,
                "status_label": status_label,
                "status_code": status_code,
                "status_badge_color": status_badge_color,
            }
        )

    # Сначала показываем тех, кто дольше всего не заходил
    risk_students.sort(
        key=lambda s: (s["days_since_last_visit"] or -1),
        reverse=True,
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
            Course.id.label("course_id"),
            Course.title.label("course_title"),
        )
        .join(Course, Lesson.course_id == Course.id)
        .all()
    )

    for lesson_id, lesson_title, course_id, course_title in lesson_rows:
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
                "course_id": course_id,
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
                "course_id": course_id,
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

    # Обзор преподавателей и расписания для директора
    total_teachers = db.query(func.count(Teacher.id)).scalar() or 0

    today = datetime.utcnow().date()
    schedule_start = datetime.combine(today, datetime.min.time())
    schedule_end = schedule_start + timedelta(days=7)

    upcoming_slots_query = db.query(ScheduleSlot).filter(
        ScheduleSlot.start_at >= schedule_start,
        ScheduleSlot.start_at < schedule_end,
    )

    upcoming_slots_count = upcoming_slots_query.count()

    active_teachers_7d = (
        db.query(func.count(distinct(ScheduleSlot.teacher_id)))
        .filter(
            ScheduleSlot.start_at >= schedule_start,
            ScheduleSlot.start_at < schedule_end,
        )
        .scalar()
        or 0
    )

    upcoming_slots_raw: list[ScheduleSlot] = (
        upcoming_slots_query.order_by(ScheduleSlot.start_at.asc()).limit(5).all()
    )

    upcoming_slots: list[dict] = []
    for slot in upcoming_slots_raw:
        start = slot.start_at
        end = slot.end_at
        course = slot.course
        lesson = slot.lesson
        teacher = slot.teacher

        upcoming_slots.append(
            {
                "id": slot.id,
                "teacher_id": slot.teacher_id,
                "start_dt": start,
                "start_date": start.date().isoformat(),
                "start_time": start.strftime("%H:%M"),
                "end_time": end.strftime("%H:%M") if end else None,
                "course_title": course.title if course else "?",
                "lesson_title": lesson.title if lesson else None,
                "teacher_name": teacher.name if teacher else "?",
                "group_name": slot.group_name,
                "location": slot.location,
            }
        )

    # Дополнительные KPI под директора
    problem_courses_count = len(
        [c for c in courses if (c.get("completion_rate") or 0) < 50]
    )
    risk_students_count = len(risk_students)

    active_course_ids_30d = set(
        row[0]
        for row in db.query(distinct(Event.course_id))
        .filter(
            Event.occurred_at >= datetime.utcnow() - timedelta(days=30),
            Event.course_id.isnot(None),
        )
        .all()
        if row[0] is not None
    )
    for c in courses:
        c["inactive_30d"] = c["id"] not in active_course_ids_30d

    inactive_courses_30d_count = len(
        [c for c in courses if c["inactive_30d"]]
    )

    # Демонстрационные, но более реалистичные данные для финансовых блоков
    # Распределяем общее число студентов по каналам и считаем LTV/продажи на их основе.
    channel_templates: list[tuple[str, float, float, int]] = [
        ("Таргет ВК", 0.4, 3.2, 60),
        ("YouTube и контент", 0.25, 1.8, 70),
        ("Telegram / комьюнити", 0.2, 4.5, 75),
        ("Партнёры и рекомендации", 0.15, 7.5, 82),
    ]

    total_students_for_finance = max(total_students_all, 0)
    finance_channels: list[dict] = []

    if total_students_for_finance > 0:
        remaining_students = total_students_for_finance
        for idx, (name, share, conversion_rate, completion_rate) in enumerate(
            channel_templates
        ):
            if idx == len(channel_templates) - 1:
                students = remaining_students
            else:
                students = int(total_students_for_finance * share)
                remaining_students -= students

            # Простая модель LTV: базовая сумма + надбавка за высокую завершённость.
            ltv = 8000 + int(completion_rate * 160)

            finance_channels.append(
                {
                    "name": name,
                    "students": max(students, 0),
                    "conversion_rate": conversion_rate,
                    "completion_rate": completion_rate,
                    "ltv": ltv,
                }
            )
    else:
        # Если данных по студентам нет, показываем небольшие демо-значения.
        for name, conversion_rate, completion_rate, ltv in [
            ("Таргет ВК", 3.0, 60, 12000),
            ("YouTube и контент", 1.8, 70, 14000),
        ]:
            finance_channels.append(
                {
                    "name": name,
                    "students": 0,
                    "conversion_rate": conversion_rate,
                    "completion_rate": completion_rate,
                    "ltv": ltv,
                }
            )

    # Продажи и возвраты по неделям: раскладываем суммарные продажи по 4 неделям.
    # Берём продажи как примерно число студентов за период, возвраты ~5%.
    total_sales = max(total_students_for_finance, 0)
    weekly_shares = [0.2, 0.3, 0.25, 0.25]
    refund_rate = 0.05

    finance_sales: list[dict] = []
    remaining_sales = total_sales
    for idx, share in enumerate(weekly_shares):
        if idx == len(weekly_shares) - 1:
            sales = remaining_sales
        else:
            sales = int(total_sales * share)
            remaining_sales -= sales

        refunds = int(sales * refund_rate)
        finance_sales.append(
            {
                "label": f"Неделя {idx + 1}",
                "sales": max(sales, 0),
                "refunds": max(refunds, 0),
            }
        )

    # Средний LTV и оценка выручки: взвешенное среднее по каналам.
    if finance_channels and total_students_for_finance > 0:
        total_ltv_sum = sum(
            ch["ltv"] * max(ch["students"], 0) for ch in finance_channels
        )
        avg_ltv = int(total_ltv_sum / max(total_students_for_finance, 1))
        paying_students = total_students_for_finance
    else:
        avg_ltv = 0
        paying_students = 0

    finance_ltv = SimpleNamespace(
        avg_ltv=avg_ltv,
        paying_students=paying_students,
        estimated_revenue=avg_ltv * paying_students,
    )

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
        problem_courses_count=problem_courses_count,
        risk_students_count=risk_students_count,
        inactive_courses_30d_count=inactive_courses_30d_count,
        total_teachers=total_teachers,
        active_teachers_7d=active_teachers_7d,
        upcoming_slots=upcoming_slots,
        upcoming_slots_count=upcoming_slots_count,
        period=period,
        period_label=period_label,
        finance_channels=finance_channels,
        finance_sales=finance_sales,
        finance_ltv=finance_ltv,
    )


@app.route("/teachers/<int:teacher_id>")
def teacher_detail(teacher_id: int):
    db = SessionLocal()

    teacher = db.query(Teacher).filter(Teacher.id == teacher_id).one_or_none()
    if teacher is None:
        abort(404)

    filters = _get_schedule_filters()

    slots: list[ScheduleSlot] = (
        db.query(ScheduleSlot)
        .filter(
            ScheduleSlot.teacher_id == teacher.id,
            ScheduleSlot.start_at >= filters.start_dt,
            ScheduleSlot.start_at < filters.end_dt,
        )
        .order_by(ScheduleSlot.start_at.asc())
        .all()
    )

    slots_count = len(slots)
    course_ids = {slot.course_id for slot in slots if slot.course_id is not None}

    total_hours = 0.0
    for slot in slots:
        start = slot.start_at
        end = slot.end_at or (slot.start_at + timedelta(hours=1))
        total_hours += max((end - start).total_seconds() / 3600.0, 0.0)

    first_slot_date: str | None = None
    last_slot_date: str | None = None
    if slots:
        first_slot_date = slots[0].start_at.date().isoformat()
        last_slot_date = slots[-1].start_at.date().isoformat()

    schedule_rows: list[dict] = []
    for slot in slots:
        start = slot.start_at
        end = slot.end_at
        course = slot.course
        lesson = slot.lesson

        schedule_rows.append(
            {
                "id": slot.id,
                "start_date": start.date().isoformat(),
                "start_time": start.strftime("%H:%M"),
                "end_time": end.strftime("%H:%M") if end else None,
                "course_title": course.title if course else "?",
                "lesson_title": lesson.title if lesson else None,
                "group_name": slot.group_name,
                "location": slot.location,
            }
        )

    schedule_by_date: dict[str, list[dict]] = {}
    for row in schedule_rows:
        date_key = row["start_date"]
        schedule_by_date.setdefault(date_key, []).append(row)

    schedule_dates = sorted(schedule_by_date.keys())

    # KPI и список проблемных курсов преподавателя за период
    teacher_problem_courses: list[dict] = []
    teacher_courses_stats: list[dict] = []
    teacher_total_students = 0
    teacher_completed_students = 0
    if course_ids:
        for course in (
            db.query(Course)
            .filter(Course.id.in_(course_ids))
            .order_by(Course.title)
            .all()
        ):
            total_students, completed_students, completion_rate = _calculate_course_metrics_for_period(  # noqa: E501
                db, course.id, filters.start_dt
            )
            rate = completion_rate or 0

            stat = {
                "id": course.id,
                "title": course.title,
                "total_students": total_students,
                "completed_students": completed_students,
                "completion_rate": rate,
            }
            teacher_courses_stats.append(stat)

            teacher_total_students += total_students
            teacher_completed_students += completed_students

            if rate < 50:
                teacher_problem_courses.append(stat)

    teacher_avg_completion_rate = (
        int(teacher_completed_students / teacher_total_students * 100)
        if teacher_total_students
        else 0
    )

    metrics = SimpleNamespace(
        slots_count=slots_count,
        courses_count=len(course_ids),
        total_hours=int(round(total_hours)),
        first_slot_date=first_slot_date,
        last_slot_date=last_slot_date,
        problem_courses_count=len(teacher_problem_courses),
        students_total=teacher_total_students,
        students_completed=teacher_completed_students,
        avg_completion_rate=teacher_avg_completion_rate,
    )

    # Простые текстовые инсайты по преподавателю
    teacher_insights: list[str] = []

    if teacher_total_students == 0:
        teacher_insights.append(
            "В выбранном периоде у преподавателя нет студентов — можно планировать для него дополнительные занятия."
        )
    else:
        if teacher_avg_completion_rate >= 80:
            teacher_insights.append(
                "Высокий средний процент завершения курсов у студентов этого преподавателя."
            )
        elif teacher_avg_completion_rate >= 50:
            teacher_insights.append(
                "Средний процент завершения курсов на уровне нормы."
            )
        else:
            teacher_insights.append(
                "Низкий средний процент завершения курсов — стоит проверить содержание материалов и поддержку студентов."
            )

        if len(teacher_problem_courses) == 0 and course_ids:
            teacher_insights.append(
                "В выбранном периоде нет проблемных курсов (ниже 50% завершения) по этому преподавателю."
            )
        elif len(teacher_problem_courses) == 1:
            teacher_insights.append(
                "Есть 1 проблемный курс с низкой завершённостью — имеет смысл уделить ему особое внимание."
            )
        elif len(teacher_problem_courses) > 1:
            teacher_insights.append(
                f"Есть {len(teacher_problem_courses)} проблемных курса с низкой завершённостью — приоритизируйте их улучшение."
            )

        if metrics.slots_count <= 3 and metrics.courses_count <= 1:
            teacher_insights.append(
                "Низкая нагрузка по занятиям — можно рассмотреть увеличение числа групп или курсов."
            )
        elif metrics.slots_count >= 15:
            teacher_insights.append(
                "Высокая нагрузка по занятиям — при планировании стоит учитывать возможное выгорание."
            )

    # Навигация по периоду (шагами по 7 дней)
    try:
        current_start = datetime.strptime(filters.start_date_str, "%Y-%m-%d").date()
        current_end = datetime.strptime(filters.end_date_str, "%Y-%m-%d").date()
    except ValueError:
        current_start = datetime.utcnow().date()
        current_end = current_start

    prev_start = (current_start - timedelta(days=7)).isoformat()
    prev_end = (current_end - timedelta(days=7)).isoformat()
    next_start = (current_start + timedelta(days=7)).isoformat()
    next_end = (current_end + timedelta(days=7)).isoformat()

    period_nav = SimpleNamespace(
        prev_start=prev_start,
        prev_end=prev_end,
        next_start=next_start,
        next_end=next_end,
    )

    return render_template(
        "teacher_detail.html",
        teacher=teacher,
        filters=filters,
        metrics=metrics,
        period_nav=period_nav,
        teacher_courses_stats=teacher_courses_stats,
        teacher_courses_labels=[c["title"] for c in teacher_courses_stats],
        teacher_courses_completion_rates=[
            c["completion_rate"] for c in teacher_courses_stats
        ],
        teacher_problem_courses=teacher_problem_courses,
        teacher_insights=teacher_insights,
        schedule_rows=schedule_rows,
        schedule_by_date=schedule_by_date,
        schedule_dates=schedule_dates,
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

    # Для списка студентов считаем также количество завершённых курсов,
    # чтобы приблизительно оценить средний прогресс по курсам.
    completed_courses_expr = func.count(
        distinct(
            case(
                (Event.event_type == EventTypeEnum.COURSE_COMPLETED.value, Event.course_id),
                else_=None,
            )
        )
    )

    rows = (
        db.query(
            Student.id,
            Student.external_id,
            Student.email,
            func.min(Event.occurred_at),
            func.max(Event.occurred_at),
            func.count(distinct(Event.course_id)),
            completed_courses_expr,
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
        completed_courses_count,
    ) in rows:
        status_label, status_code, status_badge_color = _get_student_status(last_seen_at)

        # Для списка студентов также используем external_id как отображаемое имя
        display_name = external_id

        completed_courses_count = completed_courses_count or 0
        courses_count = courses_count or 0

        overall_progress = (
            int(completed_courses_count / courses_count * 100)
            if courses_count
            else 0
        )

        students.append(
            {
                "id": student_id,
                "external_id": external_id,
                "email": email,
                "display_name": display_name,
                "status_label": status_label,
                "status_code": status_code,
                "status_badge_color": status_badge_color,
                "courses_count": courses_count,
                "overall_progress": overall_progress,
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
                    "overall_progress": s["overall_progress"],
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


@app.route("/teachers")
def teachers_list():
    """Список преподавателей и их нагрузка за выбранный период.

    Используем те же фильтры дат, что и для расписания (см. _get_schedule_filters).
    """

    db = SessionLocal()

    filters = _get_schedule_filters()

    teachers = db.query(Teacher).order_by(Teacher.name).all()

    teacher_rows: list[dict] = []

    for teacher in teachers:
        slots: list[ScheduleSlot] = (
            db.query(ScheduleSlot)
            .filter(
                ScheduleSlot.teacher_id == teacher.id,
                ScheduleSlot.start_at >= filters.start_dt,
                ScheduleSlot.start_at < filters.end_dt,
            )
            .order_by(ScheduleSlot.start_at.asc())
            .all()
        )

        if not slots:
            # Для директора иногда полезно видеть и преподавателей без слотов,
            # чтобы понимать, кого можно задействовать.
            teacher_rows.append(
                {
                    "id": teacher.id,
                    "name": teacher.name,
                    "email": teacher.email,
                    "slots_count": 0,
                    "courses_count": 0,
                    "total_hours": 0,
                    "first_slot_date": None,
                    "last_slot_date": None,
                    "problem_courses_count": 0,
                }
            )
            continue

        slots_count = len(slots)
        course_ids = {slot.course_id for slot in slots if slot.course_id is not None}

        total_hours = 0.0
        for slot in slots:
            start = slot.start_at
            end = slot.end_at or (slot.start_at + timedelta(hours=1))
            total_hours += max((end - start).total_seconds() / 3600.0, 0.0)

        first_slot = slots[0].start_at
        last_slot = slots[-1].start_at

        # Проблемные курсы конкретного преподавателя (по данному периоду)
        problem_courses_count = 0
        if course_ids:
            for course in (
                db.query(Course)
                .filter(Course.id.in_(course_ids))
                .order_by(Course.title)
                .all()
            ):
                _total_students, _completed_students, completion_rate = _calculate_course_metrics_for_period(  # noqa: E501
                    db, course.id, filters.start_dt
                )
                rate = completion_rate or 0
                if rate < 50:
                    problem_courses_count += 1

        teacher_rows.append(
            {
                "id": teacher.id,
                "name": teacher.name,
                "email": teacher.email,
                "slots_count": slots_count,
                "courses_count": len(course_ids),
                "total_hours": int(round(total_hours)),
                "first_slot_date": first_slot.date().isoformat(),
                "last_slot_date": last_slot.date().isoformat(),
                "problem_courses_count": problem_courses_count,
            }
        )

    return render_template(
        "teachers_list.html",
        teachers=teacher_rows,
        filters=filters,
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
            Course.id.label("course_id"),
            Course.title.label("course_title"),
        )
        .join(Course, Lesson.course_id == Course.id)
        .order_by(Course.title, Lesson.position.nulls_last(), Lesson.id)
        .all()
    )

    lessons_all: list[dict] = []
    for lesson_id, lesson_title, course_id, course_title in lesson_rows:
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
                "course_id": course_id,
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


@app.route("/schedule")
def schedule_view():
    """Просмотр расписания занятий.

    Для MVP отображаем список слотов с фильтрами по дате, курсу и преподавателю.
    """

    db = SessionLocal()

    filters = _get_schedule_filters()

    query = (
        db.query(ScheduleSlot)
        .filter(
            ScheduleSlot.start_at >= filters.start_dt,
            ScheduleSlot.start_at < filters.end_dt,
        )
        .order_by(ScheduleSlot.start_at.asc())
    )

    if filters.teacher_id is not None:
        query = query.filter(ScheduleSlot.teacher_id == filters.teacher_id)
    if filters.course_id is not None:
        query = query.filter(ScheduleSlot.course_id == filters.course_id)

    slots: list[ScheduleSlot] = query.all()

    # Определяем конфликты по преподавателям: слоты, которые пересекаются по времени.
    conflict_slot_ids: set[int] = set()
    slots_by_teacher: dict[int, list[ScheduleSlot]] = {}
    for slot in slots:
        if slot.teacher_id is None:
            continue
        slots_by_teacher.setdefault(slot.teacher_id, []).append(slot)

    for teacher_id, teacher_slots in slots_by_teacher.items():
        teacher_slots.sort(key=lambda s: s.start_at)
        for i, current in enumerate(teacher_slots[:-1]):
            next_slot = teacher_slots[i + 1]
            current_end = current.end_at or (current.start_at + timedelta(hours=1))
            next_start = next_slot.start_at
            if next_start < current_end:
                conflict_slot_ids.add(current.id)
                conflict_slot_ids.add(next_slot.id)

    teachers = db.query(Teacher).order_by(Teacher.name).all()
    courses = db.query(Course).order_by(Course.title).all()

    # Список уроков для выпадающего списка (опционально при создании слота)
    lesson_rows = (
        db.query(
            Lesson.id,
            Lesson.title,
            Lesson.course_id,
            Course.title.label("course_title"),
        )
        .join(Course, Lesson.course_id == Course.id)
        .order_by(Course.title, Lesson.position.nulls_last(), Lesson.id)
        .all()
    )

    lessons: list[dict] = []
    for lesson_id, lesson_title, course_id, course_title in lesson_rows:
        lessons.append(
            {
                "id": lesson_id,
                "title": lesson_title,
                "course_id": course_id,
                "course_title": course_title,
            }
        )

    schedule_rows: list[dict] = []
    for slot in slots:
        start = slot.start_at
        end = slot.end_at
        course = slot.course
        lesson = slot.lesson
        teacher = slot.teacher

        schedule_rows.append(
            {
                "id": slot.id,
                "course_id": slot.course_id,
                "lesson_id": slot.lesson_id,
                "teacher_id": slot.teacher_id,
                "start_dt": start,
                "start_date": start.date().isoformat(),
                "start_time": start.strftime("%H:%M"),
                "end_time": end.strftime("%H:%M") if end else None,
                "course_title": course.title if course else "?",
                "lesson_title": lesson.title if lesson else None,
                "teacher_name": teacher.name if teacher else "?",
                "group_name": slot.group_name,
                "location": slot.location,
                "has_conflict": slot.id in conflict_slot_ids,
            }
        )

    # Подготовка данных для календарного вида по дням
    schedule_by_date: dict[str, list[dict]] = {}
    for row in schedule_rows:
        date_key = row["start_date"]
        schedule_by_date.setdefault(date_key, []).append(row)

    ordered_dates = sorted(schedule_by_date.keys())

    return render_template(
        "schedule.html",
        schedule_rows=schedule_rows,
        teachers=teachers,
        courses=courses,
        lessons=lessons,
        schedule_by_date=schedule_by_date,
        schedule_dates=ordered_dates,
        filters=filters,
    )


@app.route("/schedule/new", methods=["POST"])
def schedule_create():
    """Создать новый слот расписания из формы на странице расписания."""

    db = SessionLocal()

    form = request.form

    date_str = form.get("date")
    start_time_str = form.get("start_time")
    end_time_str = form.get("end_time")
    course_id_str = form.get("course_id")
    teacher_id_str = form.get("teacher_id")
    lesson_id_str = form.get("lesson_id")
    group_name = form.get("group_name") or None
    location = form.get("location") or None

    try:
        course_id = int(course_id_str) if course_id_str is not None else None
        teacher_id = int(teacher_id_str) if teacher_id_str is not None else None
        lesson_id = int(lesson_id_str) if lesson_id_str else None
    except (TypeError, ValueError):
        return redirect(url_for("schedule_view"))

    if not course_id or not teacher_id or not date_str or not start_time_str:
        return redirect(url_for("schedule_view"))

    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        start_time_obj = datetime.strptime(start_time_str, "%H:%M").time()
        end_time_obj = (
            datetime.strptime(end_time_str, "%H:%M").time()
            if end_time_str
            else None
        )
    except ValueError:
        return redirect(url_for("schedule_view"))

    start_at = datetime.combine(date_obj, start_time_obj)
    end_at = datetime.combine(date_obj, end_time_obj) if end_time_obj else None

    slot = ScheduleSlot(
        course_id=course_id,
        lesson_id=lesson_id,
        teacher_id=teacher_id,
        start_at=start_at,
        end_at=end_at,
        group_name=group_name,
        location=location,
    )

    db.add(slot)
    db.commit()

    return redirect(
        url_for(
            "schedule_view",
            start=date_obj.isoformat(),
            end=date_obj.isoformat(),
            course_id=course_id,
            teacher_id=teacher_id,
        )
    )


@app.route("/schedule/<int:slot_id>/delete", methods=["POST"])
def schedule_delete(slot_id: int):
    db = SessionLocal()

    slot = (
        db.query(ScheduleSlot)
        .filter(ScheduleSlot.id == slot_id)
        .one_or_none()
    )
    if slot is None:
        return redirect(url_for("schedule_view"))

    start_date = slot.start_at.date().isoformat()
    course_id = slot.course_id
    teacher_id = slot.teacher_id

    db.delete(slot)
    db.commit()

    return redirect(
        url_for(
            "schedule_view",
            start=start_date,
            end=start_date,
            course_id=course_id,
            teacher_id=teacher_id,
        )
    )


@app.route("/schedule/<int:slot_id>/edit", methods=["POST"])
def schedule_edit(slot_id: int):
    """Обновление существующего слота расписания из формы."""

    db = SessionLocal()

    slot = (
        db.query(ScheduleSlot)
        .filter(ScheduleSlot.id == slot_id)
        .one_or_none()
    )
    if slot is None:
        return redirect(url_for("schedule_view"))

    form = request.form

    date_str = form.get("date")
    start_time_str = form.get("start_time")
    end_time_str = form.get("end_time")
    course_id_str = form.get("course_id")
    teacher_id_str = form.get("teacher_id")
    lesson_id_str = form.get("lesson_id")
    group_name = form.get("group_name") or None
    location = form.get("location") or None

    try:
        course_id = int(course_id_str) if course_id_str is not None else None
        teacher_id = int(teacher_id_str) if teacher_id_str is not None else None
        lesson_id = int(lesson_id_str) if lesson_id_str else None
    except (TypeError, ValueError):
        return redirect(url_for("schedule_view"))

    if not course_id or not teacher_id or not date_str or not start_time_str:
        return redirect(url_for("schedule_view"))

    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        start_time_obj = datetime.strptime(start_time_str, "%H:%M").time()
        end_time_obj = (
            datetime.strptime(end_time_str, "%H:%M").time()
            if end_time_str
            else None
        )
    except ValueError:
        return redirect(url_for("schedule_view"))

    slot.course_id = course_id
    slot.teacher_id = teacher_id
    slot.lesson_id = lesson_id
    slot.start_at = datetime.combine(date_obj, start_time_obj)
    slot.end_at = datetime.combine(date_obj, end_time_obj) if end_time_obj else None
    slot.group_name = group_name
    slot.location = location

    db.commit()

    return redirect(
        url_for(
            "schedule_view",
            start=date_obj.isoformat(),
            end=date_obj.isoformat(),
            course_id=course_id,
            teacher_id=teacher_id,
        )
    )


@app.route("/schedule/export")
def schedule_export():
    """Экспорт расписания в CSV-файл (удобно открывать в Excel)."""

    db = SessionLocal()
    filters = _get_schedule_filters()

    query = (
        db.query(ScheduleSlot)
        .filter(
            ScheduleSlot.start_at >= filters.start_dt,
            ScheduleSlot.start_at < filters.end_dt,
        )
        .order_by(ScheduleSlot.start_at.asc())
    )

    if filters.teacher_id is not None:
        query = query.filter(ScheduleSlot.teacher_id == filters.teacher_id)
    if filters.course_id is not None:
        query = query.filter(ScheduleSlot.course_id == filters.course_id)

    slots: list[ScheduleSlot] = query.all()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(
        [
            "ID",
            "Дата",
            "Время начала",
            "Время окончания",
            "Курс",
            "Урок",
            "Преподаватель",
            "Группа",
            "Локация",
        ]
    )

    for slot in slots:
        start = slot.start_at
        end = slot.end_at
        course = slot.course
        lesson = slot.lesson
        teacher = slot.teacher

        writer.writerow(
            [
                slot.id,
                start.date().isoformat(),
                start.strftime("%H:%M"),
                end.strftime("%H:%M") if end else "",
                course.title if course else "",
                lesson.title if lesson else "",
                teacher.name if teacher else "",
                slot.group_name or "",
                slot.location or "",
            ]
        )

    csv_text = output.getvalue()
    output.close()

    filename = f"schedule_{filters.start_date_str}_to_{filters.end_date_str}.csv"

    return Response(
        csv_text,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
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

    try:
        items = _get_index_ai_recommendations(
            period=period,
            courses=courses,
            total_students_all=total_students_all,
            completed_students_all=completed_students_all,
            overall_completion_rate=overall_completion_rate,
        )
        status = "ok" if items else "empty"
        return jsonify(status=status, items=items)
    except Exception as exc:  # защитный лог на случай неожиданных ошибок
        print(f"[AI] /api/ai-recommendations error: {exc}")
        return jsonify(status="error", items=[])


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

    try:
        items = _get_student_ai_insights(
            display_name=display_name,
            status_code=status_code,
            overall_progress=overall_progress,
            days_since_last_visit=days_since_last_visit,
            courses=courses,
        )

        status = "ok" if items else "empty"
        return jsonify(status=status, items=items)
    except Exception as exc:
        print(f"[AI] /api/student-ai-insights error: {exc}")
        return jsonify(status="error", items=[])


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


def _get_schedule_filters() -> SimpleNamespace:
    """Получить параметры фильтрации расписания из query-параметров.

    Используем диапазон дат (start/end), а также опциональные фильтры по
    преподавателю и курсу. По умолчанию показываем промежуток ±7 дней от
    текущей даты.
    """

    teacher_param = request.args.get("teacher_id")
    course_param = request.args.get("course_id")
    start_str = request.args.get("start")
    end_str = request.args.get("end")

    now = datetime.utcnow()
    default_start_date = (now - timedelta(days=7)).date()
    default_end_date = (now + timedelta(days=7)).date()

    def _parse_date(value: str | None, default_date: datetime.date) -> datetime.date:  # type: ignore[name-defined]
        if not value:
            return default_date
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return default_date

    start_date = _parse_date(start_str, default_start_date)
    end_date = _parse_date(end_str, default_end_date)

    # Диапазон по времени: [start_dt, end_dt)
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())

    teacher_id: int | None = None
    if teacher_param:
        try:
            teacher_id = int(teacher_param)
        except ValueError:
            teacher_id = None

    course_id: int | None = None
    if course_param:
        try:
            course_id = int(course_param)
        except ValueError:
            course_id = None

    return SimpleNamespace(
        start_dt=start_dt,
        end_dt=end_dt,
        start_date_str=start_date.isoformat(),
        end_date_str=end_date.isoformat(),
        teacher_id=teacher_id,
        course_id=course_id,
    )


def _get_index_ai_recommendations(
    *,
    period: str,
    courses: list[dict],
    total_students_all: int,
    completed_students_all: int,
    overall_completion_rate: int,
) -> list[str]:
    provider = AI_PROVIDER
    if provider != "gemini":
        print(f"[AI] Unknown AI_PROVIDER: {provider}")
        return []

    if not GEMINI_API_KEY:
        print("[AI] Gemini disabled: missing GEMINI_API_KEY")
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

    # На текущем этапе поддерживаем только Gemini
    text = _call_gemini(system_text=system_text, user_text=prompt)

    # Если провайдер вернул None или пустую строку, считаем это ошибкой, чтобы
    # роут /api/ai-recommendations отдал status="error", а не "empty".
    if not text:
        print("[AI] _get_index_ai_recommendations: model returned empty text")
        raise RuntimeError("AI provider returned empty text")

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
    if provider != "gemini":
        print(f"[AI] Unknown AI_PROVIDER: {provider}")
        return []

    if not GEMINI_API_KEY:
        print("[AI] Gemini disabled: missing GEMINI_API_KEY")
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

    # На текущем этапе поддерживаем только Gemini
    text = _call_gemini(system_text=system_text, user_text=prompt)

    # Аналогично индексу: отсутствие текста от провайдера считаем ошибкой,
    # чтобы /api/student-ai-insights вернул status="error".
    if not text:
        print("[AI] _get_student_ai_insights: model returned empty text")
        raise RuntimeError("AI provider returned empty text")

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
        # Увеличиваем таймаут, чтобы дать модели время ответить
        with urlrequest.urlopen(request_obj, timeout=40) as response:
            response_text = response.read().decode("utf-8")
    except urlerror.URLError as exc:
        print(f"[AI] Gemini request error: {exc}")
        return None

    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError as exc:
        # Логируем, если Gemini вернул невалидный JSON
        print(
            f"[AI] Gemini JSON decode error: {exc}; raw={response_text[:400]!r}"
        )
        return None

    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        print("[AI] Gemini: empty candidates")
        return None

    first = candidates[0]
    if not isinstance(first, dict):
        print("[AI] Gemini: first candidate is not a dict")
        return None

    content = first.get("content")
    if not isinstance(content, dict):
        print("[AI] Gemini: unexpected content format")
        return None

    parts = content.get("parts")
    if not isinstance(parts, list) or not parts:
        print("[AI] Gemini: empty parts")
        return None

    first_part = parts[0]
    if not isinstance(first_part, dict):
        print("[AI] Gemini: unexpected part format")
        return None

    text = first_part.get("text")
    if not isinstance(text, str):
        print("[AI] Gemini: text is not a string")
        return None

    # Успешный разбор ответа от Gemini
    print(
        f"[AI] Gemini: got text of length {len(text)} from model {model}; candidates={len(candidates)}"
    )

    return text
