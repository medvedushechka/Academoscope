from datetime import datetime, timedelta

from common.db import Base, SessionLocal, engine
from common.models import Course, Lesson, Student, Event, EventTypeEnum


def get_or_create_course(session, external_id: str, title: str) -> Course:
    course = (
        session.query(Course)
        .filter(Course.external_id == external_id)
        .one_or_none()
    )
    if course is None:
        course = Course(external_id=external_id, title=title)
        session.add(course)
        session.flush()
    return course


def get_or_create_lesson(
    session, course: Course, external_id: str, title: str, position: int
) -> Lesson:
    lesson = (
        session.query(Lesson)
        .filter(
            Lesson.course_id == course.id,
            Lesson.external_id == external_id,
        )
        .one_or_none()
    )
    if lesson is None:
        lesson = Lesson(
            course_id=course.id,
            external_id=external_id,
            title=title,
            position=position,
        )
        session.add(lesson)
        session.flush()
    return lesson


def get_or_create_student(session, external_id: str, email: str | None) -> Student:
    student = (
        session.query(Student)
        .filter(Student.external_id == external_id)
        .one_or_none()
    )
    if student is None:
        student = Student(external_id=external_id, email=email)
        session.add(student)
        session.flush()
    else:
        if email and student.email != email:
            student.email = email
    return student


def add_event(
    session,
    *,
    course: Course,
    student: Student,
    lesson: Lesson | None,
    event_type: EventTypeEnum,
    occurred_at: datetime,
) -> None:
    event = Event(
        course_id=course.id,
        student_id=student.id,
        lesson_id=lesson.id if lesson else None,
        event_type=event_type.value,
        occurred_at=occurred_at,
        payload=None,
    )
    session.add(event)


def seed() -> None:
    Base.metadata.create_all(bind=engine)
    session = SessionLocal()

    try:
        now = datetime.utcnow()

        # Курсы: 12 штук, в каждом от 5 до 14 уроков
        courses_def = [
            ("python_basics", "Основы Python", 8),
            ("python_advanced", "Продвинутый Python", 10),
            ("datascience_intro", "Data Science для новичков", 9),
            ("product_analytics", "Продуктовая аналитика", 7),
            ("ml_basics", "Машинное обучение: основы", 12),
            ("web_dev", "Веб-разработка для аналитиков", 14),
            ("sql_analytics", "SQL для аналитиков", 6),
            ("ab_testing", "A/B-тестирование на практике", 5),
            ("marketing_analytics", "Маркетинговая аналитика", 11),
            ("data_viz", "Визуализация данных", 8),
            ("stats_basics", "Статистика для аналитиков", 13),
            ("time_management", "Тайм-менеджмент для студентов", 5),
        ]

        courses: dict[str, Course] = {}
        lessons_by_course: dict[str, list[Lesson]] = {}

        for idx, (c_ext, c_title, lessons_count) in enumerate(courses_def, start=1):
            course = get_or_create_course(session, c_ext, c_title)
            courses[c_ext] = course
            lessons: list[Lesson] = []
            for i in range(1, lessons_count + 1):
                lesson_ext = f"{c_ext}_lesson_{i}"
                lesson_title = f"Урок {i}: {c_title}" if i == 1 else f"Урок {i}"
                lesson = get_or_create_lesson(
                    session,
                    course,
                    lesson_ext,
                    lesson_title,
                    i,
                )
                lessons.append(lesson)
            lessons_by_course[c_ext] = lessons

        students_def = [
            ("Иван Петров", "ivan.petrov@example.com"),
            ("Мария Смирнова", "maria.smirnova@example.com"),
            ("Алексей Кузнецов", "alexey.kuznetsov@example.com"),
            ("Ольга Иванова", "olga.ivanova@example.com"),
            ("Дмитрий Соколов", "dmitry.sokolov@example.com"),
            ("Анна Морозова", "anna.morozova@example.com"),
            ("Сергей Волков", "sergey.volkov@example.com"),
            ("Екатерина Федорова", "ekaterina.fedorova@example.com"),
            ("Павел Никитин", "pavel.nikitin@example.com"),
            ("Юлия Сергеева", "yulia.sergeeva@example.com"),
        ]

        students: list[Student] = []
        for s_ext, s_email in students_def:
            students.append(get_or_create_student(session, s_ext, s_email))

        course_keys = list(courses.keys())

        # Сценарии активности для разных студентов
        for idx, student in enumerate(students):
            # Разбрасываем по временной шкале последних ~60 дней
            base_offset_days = idx * 4

            # Для каждого студента берём два разных курса так, чтобы все курсы получили активность
            primary_idx = (idx * 2) % len(course_keys)
            secondary_idx = (idx * 2 + 1) % len(course_keys)

            for course_idx, is_primary in ((primary_idx, True), (secondary_idx, False)):
                course_key = course_keys[course_idx]
                course = courses[course_key]
                lessons = lessons_by_course[course_key]

                # Более ранний старт для "основного" курса, более поздний — для второго
                offset_days = base_offset_days + (30 if is_primary else 15)
                enrolled_at = now - timedelta(days=offset_days)

                add_event(
                    session,
                    course=course,
                    student=student,
                    lesson=None,
                    event_type=EventTypeEnum.ENROLLED,
                    occurred_at=enrolled_at,
                )

                if not lessons:
                    continue

                # Количество уроков, до которых студент дошёл в этом курсе
                completed_lessons_count = (idx % len(lessons)) + 1

                for i, lesson in enumerate(lessons[:completed_lessons_count], start=1):
                    start_time = enrolled_at + timedelta(days=i)
                    add_event(
                        session,
                        course=course,
                        student=student,
                        lesson=lesson,
                        event_type=EventTypeEnum.LESSON_STARTED,
                        occurred_at=start_time,
                    )

                    # Только по основному курсу часть уроков помечаем как завершённые
                    if is_primary and i % 2 == 1:
                        complete_time = start_time + timedelta(hours=1)
                        add_event(
                            session,
                            course=course,
                            student=student,
                            lesson=lesson,
                            event_type=EventTypeEnum.LESSON_COMPLETED,
                            occurred_at=complete_time,
                        )

                # Некоторым студентам даём завершить основной курс целиком
                if is_primary and idx % 3 == 0:
                    completed_time = enrolled_at + timedelta(
                        days=completed_lessons_count + 3
                    )
                    add_event(
                        session,
                        course=course,
                        student=student,
                        lesson=None,
                        event_type=EventTypeEnum.COURSE_COMPLETED,
                        occurred_at=completed_time,
                    )

        session.commit()
        print("Demo data seeded successfully.")
    finally:
        session.close()


if __name__ == "__main__":
    seed()
