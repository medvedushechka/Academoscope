import json
import os
import random
import string
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
from urllib import request


API_URL = os.getenv("ACAD_API_URL", "http://127.0.0.1:8000/events")


def _random_email(prefix: str) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
    return f"{prefix}_{suffix}@example.com"


def send_event(payload: Dict[str, Any]) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        API_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req) as resp:  # type: ignore[arg-type]
            body = resp.read().decode("utf-8")
        print(f"[OK] {payload['event_type']} -> status {resp.status}, response={body}")
    except Exception as exc:  # noqa: BLE001
        print(f"[ERR] Failed to send event {payload}: {exc}")


def generate_scenario(num_students: int = 20) -> List[Dict[str, Any]]:
    """Генерирует список событий для нескольких курсов и студентов.

    Сценарий:
    - Есть два курса: Python 101 и Data Science Basics.
    - Каждый студент записывается на один курс.
    - По каждому уроку часть студентов отваливается.
    """

    now = datetime.utcnow()

    courses = [
        {
            "external_id": "python-101",
            "title": "Python 101",
            "lessons": [
                {"external_id": "py-1", "title": "Введение"},
                {"external_id": "py-2", "title": "Переменные"},
                {"external_id": "py-3", "title": "Циклы"},
                {"external_id": "py-4", "title": "Функции"},
            ],
        },
        {
            "external_id": "ds-101",
            "title": "Data Science Basics",
            "lessons": [
                {"external_id": "ds-1", "title": "Что такое DS"},
                {"external_id": "ds-2", "title": "Pandas"},
                {"external_id": "ds-3", "title": "Визуализация"},
            ],
        },
    ]

    events: List[Dict[str, Any]] = []

    for i in range(num_students):
        student_id = f"student-{i+1}"
        student_email = _random_email(f"student{i+1}")

        course = random.choice(courses)
        course_external_id = course["external_id"]
        course_title = course["title"]

        enrolled_time = now - timedelta(days=random.randint(0, 14))

        # Событие записи на курс
        events.append(
            {
                "course_external_id": course_external_id,
                "course_title": course_title,
                "lesson_external_id": None,
                "lesson_title": None,
                "student_external_id": student_id,
                "student_email": student_email,
                "event_type": "enrolled",
                "occurred_at": enrolled_time.isoformat() + "Z",
                "payload": {},
            }
        )

        # Прохождение уроков с постепенным дроп-оффом
        last_time = enrolled_time
        completed_all = True

        for idx, lesson in enumerate(course["lessons"]):
            # Смещение времени вперёд
            last_time += timedelta(hours=random.randint(4, 48))

            # Начал урок
            events.append(
                {
                    "course_external_id": course_external_id,
                    "course_title": course_title,
                    "lesson_external_id": lesson["external_id"],
                    "lesson_title": lesson["title"],
                    "student_external_id": student_id,
                    "student_email": student_email,
                    "event_type": "lesson_started",
                    "occurred_at": last_time.isoformat() + "Z",
                    "payload": {"lesson_index": idx + 1},
                }
            )

            # Вероятность, что студент не завершит урок
            drop_chance = 0.2 + 0.15 * idx  # больше отвалов на поздних уроках
            if random.random() < drop_chance:
                completed_all = False
                break

            # Завершил урок
            last_time += timedelta(hours=random.randint(1, 8))
            events.append(
                {
                    "course_external_id": course_external_id,
                    "course_title": course_title,
                    "lesson_external_id": lesson["external_id"],
                    "lesson_title": lesson["title"],
                    "student_external_id": student_id,
                    "student_email": student_email,
                    "event_type": "lesson_completed",
                    "occurred_at": last_time.isoformat() + "Z",
                    "payload": {"lesson_index": idx + 1},
                }
            )

        if completed_all:
            last_time += timedelta(hours=random.randint(2, 24))
            events.append(
                {
                    "course_external_id": course_external_id,
                    "course_title": course_title,
                    "lesson_external_id": None,
                    "lesson_title": None,
                    "student_external_id": student_id,
                    "student_email": student_email,
                    "event_type": "course_completed",
                    "occurred_at": last_time.isoformat() + "Z",
                    "payload": {},
                }
            )

    return events


def main() -> None:
    num_students_str = os.getenv("ACAD_FAKE_STUDENTS", "30")
    try:
        num_students = int(num_students_str)
    except ValueError:
        num_students = 30

    print(f"Sending fake events to {API_URL} for {num_students} students...")

    events = generate_scenario(num_students=num_students)
    for ev in events:
        send_event(ev)
        time.sleep(0.05)

    print("Done.")


if __name__ == "__main__":
    main()
