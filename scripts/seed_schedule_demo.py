from datetime import datetime, timedelta, time as time_cls

from common.db import Base, SessionLocal, engine
from common.models import Course, Lesson, Teacher, ScheduleSlot


TEACHERS_DEF = [
    ("ivanov_petr", "Пётр Иванов", "p.ivanov@example.com"),
    ("smirnova_olga", "Ольга Смирнова", "o.smirnova@example.com"),
    ("sidorov_alex", "Алексей Сидоров", "a.sidorov@example.com"),
    ("petrova_maria", "Мария Петрова", "m.petrova@example.com"),
    ("sokolov_ivan", "Иван Соколов", "i.sokolov@example.com"),
    ("morozova_anna", "Анна Морозова", "a.morozova@example.com"),
    ("volkov_sergey", "Сергей Волков", "s.volkov@example.com"),
    ("fedorova_ekaterina", "Екатерина Фёдорова", "e.fedorova@example.com"),
    ("nikitin_pavel", "Павел Никитин", "p.nikitin@example.com"),
    ("sergeeva_yulia", "Юлия Сергеева", "y.sergeeva@example.com"),
    ("alexeev_denis", "Денис Алексеев", "d.alexeev@example.com"),
    ("egorova_olga", "Ольга Егорова", "o.egorova@example.com"),
    ("belov_kirill", "Кирилл Белов", "k.belov@example.com"),
    ("antonova_alena", "Алёна Антонова", "a.antonova@example.com"),
    ("tarasov_roman", "Роман Тарасов", "r.tarasov@example.com"),
    ("vasilieva_daria", "Дарья Васильева", "d.vasilieva@example.com"),
    ("gusev_andrey", "Андрей Гусев", "a.gusev@example.com"),
    ("belova_irina", "Ирина Белова", "i.belova@example.com"),
    ("zaitsev_ilya", "Илья Зайцев", "i.zaitsev@example.com"),
    ("popova_ekaterina", "Екатерина Попова", "k.popova@example.com"),
    ("medvedev_oleg", "Олег Медведев", "o.medvedev@example.com"),
    ("sidorova_natalia", "Наталья Сидорова", "n.sidorova@example.com"),
    ("kravtsov_maksim", "Максим Кравцов", "m.kravtsov@example.com"),
    ("filippova_elena", "Елена Филиппова", "e.filippova@example.com"),
    ("lebedev_vladimir", "Владимир Лебедев", "v.lebedev@example.com"),
    ("novikova_alexandra", "Александра Новикова", "a.novikova@example.com"),
    ("danilov_roman", "Роман Данилов", "r.danilov@example.com"),
    ("orlova_anna", "Анна Орлова", "a.orlova@example.com"),
    ("komarov_artem", "Артём Комаров", "a.komarov@example.com"),
    ("afanasyeva_viktoria", "Виктория Афанасьева", "v.afanasyeva@example.com"),
    ("vasiliev_dmitry", "Дмитрий Васильев", "d.vasiliev@example.com"),
    ("nikolaeva_vera", "Вера Николаева", "v.nikolaeva@example.com"),
    ("makarov_stepan", "Степан Макаров", "s.makarov@example.com"),
    ("koroleva_anna", "Анна Королёва", "a.koroleva@example.com"),
    ("bogdanov_egor", "Егор Богданов", "e.bogdanov@example.com"),
    ("sorokina_lyudmila", "Людмила Сорокина", "l.sorokina@example.com"),
    ("litvinov_sergey", "Сергей Литвинов", "s.litvinov@example.com"),
    ("rodionova_olga", "Ольга Родионова", "o.rodionova@example.com"),
    ("martynov_anton", "Антон Мартынов", "a.martynov@example.com"),
    ("frolova_tatyana", "Татьяна Фролова", "t.frolova@example.com"),
]


def get_or_create_teacher(session, external_id: str, name: str, email: str | None) -> Teacher:
    teacher = (
        session.query(Teacher)
        .filter(Teacher.external_id == external_id)
        .one_or_none()
    )
    if teacher is None:
        teacher = Teacher(external_id=external_id, name=name, email=email)
        session.add(teacher)
        session.flush()
    else:
        if name and teacher.name != name:
            teacher.name = name
        if email and teacher.email != email:
            teacher.email = email
    return teacher


def seed_schedule() -> None:
    Base.metadata.create_all(bind=engine)
    session = SessionLocal()

    try:
        # Преподаватели
        teachers: list[Teacher] = []
        for ext_id, name, email in TEACHERS_DEF:
            teachers.append(get_or_create_teacher(session, ext_id, name, email))

        courses: list[Course] = session.query(Course).order_by(Course.id).all()
        if not courses:
            print("Нет курсов, сначала запустите scripts/seed_demo_data.py")
            return

        today = datetime.utcnow().date()
        # Берём ближайшие 14 дней как горизонт планирования
        base_start_date = today

        slots_created = 0

        for idx, course in enumerate(courses):
            # Ищем первый урок курса, чтобы привязать слот к конкретному уроку
            lesson = (
                session.query(Lesson)
                .filter(Lesson.course_id == course.id)
                .order_by(Lesson.position.nulls_last(), Lesson.id)
                .first()
            )

            teacher = teachers[idx % len(teachers)]

            # Для каждого курса создадим 3 занятия в разные дни и время
            for offset_days, hour in ((idx % 5, 19), (idx % 5 + 7, 11), (idx % 5 + 10, 15)):
                start_date = base_start_date + timedelta(days=offset_days)
                start_dt = datetime.combine(start_date, time_cls(hour=hour, minute=0))
                end_dt = start_dt + timedelta(hours=1, minutes=30)

                slot = ScheduleSlot(
                    course_id=course.id,
                    lesson_id=lesson.id if lesson else None,
                    teacher_id=teacher.id,
                    start_at=start_dt,
                    end_at=end_dt,
                    location="Онлайн (Zoom)",
                    group_name="Группа A",
                )
                session.add(slot)
                slots_created += 1

        session.commit()
        print(f"Создано слотов расписания: {slots_created}")
    finally:
        session.close()


if __name__ == "__main__":
    seed_schedule()
