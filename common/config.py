import os
from dataclasses import dataclass


@dataclass
class DatabaseSettings:
    """Настройки подключения к базе данных."""

    url: str = os.getenv(
        "DATABASE_URL",
        # Для локальной разработки по умолчанию используем SQLite-файл.
        # В Kubernetes/проде это значение будет переопределено переменной окружения.
        "sqlite:///./academoscope.db",
    )


def get_db_settings() -> DatabaseSettings:
    return DatabaseSettings()
