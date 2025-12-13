from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from .config import get_db_settings


db_settings = get_db_settings()

connect_args = {}
if db_settings.url.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

engine = create_engine(
    db_settings.url,
    pool_pre_ping=True,
    future=True,
    connect_args=connect_args,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True,
)

Base = declarative_base()


def get_db() -> Generator:
    """Генератор для использования в качестве зависимости FastAPI.

    Пример:

    ```python
    from fastapi import Depends
    from sqlalchemy.orm import Session

    @router.get("/items")
    def read_items(db: Session = Depends(get_db)):
        ...
    ```
    """

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
