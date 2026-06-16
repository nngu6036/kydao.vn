from celery import Celery
from celery.schedules import crontab

from app.config import get_settings


settings = get_settings()

celery_app = Celery(
    "chess_elo",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.tasks"],
)

celery_app.conf.update(
    timezone=settings.celery_timezone,
    enable_utc=False,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    beat_schedule={
        "daily-midnight-task": {
            "task": "app.tasks.daily_midnight_task",
            "schedule": crontab(hour=0, minute=0),
        },
    },
)
