import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from app.celery_app import celery_app
from app.config import get_settings


logger = logging.getLogger("chess_elo.tasks")


@celery_app.task(name="app.tasks.daily_midnight_task")
def daily_midnight_task() -> dict[str, str]:
    settings = get_settings()
    now = datetime.now(ZoneInfo(settings.celery_timezone))
    logger.info("Running daily midnight task at %s", now.isoformat())
    return {"status": "ok", "ran_at": now.isoformat()}
