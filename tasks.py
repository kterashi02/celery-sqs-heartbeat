"""実行時間が長いタスクの例。"""

from __future__ import annotations

import time

from celery.utils.log import get_task_logger

from celery_app import celery_app
from heartbeat import sqs_visibility_heartbeat

logger = get_task_logger(__name__)


@celery_app.task(bind=True, name="long_task")
def long_task(self, seconds: int = 90) -> dict:
    """base を超える長さの処理。with で囲むことで処理中は可視性が延長される。"""
    logger.info("long_task start: %ss (task=%s)", seconds, self.request.id)
    with sqs_visibility_heartbeat(self):
        for _ in range(seconds):
            time.sleep(1)
    logger.info("long_task done (task=%s)", self.request.id)
    return {"slept": seconds}
