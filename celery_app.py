"""Celery アプリ。SQS_TARGET でローカル ElasticMQ と実 AWS SQS を切り替える。

環境変数:
  SQS_TARGET       : "local"(ElasticMQ) / "aws"(実 AWS SQS)。既定 local
  SQS_ENDPOINT_URL : local 時のエンドポイント（既定 http://localhost:9324）
  QUEUE_NAME       : キュー名（既定 jobs）
  BASE_VISIBILITY  : kombu がキューを作成する場合の VisibilityTimeout 秒（既存キューには影響しない）
  AWS_REGION       : aws 時のリージョン
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

from celery import Celery

QUEUE_NAME = os.environ.get("QUEUE_NAME", "jobs")
SQS_TARGET = os.environ.get("SQS_TARGET", "local").lower()
BASE_VISIBILITY = int(os.environ.get("BASE_VISIBILITY", "10"))
REGION = os.environ.get("AWS_REGION", "ap-northeast-1")

if SQS_TARGET == "aws":
    # broker_url に host を書かず、kombu に region からエンドポイントを解決させる。
    BROKER_URL = "sqs://"
    _is_secure = True
else:
    # kombu は boto3 の endpoint を broker_url の host:port から導出するので、そこに埋める。
    _endpoint = os.environ.get("SQS_ENDPOINT_URL", "http://localhost:9324")
    _parsed = urlparse(_endpoint)
    _key = os.environ.get("AWS_ACCESS_KEY_ID", "x")
    _secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "x")
    BROKER_URL = f"sqs://{_key}:{_secret}@{_parsed.hostname}:{_parsed.port}"
    _is_secure = _parsed.scheme == "https"

celery_app = Celery("heartbeat_sample", broker=BROKER_URL, include=["tasks"])
celery_app.conf.update(
    broker_transport_options={
        "is_secure": _is_secure,                    # ElasticMQ は http / 実 SQS は https
        "region": REGION,
        "visibility_timeout": BASE_VISIBILITY,      # base（既存キューがあれば上書きされない）
        "wait_time_seconds": 5,                     # long polling
    },
    task_serializer="json",
    accept_content=["json"],
    result_backend=None,                            # SQS は結果保存に使えない
    task_default_queue=QUEUE_NAME,
    task_routes={"long_task": {"queue": QUEUE_NAME}},
    task_acks_late=True,                            # 延長対象を保持する前提のため完了後に ack。
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
)
