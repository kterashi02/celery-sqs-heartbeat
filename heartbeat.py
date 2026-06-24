"""SQS の Visibility Timeout を処理中に動的延長するハートビートユーティリティ。

Celery + SQS(kombu) で処理時間が長いタスクを安全に処理するためのもの。
タスク内を `with sqs_visibility_heartbeat(self):` で囲むことで使える。

仕組み:
  1. kombu の SQS transport は、受信メッセージの ReceiptHandle と Queue URL を
     `task.request.delivery_info` に載せる（キー: sqs_message / sqs_queue）。
  2. それを使い、daemon thread から定期的に SQS の ChangeMessageVisibility を打って
     可視性タイムアウトを extend_by 秒に設定し直す（＝処理中は再配信されない）。
  3. クラッシュ/強制 kill 時はこのスレッドもプロセス道連れで止まるため延長が途絶え、
     最後の設定から最大 extend_by 秒後に SQS が自動で再配信する。

なぜ自前実装が必要か:
  kombu は in-flight メッセージの可視性を自動延長しない。よって処理中の延長は
  アプリ側でハートビートを実装する必要がある。
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from urllib.parse import urlsplit

import boto3

logger = logging.getLogger(__name__)

_client_lock = threading.Lock()
_client_cache: dict[tuple, object] = {}


def _get_sqs_client(endpoint_url: str | None, region: str):
    """boto3 SQS クライアントをプロセス単位でキャッシュして返す。

    client() 生成は認証/エンドポイント解決が走り軽くないため使い回す。クライアントは
    スレッドセーフなので HB スレッドと共有してよい。PID をキーにするのは、Celery prefork が
    親から fork した子が親のクライアント（urllib3 接続/SSL 状態）を共有して壊すのを避けるため
    （各ワーカープロセスが初回に1個だけ作る）。
    """
    key = (os.getpid(), endpoint_url, region)
    client = _client_cache.get(key)
    if client is None:
        with _client_lock:
            client = _client_cache.get(key)  # double-checked: 同時生成を防ぐ
            if client is None:
                client = boto3.client("sqs", endpoint_url=endpoint_url, region_name=region)
                _client_cache[key] = client
    return client


def _endpoint_and_region_from_queue_url(queue_url: str) -> tuple[str | None, str | None]:
    """SQS の Queue URL から boto3 用の (endpoint_url, region) を導出する。

    メッセージ由来の URL を使うので、ローカル(ElasticMQ)でも実 AWS でも必ず一致する。
    環境変数での local/aws 判定が要らなくなる。
      - 実 SQS : https://sqs.<region>.amazonaws.com/... → endpoint と region を取り出す
      - ローカル: http://localhost:9324/...          → endpoint のみ（region は無関係）
    """
    parts = urlsplit(queue_url)
    if not parts.scheme or not parts.netloc:
        return None, None
    endpoint = f"{parts.scheme}://{parts.netloc}"
    host = parts.hostname or ""
    region = None
    # sqs.<region>.amazonaws.com から region を取り出す（SigV4 署名の region 一致のため）
    if host.startswith("sqs.") and host.endswith(".amazonaws.com"):
        region = host.split(".")[1]
    return endpoint, region


def extract_sqs_receipt(request) -> tuple[str | None, str | None]:
    """Celery タスクの request から (ReceiptHandle, QueueURL) を取り出す。

    SQS 以外の broker（Redis 等）や、handle が取れない場合は (None, None)。
    """
    sources = [
        getattr(request, "delivery_info", None),
        (getattr(request, "properties", None) or {}).get("delivery_info"),
    ]
    for src in sources:
        if isinstance(src, dict):
            msg = src.get("sqs_message")
            if msg and msg.get("ReceiptHandle"):
                return msg["ReceiptHandle"], src.get("sqs_queue")
    return None, None


@contextlib.contextmanager
def sqs_visibility_heartbeat(
    task,
    *,
    interval: int | None = None,
    extend_by: int | None = None,
    endpoint_url: str | None = None,
    region: str | None = None,
):
    """task 実行中、対象 SQS メッセージの可視性タイムアウトを定期延長する context manager。

    Args:
        task:      `bind=True` のタスクの self。
        interval:  延長間隔(秒)。base の 1/2〜2/3 を目安に（期限切れマージン確保）。
        extend_by: 1 回の延長で設定する VisibilityTimeout(秒)。

    SQS 以外の broker や handle 不在時は no-op として素通りする。
    """
    interval = interval or int(os.environ.get("HEARTBEAT_INTERVAL", "15"))
    extend_by = extend_by or int(os.environ.get("HEARTBEAT_EXTEND_BY", "30"))

    # task.request はスレッドローカル。ハートビートスレッドからは読めないので、
    # 必要な値はここ（メイン＝タスク実行スレッド）で確定させてクロージャに渡す。
    task_id = task.request.id
    receipt_handle, queue_url = extract_sqs_receipt(task.request)
    if not (receipt_handle and queue_url):
        # SQS 以外の broker や handle 取得不可時は安全に素通り（no-op）
        logger.info("heartbeat: no-op (no SQS receipt handle)")
        yield
        return

    # エンドポイント/リージョンはメッセージの Queue URL から導出（引数指定があれば優先）。
    derived_endpoint, derived_region = _endpoint_and_region_from_queue_url(queue_url)
    sqs = _get_sqs_client(
        endpoint_url or derived_endpoint,
        region or derived_region or os.environ.get("AWS_REGION", "us-east-1"),
    )
    stop = threading.Event()

    def _beat() -> None:
        n = 0
        # stop されるか interval 経過のたびにループ（stop されたら即終了）
        while not stop.wait(interval):
            try:
                sqs.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=extend_by,
                )
                n += 1
                logger.info(
                    "heartbeat #%d: +%ss (task=%s)", n, extend_by, task_id
                )
            except Exception as e:  # noqa: BLE001  期限切れ等は延長を諦めて停止
                logger.warning("heartbeat stopped: %r", e)
                break

    t = threading.Thread(target=_beat, name="sqs-visibility-heartbeat", daemon=True)
    t.start()
    try:
        yield
    finally:
        stop.set()       # 必ずハートビートを止める
        t.join(timeout=2)
