# celery-sqs-heartbeat

Celery + SQS(kombu) で、処理中タスクの Visibility Timeout を動的に延長（ハートビート）する実装サンプル。

SQS の可視性タイムアウトは、メッセージの処理〜削除に通常かかる最大時間に合わせて設定する。処理時間を正確に見積もれない場合は、キューに短い初期可視性タイムアウト（以降 base と呼ぶ）を設定して開始し、処理中にハートビートで定期的に延長する。これにより、再表示が早すぎてメッセージが重複処理されるのを防ぎつつ、未処理メッセージの再処理の遅れを最小限に抑えられる（参考: [Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)）。

kombu は in-flight メッセージの可視性を自動延長しないため、延長はアプリ側で実装する必要がある。

## 使い方

タスクを context manager で囲む。

```python
from heartbeat import sqs_visibility_heartbeat

@celery_app.task(bind=True, name="long_task")
def long_task(self, seconds: int = 90):
    with sqs_visibility_heartbeat(self):
        do_heavy_work()
```

## 仕組み

1. kombu の SQS transport は、受信メッセージの ReceiptHandle と Queue URL を `task.request.delivery_info`（`sqs_message` / `sqs_queue`）に格納する。
2. これを用いて daemon thread が一定間隔（`HEARTBEAT_INTERVAL`）で `ChangeMessageVisibility` を呼び、可視性タイムアウトを `HEARTBEAT_EXTEND_BY` 秒に設定し直す。
3. プロセスが終了するとスレッドも停止し、最後の設定から最大 `HEARTBEAT_EXTEND_BY` 秒後に再配信される。

## 前提設定

`task_acks_late = True`（`celery_app.py`）。これが無いと kombu は受信直後にメッセージを削除（ack = DeleteMessage）するため、延長対象が消え、クラッシュ時も再配信されない。

base となるキューの `VisibilityTimeout` はキュー側で設定する（`elasticmq.conf` / 実 SQS のキュー作成時）。

## ローカル実行（ElasticMQ）

キュー `jobs` の base は 10 秒（`elasticmq.conf`）。

```bash
uv sync
docker compose up -d
```

worker と投入の各ターミナルで環境変数を設定する。

```bash
export SQS_ENDPOINT_URL=http://localhost:9324 \
  AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x AWS_REGION=us-east-1 \
  HEARTBEAT_INTERVAL=4 HEARTBEAT_EXTEND_BY=10
```

```bash
# worker
uv run celery -A celery_app worker -Q jobs --concurrency=4 --loglevel=INFO

# 投入（別ターミナル）
uv run python enqueue.py --seconds 25
```

base(10s) を超える 25 秒タスクが、worker ログに `heartbeat #N` を出しながら再配信されず 1 回で完走する。

## 環境変数

| 変数 | 既定 | 説明 |
|---|---|---|
| `SQS_TARGET` | `local` | `local`(ElasticMQ) / `aws`(実 AWS SQS) |
| `SQS_ENDPOINT_URL` | `http://localhost:9324` | local 時のエンドポイント |
| `QUEUE_NAME` | `jobs` | キュー名 |
| `BASE_VISIBILITY` | `10` | kombu がキューを作成する場合の visibility|
| `HEARTBEAT_INTERVAL` | `15` | 延長間隔(秒)。 |
| `HEARTBEAT_EXTEND_BY` | `30` | 1 回の延長で設定する VisibilityTimeout(秒) |

## 実 AWS SQS


```bash
export AWS_PROFILE=your-dev-profile
export AWS_REGION=ap-northeast-1
export SQS_TARGET=aws
export QUEUE_NAME=jobs                     # 作成済みのキュー名に合わせる
export HEARTBEAT_INTERVAL=15

uv run celery -A celery_app worker -Q "$QUEUE_NAME" --concurrency=4 --loglevel=INFO
uv run python enqueue.py --seconds 90
```