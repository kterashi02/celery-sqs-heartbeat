"""タスクを 1 件投入するスクリプト。

  python enqueue.py [--seconds 90]
"""

from __future__ import annotations

import argparse

from celery_app import QUEUE_NAME
from tasks import long_task


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=int, default=90)
    args = p.parse_args()
    r = long_task.apply_async(args=[args.seconds], queue=QUEUE_NAME)
    print(f"enqueued task_id={r.id} seconds={args.seconds} queue={QUEUE_NAME}")


if __name__ == "__main__":
    main()
