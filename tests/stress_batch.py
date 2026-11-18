"""并发批量写入压测：确认批量按相同锁序取守卫行锁，不产生死锁。

用法：python3 tests/stress_batch.py --base http://127.0.0.1:8033 --rounds 30
每轮两个线程同时提交相反的编号顺序，例如 [A,B] 与 [B,A]；
若锁序不统一，这两个事务会互相等待并被源库判定死锁。
"""
from __future__ import annotations

import argparse
import json
import threading
import urllib.error
import urllib.request

SHA = "e" * 64


def post(base: str, body: dict) -> tuple[int, dict]:
    req = urllib.request.Request(
        base + "/api/source/operations",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def ops(order: list[int], tag: str) -> dict:
    return {
        "operations": [
            {
                "op": "update",
                "artifact_no": no,
                "service_name": f"batch-{tag}-{no}",
                "version": tag,
                "sha256": SHA,
            }
            for no in order
        ]
    }


def ensure_rows(base: str, numbers: list[int]) -> None:
    """确保压测编号存在（已存在时 insert 返回 400，忽略即可）。"""
    post(
        base,
        {
            "operations": [
                {
                    "op": "insert",
                    "artifact_no": no,
                    "service_name": f"batch-{no}",
                    "version": "0",
                    "sha256": SHA,
                }
                for no in numbers
            ]
        },
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--rounds", type=int, default=20)
    args = ap.parse_args()

    deadlocks = 0
    ok = 0
    other = 0
    ensure_rows(args.base, [6001, 6002])
    for i in range(args.rounds):
        a, b = 6001, 6002
        results: dict[str, tuple[int, dict]] = {}

        def run(name: str, order: list[int]):
            results[name] = post(args.base, ops(order, f"{name}{i}"))

        threads = [
            threading.Thread(target=run, args=("fwd", [a, b])),
            threading.Thread(target=run, args=("rev", [b, a])),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for status, body in results.values():
            if status == 200:
                ok += 1
            elif status == 409 and body.get("detail", {}).get("code") == "SOURCE_BUSY":
                deadlocks += 1
            else:
                other += 1
                print(f"round {i}: 非预期结果 {status} {body}")
    print(f"rounds={args.rounds} ok={ok} deadlock={deadlocks} other={other}")
    return 0 if deadlocks == 0 and other == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
