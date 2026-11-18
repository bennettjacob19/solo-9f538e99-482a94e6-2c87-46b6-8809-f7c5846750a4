"""并发改版 × 冻结申请压测：断言不变量“不存在已冻结却已换版”。

用法：
    python3 tests/stress_freeze.py --base http://127.0.0.1:8033 \
        --dsn "host=... port=... dbname=postgres user=postgres" --rounds 40

每轮：
  1. 通过直连 SQL 把某编号改成已知版本 v_n，等它被捕获成提交点；
  2. 同时在两个线程里发起：网页 API 申请冻结该编号 / 直接 SQL 改版该编号；
  3. 核对最终状态：若冻结生效，则源库值必须仍等于提交点值；
  4. 检查冻结组与源库当前值，任何 “active 且值不同” 都计为违规。
"""
from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.parse
import urllib.request

import psycopg2

SHA = "c" * 64


def http(base: str, method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--rounds", type=int, default=30)
    ap.add_argument("--no", type=int, default=5001)
    args = ap.parse_args()

    violations = 0
    freeze_ok = 0
    conflict = 0
    update_ok = 0
    review: list[dict] = []

    for i in range(args.rounds):
        version = f"5.{i}.0"
        with psycopg2.connect(args.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO artifacts(artifact_no, service_name, version, sha256)"
                " VALUES (%s, 'stress-svc', %s, %s)"
                " ON CONFLICT (artifact_no) DO UPDATE"
                " SET version = EXCLUDED.version, sha256 = EXCLUDED.sha256",
                (args.no, version, SHA),
            )
        # 等该事务成为已捕获提交点
        point = None
        for _ in range(60):
            status, body = http(args.base, "GET", "/api/points")
            if status == 200:
                latest = body["points"][-1]
                status2, body2 = http(
                    args.base,
                    "GET",
                    "/api/directory?point=" + urllib.parse.quote(latest["lsn"]),
                )
                if status2 == 200:
                    rows = [r for r in body2["artifacts"] if r["artifact_no"] == args.no]
                    if rows and rows[0]["version"] == version:
                        point = latest["lsn"]
                        break
            time.sleep(0.2)
        if point is None:
            print(f"round {i}: 等待提交点超时，跳过")
            continue

        results: dict[str, tuple[int, dict]] = {}

        def do_freeze():
            results["freeze"] = http(
                args.base,
                "POST",
                "/api/freezes",
                {
                    "point": point,
                    "artifact_nos": [args.no],
                    "duration_seconds": 600,
                },
            )

        def do_update():
            try:
                with psycopg2.connect(args.dsn) as conn, conn.cursor() as cur:
                    cur.execute(
                        "UPDATE artifacts SET version=%s, sha256=%s"
                        " WHERE artifact_no=%s",
                        (f"{version}-upd", SHA, args.no),
                    )
                results["update"] = (200, {"ok": True})
            except psycopg2.Error as exc:
                results["update"] = (409, {"error": str(exc)})

        threads = [threading.Thread(target=do_freeze),
                   threading.Thread(target=do_update)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        f_status = results["freeze"][0]
        u_status = results["update"][0]
        if f_status == 201:
            freeze_ok += 1
        elif f_status in (409, 400):
            conflict += 1
        if u_status == 200:
            update_ok += 1

        # 不变量检查：任何 active 冻结的编号，源库当前值必须等于冻结记录值
        with psycopg2.connect(args.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.artifact_no, f.version, a.version
                FROM artifact_freezes f
                JOIN artifacts a USING (artifact_no)
                WHERE f.released_at IS NULL
                  AND f.expires_at > clock_timestamp()
                  AND f.version IS DISTINCT FROM a.version
                """
            )
            bad = cur.fetchall()
        if bad:
            violations += 1
            print(f"round {i}: 违规 已冻结却已换版: {bad}")

        # 释放本轮冻结，避免影响下一轮
        if f_status == 201:
            gid = results["freeze"][1]["group_id"]
            http(args.base, "POST", f"/api/freezes/{gid}/release")
        review.append(
            {"round": i, "freeze": f_status, "update": u_status,
             "version_after": None}
        )

    print(
        f"rounds={args.rounds} freeze_ok={freeze_ok} conflict={conflict} "
        f"update_ok={update_ok} violations={violations}"
    )
    return 0 if violations == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
