#!/usr/bin/env python3
"""冻结功能并发交错测试。

用 psycopg2 直连源库 + FastAPI HTTP 调用，确定性验证：

A. 申请冻结 与 直接 SQL 换版 交错：任何一种提交顺序下，都不会出现
   “已冻结却已换版”——要么换版先提交导致冻结申请被拒（行仍可继续被保护），
   要么冻结先提交导致换版事务被触发器拒绝；
B. 两个并发冻结申请互相串行化，不会产生两条在保冻结；
C. 冻结中的同编号写事务阻塞到申请提交后被触发器拒绝，且源行保持原值；
D. 冻结期间其他编号的写事务不受影响。

用法：
  PGHOST=/tmp/sock PGPORT=55432 BASE=http://127.0.0.1:8012 \
  PYTHONPATH=/tmp/pylibs python3 -I tests/test_freeze_concurrency.py
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.request

import psycopg2

DSN = (
    f"host={os.environ['PGHOST']} port={os.environ['PGPORT']}"
    " dbname=postgres user=postgres"
)
BASE = os.environ.get("BASE", "http://127.0.0.1:8012")
PASS = 0
FAIL = 0


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"PASS: {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"FAIL: {msg}")


def check(cond: bool, msg: str) -> None:
    ok(msg) if cond else bad(msg)


def http(method: str, path: str, payload: dict | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def latest_point() -> str:
    code, body = http("GET", "/api/points")
    assert code == 200, body
    return body["points"][-1]["lsn"]


def current_row(no: int):
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT service_name, version, sha256 FROM artifacts"
                " WHERE artifact_no=%s", (no,))
            return cur.fetchone()
    finally:
        conn.close()


def wait_capture(version: str, no: int, timeout: float = 10.0) -> None:
    """等待某版本的变更被逻辑复制捕获，确保拿到的提交点包含它。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, body = http("GET", f"/api/history/{no}")
        if code == 200 and any(h.get("version") == version
                               for h in body["history"]):
            return
        time.sleep(0.3)
    raise RuntimeError(f"version {version} on {no} not captured in time")


class Barrier:
    """简易双线程栅栏：两边都到齐后再一起放行，制造确定性交错。"""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._n = 0

    def wait(self, total: int = 2) -> None:
        with self._cond:
            self._n += 1
            if self._n == total:
                self._cond.notify_all()
            else:
                self._cond.wait_for(lambda: self._n == total, timeout=15)


def reset_freeze(no: int) -> None:
    """清理某编号全部未释放冻结，保证每个场景起点干净。"""
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE artifact_freeze SET released_at=now()"
                " WHERE artifact_no=%s AND released_at IS NULL", (no,))
        conn.commit()
    finally:
        conn.close()


def scenario_c_blocked_write(no: int) -> None:
    """C: 冻结申请先拿到行锁并提交；换版事务在锁上等待，恢复后被触发器拒绝。

    这是与场景 A 相反的提交顺序。用两段式构造确定性交错：
      1. 手动开一个事务对目标行 SELECT FOR UPDATE（模拟申请事务的加锁阶段）；
      2. 换版事务此时尝试 UPDATE，必然阻塞在该行锁上；
      3. 在同一事务里直接插入冻结记录后提交（等价于申请事务提交）；
      4. 换版事务被放行，执行 UPDATE 时触发器 RAISE，整笔回滚。
    """
    print("\n=== 场景 C：冻结先提交，持锁等待的换版事务被触发器拒绝 ===")
    reset_freeze(no)
    point = latest_point()
    original = current_row(no)
    write_started = Barrier()
    outcome: dict = {}

    def writer(go: threading.Event):
        conn = psycopg2.connect(DSN)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                go.wait(timeout=15)  # 等冻结事务确认已持有行锁
                cur.execute(
                    "UPDATE artifacts SET version='concurrent-write'"
                    " WHERE artifact_no=%s", (no,))  # 阻塞，直到冻结事务提交
                conn.commit()
            outcome["write"] = "committed"
        except psycopg2.Error as exc:
            conn.rollback()
            outcome["write"] = f"rejected:{exc.pgcode}"
        finally:
            conn.close()

    go = threading.Event()
    t = threading.Thread(target=writer, args=(go,))
    freezer = psycopg2.connect(DSN)
    freezer.autocommit = False
    try:
        with freezer.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                "SELECT artifact_no, service_name, version, sha256"
                " FROM artifacts WHERE artifact_no=%s FOR UPDATE", (no,))
            snap = cur.fetchone()
            t.start()
            go.set()  # 通知 writer 发起 UPDATE（将阻塞在本行锁上）
            time.sleep(0.8)  # 确保 writer 已阻塞
            cur.execute(
                "INSERT INTO artifact_freeze(artifact_no, point_lsn,"
                " service_name, version, sha256, reason, expires_at)"
                " VALUES (%s,%s,%s,%s,%s,'scenario-C', now()+interval '5 min')",
                (no, point, snap[1], snap[2], snap[3]))
        freezer.commit()  # 放锁；writer 恢复后立刻撞上触发器
        t.join(15)
    finally:
        freezer.close()

    print("  outcome:", outcome)
    check(outcome.get("write", "").startswith("rejected:P0001"),
          "C: 冻结先提交后，换版事务被触发器拒绝（P0001），整笔回滚")
    check(current_row(no) == original,
          "C: 被拒后源行保持冻结快照原值，不存在已冻结却已换版")


def scenario_a_interleave(no: int, slow: bool) -> None:
    """A: 冻结申请与直接 SQL 换版并发，换版事务先拿到行锁。

    无论冻结申请是立刻发起还是稍后发起，它都必须在换版事务的行锁上排队；
    换版提交后申请事务才能继续，此时快照校验必须发现版本已变并整组拒绝，
    从而杜绝“已冻结却已换版”。``slow`` 只改变申请的到达节奏做压力覆盖。
    相反的提交顺序（冻结先提交、换版被拒）由场景 C 覆盖。
    """
    label = "延迟申请" if slow else "立即申请"
    print(f"\n=== 场景 A：换版持锁与冻结申请交错（{label}）===")
    reset_freeze(no)
    point = latest_point()
    start = Barrier()
    outcome: dict = {}
    new_version = f"interleave-{int(time.time()*1000)%100000}"

    def writer():
        conn = psycopg2.connect(DSN)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                start.wait()
                cur.execute(
                    "UPDATE artifacts SET version=%s WHERE artifact_no=%s",
                    (new_version, no))
                time.sleep(1.0)  # 持有行锁，让冻结申请在锁上排队
                conn.commit()
            outcome["write"] = "committed"
        except psycopg2.Error as exc:
            conn.rollback()
            outcome["write"] = f"rejected:{exc.pgcode}"
        finally:
            conn.close()

    def freezer():
        start.wait()
        if slow:
            time.sleep(0.4)
        code, body = http("POST", "/api/freezes", {
            "point_lsn": point, "artifact_nos": [no],
            "duration_minutes": 5, "reason": f"A-{label}"})
        outcome["freeze_code"] = code
        outcome["freeze_body"] = body

    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=freezer)
    t1.start(); t2.start(); t1.join(25); t2.join(25)
    print("  outcome:", outcome)
    row = current_row(no)

    check(outcome.get("write") == "committed", "A: 换版事务正常提交")
    check(outcome.get("freeze_code") == 409,
          "A: 换版先提交后，冻结申请检测到快照不一致，整组拒绝（409）")
    check(row[1] == new_version, "A: 源行为新版本，且不存在失实冻结")

    # 关键安全不变量：换版成功的编号上不允许存在任何在保冻结
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM artifact_freeze"
                " WHERE artifact_no=%s AND released_at IS NULL"
                " AND expires_at > now()", (no,))
            active = cur.fetchone()[0]
    finally:
        conn.close()
    check(active == 0, "A: 换版成功的编号上没有任何在保冻结")


def scenario_b_dueling_freezes(no: int) -> None:
    """B: 两个并发冻结申请，最终至多一条在保冻结，另一个收到冲突拒绝。"""
    print("\n=== 场景 B：两个并发冻结申请串行化 ===")
    reset_freeze(no)
    point = latest_point()
    start = Barrier()
    results: list = []
    lock = threading.Lock()

    def one(idx: int, delay: float):
        start.wait()
        time.sleep(delay)
        code, body = http("POST", "/api/freezes", {
            "point_lsn": point, "artifact_nos": [no],
            "duration_minutes": 5, "reason": f"B-{idx}"})
        with lock:
            results.append((idx, code, body))

    threads = [threading.Thread(target=one, args=(i, 0.0)) for i in (1, 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(20)
    print("  results:", [(i, c) for i, c, _ in results])

    codes = sorted(c for _, c, _ in results)
    check(codes == [201, 409], "B: 并发申请一个成功(201)一个冲突拒绝(409)")
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM artifact_freeze"
                " WHERE artifact_no=%s AND released_at IS NULL"
                " AND expires_at > now()", (no,))
            active = cur.fetchone()[0]
    finally:
        conn.close()
    check(active == 1, "B: 该编号在保冻结恰好一条，无重复保护")


def scenario_d_other_artifacts(no_frozen: int, no_free: int) -> None:
    """D: 冻结期间其他编号仍可正常写入。"""
    print("\n=== 场景 D：冻结不影响其他编号 ===")
    reset_freeze(no_frozen)
    point = latest_point()
    code, _ = http("POST", "/api/freezes", {
        "point_lsn": point, "artifact_nos": [no_frozen],
        "duration_minutes": 5, "reason": "D"})
    check(code == 201, "D: 参考冻结建立成功")
    before = current_row(no_free)
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                "UPDATE artifacts SET version='other-ok' WHERE artifact_no=%s",
                (no_free,))
            conn.commit()
        ok("D: 其他编号更新事务正常提交")
    except psycopg2.Error as exc:
        conn.rollback()
        bad(f"D: 其他编号不应被拦截: {exc}")
    finally:
        conn.close()
    row = current_row(no_free)
    check(row[1] == "other-ok" and row[0] == before[0] and row[2] == before[2],
          "D: 其他编号写入生效，除 version 外其余列不变")


def main() -> int:
    # 用独立编号，避免与其他手工测试数据互相干扰
    no_main = 4101
    no_other = 4102
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            # 先清理历史运行可能残留的冻结，避免夹具 upsert 撞上触发器
            cur.execute(
                "UPDATE artifact_freeze SET released_at=now()"
                " WHERE artifact_no IN (%s,%s) AND released_at IS NULL",
                (no_main, no_other))
            sha = "b" * 64
            cur.execute(
                "INSERT INTO artifacts(artifact_no, service_name, version,"
                " sha256) VALUES (%s,'conc-svc','v0',%s)"
                " ON CONFLICT (artifact_no) DO UPDATE SET version='v0'",
                (no_main, sha))
            cur.execute(
                "INSERT INTO artifacts(artifact_no, service_name, version,"
                " sha256) VALUES (%s,'other-svc','v0',%s)"
                " ON CONFLICT (artifact_no) DO UPDATE SET version='v0'",
                (no_other, sha))
        conn.commit()
    finally:
        conn.close()
    wait_capture("v0", no_main)
    wait_capture("v0", no_other)

    scenario_c_blocked_write(no_main)
    scenario_a_interleave(no_main, slow=False)
    scenario_a_interleave(no_main, slow=True)
    scenario_b_dueling_freezes(no_main)
    scenario_d_other_artifacts(no_main, no_other)

    print(f"\n================ 并发自测: {PASS} passed / {FAIL} failed ================")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
