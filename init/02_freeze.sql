-- 写入冻结：发布窗口内保护若干制品编号，网页与直接 SQL 的 UPDATE/DELETE
-- 都必须让整笔源事务失败；到期后自动失效。
--
-- 设计要点
-- 1. 冻结事实存放在源库（而不是应用内存/SQLite），因此应用或数据库重启后
--    未到期的冻结仍然生效；触发器在应用完全停机时依旧拦截直接 SQL。
-- 2. 串行化点使用 artifact_write_guards 的“守卫行”：
--    - 每个修改 artifacts 行的触发器事务，先对受影响编号做一次守卫行写入
--      （INSERT ... ON CONFLICT DO UPDATE），从而持有该行的行锁直到提交；
--    - 申请冻结同样按编号升序写这些守卫行，再校验当前四列与提交点一致。
--    两者在 MVCC 下严格排序：
--      · 改版事务先提交 → 冻结校验读到新值，整组申请拒绝；
--      · 冻结先提交 → READ COMMITTED 的改版事务在守卫行上等待，随后在新
--        快照中看到冻结而失败；REPEATABLE READ/SERIALIZABLE 的改版事务
--        直接得到 “could not serialize access due to concurrent update”。
--    因此不会出现“已冻结却已换版”。
-- 3. 冻结对 UPDATE/DELETE/INSERT（主键搬移）与 TRUNCATE 都生效；
--    未受保护的编号不受影响（只多一次守卫行写入）。
-- 4. 到期判定用 clock_timestamp()，无需应用参与，到期即自动放行。

CREATE TABLE IF NOT EXISTS artifact_freeze_groups (
    group_id      UUID        PRIMARY KEY,
    point_lsn     TEXT        NOT NULL,
    point_lsn_num BIGINT      NOT NULL,
    note          TEXT        NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_freezes (
    group_id     UUID        NOT NULL
                 REFERENCES artifact_freeze_groups(group_id) ON DELETE CASCADE,
    artifact_no  INTEGER     NOT NULL,
    service_name TEXT        NOT NULL,
    version      TEXT        NOT NULL,
    sha256       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL,
    released_at  TIMESTAMPTZ,
    PRIMARY KEY (group_id, artifact_no)
);

-- 同一编号最多只能有一条“未释放”的冻结；到期的旧冻结由申请事务先释放。
CREATE UNIQUE INDEX IF NOT EXISTS uq_artifact_freezes_live
    ON artifact_freezes(artifact_no) WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_artifact_freezes_group
    ON artifact_freezes(group_id);

-- MVCC 串行化守卫：每行对应一个制品编号，被修改/被冻结时都会更新。
CREATE TABLE IF NOT EXISTS artifact_write_guards (
    artifact_no INTEGER     PRIMARY KEY,
    touched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    touch_count BIGINT      NOT NULL DEFAULT 0
);

-- 为“触发器建立之前就存在”的行补齐守卫行。守卫行必须与制品行同时或更早
-- 提交，否则一个快照早于冻结的 REPEATABLE READ 事务看不到守卫行，
-- 会以“插入新行”的方式绕过冲突检测（历史教训，见 README 已知边界）。
INSERT INTO artifact_write_guards(artifact_no, touched_at, touch_count)
SELECT artifact_no, now(), 0 FROM artifacts
ON CONFLICT (artifact_no) DO NOTHING;

-- 哨兵行（编号 0，制品编号恒为正整数，不会冲突）：冻结申请会更新它，
-- 让 TRUNCATE 的语句级守卫也能与冻结在 MVCC 下串行化。
INSERT INTO artifact_write_guards(artifact_no, touched_at, touch_count)
VALUES (0, now(), 0)
ON CONFLICT (artifact_no) DO NOTHING;

-- 对单个编号取守卫行锁（在调用方事务内持有到提交）。
CREATE OR REPLACE FUNCTION artifact_guard_no(p_no integer) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO artifact_write_guards AS g(artifact_no, touched_at, touch_count)
    VALUES (p_no, now(), 0)
    ON CONFLICT (artifact_no) DO UPDATE
        SET touched_at = now(), touch_count = g.touch_count + 1;
END;
$$;

-- 编号上是否存在生效中的冻结；有则抛出 P0001，令整笔事务失败。
CREATE OR REPLACE FUNCTION artifact_assert_not_frozen(p_no integer) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_freeze artifact_freezes%ROWTYPE;
BEGIN
    SELECT * INTO v_freeze FROM artifact_freezes
     WHERE artifact_no = p_no
       AND released_at IS NULL
       AND expires_at > clock_timestamp()
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0001',
            MESSAGE = format(
                'ARTIFACT_FROZEN artifact_no=%s expires_at=%s',
                p_no, to_char(v_freeze.expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.USOF'));
    END IF;
END;
$$;

-- ---------------------------------------------------------------- 行级守卫
CREATE OR REPLACE FUNCTION artifact_freeze_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_nos integer[] := '{}';
    v_no integer;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        v_nos := array_append(v_nos, OLD.artifact_no);
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE')
       AND (TG_OP = 'INSERT' OR NEW.artifact_no IS DISTINCT FROM OLD.artifact_no)
    THEN
        v_nos := array_append(v_nos, NEW.artifact_no);
    END IF;

    -- 统一升序取守卫行锁：所有写入路径锁序一致，不会因多行顺序相反而死锁。
    -- 拿到锁之后，后续语句的快照必然包含此前已提交的冻结。
    FOREACH v_no IN ARRAY (SELECT array_agg(DISTINCT n ORDER BY n) FROM unnest(v_nos) AS n)
    LOOP
        PERFORM artifact_guard_no(v_no);
        PERFORM artifact_assert_not_frozen(v_no);
    END LOOP;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

-- ------------------------------------------------------------ 整表 TRUNCATE
-- 先取哨兵守卫行（编号 0）再检查冻结：冻结申请也更新同一行，两者在 MVCC
-- 下严格排序，避免“TRUNCATE 提交后冻结才生效”把受保护制品清空。
CREATE OR REPLACE FUNCTION artifact_freeze_truncate_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM artifact_guard_no(0);
    IF EXISTS (
        SELECT 1 FROM artifact_freezes
         WHERE released_at IS NULL AND expires_at > clock_timestamp()
    ) THEN
        RAISE EXCEPTION USING ERRCODE = 'P0001',
            MESSAGE = 'ARTIFACT_FROZEN truncate=artifacts reason=存在生效中的冻结';
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_artifacts_freeze_guard ON artifacts;
CREATE TRIGGER trg_artifacts_freeze_guard
    BEFORE INSERT OR UPDATE OR DELETE ON artifacts
    FOR EACH ROW EXECUTE FUNCTION artifact_freeze_guard();

DROP TRIGGER IF EXISTS trg_artifacts_freeze_truncate ON artifacts;
CREATE TRIGGER trg_artifacts_freeze_truncate
    BEFORE TRUNCATE ON artifacts
    FOR EACH STATEMENT EXECUTE FUNCTION artifact_freeze_truncate_guard();

-- 记录已完成的结构版本：应用每次启动先比对，一致就跳过 DDL（避免重启时
-- 反复对 artifacts 取排他锁、阻断部署窗口内的正常写入）。
INSERT INTO schema_meta (key, value)
VALUES ('init_version', '4')
ON CONFLICT (key) DO UPDATE SET value = excluded.value;

