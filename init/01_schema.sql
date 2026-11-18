-- 源库固定结构：制品目录（只允许对这四张固定列做增/改/删，不允许 DDL）
CREATE TABLE IF NOT EXISTS artifacts (
    artifact_no  INTEGER PRIMARY KEY,
    service_name TEXT    NOT NULL,
    version      TEXT    NOT NULL,
    sha256       TEXT    NOT NULL
);

-- 仅在空表时写入示例数据，重启不会重复播种
INSERT INTO artifacts (artifact_no, service_name, version, sha256)
SELECT v.artifact_no, v.service_name, v.version, v.sha256
FROM (VALUES
    (1001, 'billing-gateway', '1.4.2', 'a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90'),
    (1002, 'risk-engine',      '2.7.0', '0f1e2d3c4b5a69788796a5b4c3d2e1f009182736455463728190aabbccddeeff'),
    (1003, 'notify-hub',       '0.9.3', 'ffeeddccbbaa9988776655443322110099887766554433221100998877665544')
) AS v(artifact_no, service_name, version, sha256)
WHERE NOT EXISTS (SELECT 1 FROM artifacts);

-- ===========================================================================
-- 写入冻结（发布窗口防换版）
--
-- 冻结记录持久化在源 PostgreSQL 中：
--   * 应用重启后未到期的冻结继续生效；
--   * 到期判定完全由数据库 now() 完成，到期后触发器自动放行，无需清扫任务；
--   * 强制力来自触发器，网页操作与直接 SQL 一视同仁，无法绕过。
-- ===========================================================================
CREATE TABLE IF NOT EXISTS artifact_freeze (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    artifact_no    INTEGER NOT NULL,
    point_lsn      TEXT    NOT NULL,          -- 申请所依据的已捕获提交点
    service_name   TEXT    NOT NULL,          -- 提交点时刻该编号的四列快照
    version        TEXT    NOT NULL,
    sha256         TEXT    NOT NULL,
    reason         TEXT    NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL,
    released_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_freeze_artifact
    ON artifact_freeze(artifact_no);
-- 同一编号只允许存在一条“未释放”冻结（到期记录经释放/再次申请后仍可留痕）。
-- 注意 now() 是 STABLE 不能进索引谓词；“在保”的到期判定由触发器/查询负责，
-- 并发重复申请由申请事务内 SELECT ... FOR UPDATE 行锁串行化拦截。
CREATE UNIQUE INDEX IF NOT EXISTS uq_freeze_unreleased_one
    ON artifact_freeze(artifact_no)
    WHERE released_at IS NULL;

-- ---------------------------------------------------------------------------
-- 行级触发器：冻结期间禁止对受保护编号 UPDATE / DELETE。
-- 对网页 API 与直接 psql 写入同样生效；语句内任一行被拦截，
-- 整个源事务回滚（RAISE EXCEPTION），同事务其他编号的改动一并失败。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION artifacts_freeze_guard() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
    f RECORD;
BEGIN
    SELECT id, expires_at INTO f
    FROM artifact_freeze
    WHERE artifact_no = OLD.artifact_no
      AND released_at IS NULL
      AND expires_at > now()
    ORDER BY expires_at DESC
    LIMIT 1;

    IF f.id IS NOT NULL THEN
        RAISE EXCEPTION
            'artifact_no=% 已写入冻结至 %，期间禁止 %（SQLSTATE P0001）',
            OLD.artifact_no, f.expires_at, TG_OP
            USING ERRCODE = 'P0001';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_artifacts_freeze_guard ON artifacts;
CREATE TRIGGER trg_artifacts_freeze_guard
    BEFORE UPDATE OR DELETE ON artifacts
    FOR EACH ROW EXECUTE FUNCTION artifacts_freeze_guard();

-- ---------------------------------------------------------------------------
-- 语句级 TRUNCATE 保护：TRUNCATE 不触发行级触发器，需单独拦截。
-- 仅当存在任一在保冻结时禁止 TRUNCATE，无冻结时行为不变。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION artifacts_freeze_truncate_guard() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
    n INTEGER;
BEGIN
    SELECT count(*) INTO n
    FROM artifact_freeze
    WHERE released_at IS NULL AND expires_at > now();
    IF n > 0 THEN
        RAISE EXCEPTION
            '存在 % 个在保写入冻结，禁止 TRUNCATE artifacts（SQLSTATE P0001）', n
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_artifacts_freeze_truncate ON artifacts;
CREATE TRIGGER trg_artifacts_freeze_truncate
    BEFORE TRUNCATE ON artifacts
    FOR EACH STATEMENT EXECUTE FUNCTION artifacts_freeze_truncate_guard();
