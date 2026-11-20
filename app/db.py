"""SQLite 连接、模式与事务助手。

设计：单进程单连接 + 进程级写锁。所有写路径在 BEGIN IMMEDIATE ...
COMMIT 的单个事务中执行，因此原文、校时依据、分析供数状态同次生效；
查询与（崩溃后）重启都不会看到半次重算的中间态。

压缩段的潮位原值不存于 observations，而在 tide_blocks 的压缩块内，
observations 仅保留 block_id 块引用；未压缩段仍直接存 tide_level，
两种形态可在同一库内长期混存（含重启后）。
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

DEFAULT_DB_PATH = "./buoy.db"

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS calibration_points (
    boot_segment TEXT NOT NULL,      -- 开机段
    counter      INTEGER NOT NULL,   -- 段内毫秒计数
    utc_ms       INTEGER NOT NULL,   -- 对应 UTC 毫秒
    PRIMARY KEY (boot_segment, counter)
);

CREATE TABLE IF NOT EXISTS segment_settings (
    boot_segment        TEXT PRIMARY KEY, -- 开机段
    compression_enabled INTEGER NOT NULL  -- 1 = 潮位按无损压缩块存储
);

CREATE TABLE IF NOT EXISTS tide_blocks (
    block_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    boot_segment TEXT NOT NULL,           -- 块所属开机段
    entry_count  INTEGER NOT NULL,        -- 块内观测数，至多 256
    payload      BLOB NOT NULL            -- zlib 压缩的 [[obs_id, tide_level], ...]
);

CREATE TABLE IF NOT EXISTS observations (
    obs_id         TEXT PRIMARY KEY, -- 观测 ID
    boot_segment   TEXT NOT NULL,    -- 原文：开机段
    counter        INTEGER NOT NULL, -- 原文：段内计数
    tide_level     REAL,             -- 原文：潮位；压缩段为 NULL，原值只在块内
    block_id       INTEGER,          -- 压缩块引用；NULL 表示潮位直接存于 tide_level
    utc_ms         INTEGER,          -- 换算结果；NULL 表示待校时
    status         TEXT NOT NULL,    -- ok / pending_calibration / conflict
    reason         TEXT,             -- 状态原因
    cal_lo_counter INTEGER,          -- 校时依据：下界校时点
    cal_lo_utc_ms  INTEGER,
    cal_hi_counter INTEGER,          -- 校时依据：上界校时点
    cal_hi_utc_ms  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_observations_utc_ms ON observations(utc_ms);
CREATE INDEX IF NOT EXISTS idx_observations_segment ON observations(boot_segment);
CREATE INDEX IF NOT EXISTS idx_observations_block ON observations(block_id);
CREATE INDEX IF NOT EXISTS idx_tide_blocks_segment ON tide_blocks(boot_segment);
"""

# 旧版 observations（无 block_id、tide_level NOT NULL）迁移：SQLite 无法直接
# 改列约束，按官方做法重建表并逐列拷贝，块引用初始全部为 NULL。
_MIGRATE_OBSERVATIONS = """
CREATE TABLE observations_new (
    obs_id         TEXT PRIMARY KEY,
    boot_segment   TEXT NOT NULL,
    counter        INTEGER NOT NULL,
    tide_level     REAL,
    block_id       INTEGER,
    utc_ms         INTEGER,
    status         TEXT NOT NULL,
    reason         TEXT,
    cal_lo_counter INTEGER,
    cal_lo_utc_ms  INTEGER,
    cal_hi_counter INTEGER,
    cal_hi_utc_ms  INTEGER
);
INSERT INTO observations_new (
    obs_id, boot_segment, counter, tide_level, block_id, utc_ms, status,
    reason, cal_lo_counter, cal_lo_utc_ms, cal_hi_counter, cal_hi_utc_ms
)
SELECT obs_id, boot_segment, counter, tide_level, NULL, utc_ms, status,
       reason, cal_lo_counter, cal_lo_utc_ms, cal_hi_counter, cal_hi_utc_ms
FROM observations;
DROP TABLE observations;
ALTER TABLE observations_new RENAME TO observations;
CREATE INDEX idx_observations_utc_ms ON observations(utc_ms);
CREATE INDEX idx_observations_segment ON observations(boot_segment);
CREATE INDEX idx_observations_block ON observations(block_id);
"""


def get_conn() -> sqlite3.Connection:
    """返回进程内共享连接（惰性创建并建表/迁移）。"""
    global _conn
    if _conn is None:
        path = os.environ.get("BUOY_DB", DEFAULT_DB_PATH)
        if path != ":memory:":
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        # 旧卷（observations 已存在但缺 block_id）先迁移到新模式，
        # 再执行含新表/新索引的完整 SCHEMA；全新库迁移为空操作。
        _migrate_legacy(conn)
        conn.executescript(SCHEMA)
        _conn = conn
    return _conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone() is not None


def _migrate_legacy(conn: sqlite3.Connection) -> None:
    """把旧版 SQLite 卷升级到含 block_id 的新模式，数据逐列保留。"""
    if not _table_exists(conn, "observations"):
        return  # 全新库，由 SCHEMA 统一创建
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(observations)")}
    if "block_id" not in cols:
        conn.executescript(_MIGRATE_OBSERVATIONS)


def lock() -> threading.RLock:
    """读写共用的进程级锁：写事务进行中，查询不会读到中间态。"""
    return _lock


@contextmanager
def transaction(conn: sqlite3.Connection):
    """单事务包裹一批写入与重算；异常即整体回滚。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


def reset_for_tests() -> None:
    """测试用：丢弃当前连接（下次 get_conn 重新建库/迁移）。"""
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None
