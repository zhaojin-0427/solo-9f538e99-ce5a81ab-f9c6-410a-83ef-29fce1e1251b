"""无损分块压缩存储的端到端测试。

覆盖：启用压缩与块边界、原值只存块内、同 ID 更正/跨段覆盖、
按需解压（单条/UTC 短窗/单条更正）、补登校时不解压、失败回滚、
重启后混查、旧 SQLite 卷迁移。
"""

import os
import sqlite3
import tempfile

import pytest
from fastapi.testclient import TestClient

from app import blocks, db
from app.main import app

client = TestClient(app)
raising_client = TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def clean_tables():
    conn = db.get_conn()
    with db.lock():
        conn.execute("DELETE FROM observations")
        conn.execute("DELETE FROM calibration_points")
        conn.execute("DELETE FROM tide_blocks")
        conn.execute("DELETE FROM segment_settings")
    yield


# ---------------------------------------------------------------- 工具

def post_obs(items):
    r = client.post("/observations", json={"observations": items})
    assert r.status_code == 200, r.text
    return r.json()


def post_cal(items, expect=200):
    r = client.post("/calibrations", json={"calibrations": items})
    assert r.status_code == expect, r.text
    return r.json()


def enable(segment):
    r = client.post(f"/segments/{segment}/compression")
    assert r.status_code == 200, r.text
    return r.json()


def get_obs(obs_id):
    r = client.get(f"/observations/{obs_id}")
    assert r.status_code == 200, r.text
    return r.json()


def tide(start, end):
    r = client.get("/tide-levels", params={"start_ms": start, "end_ms": end})
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------- 启用与块边界

def test_enable_compression_moves_raw_values_into_blocks_only():
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 10, "utc_ms": 1010},
    ])
    post_obs([
        {"obs_id": "o1", "boot_segment": "A", "counter": 0, "tide_level": 1.25},
        {"obs_id": "o2", "boot_segment": "A", "counter": 5, "tide_level": 3.141592653589793},
        {"obs_id": "o3", "boot_segment": "A", "counter": 10, "tide_level": -7.5},
    ])

    resp = enable("A")
    assert resp == {
        "boot_segment": "A",
        "compression_enabled": True,
        "compressed_count": 3,
        "block_count": 1,
        "max_entries_per_block": 256,
    }

    conn = db.get_conn()
    rows = conn.execute(
        "SELECT obs_id, tide_level, block_id FROM observations ORDER BY obs_id"
    ).fetchall()
    # 压缩后 observations 不再保留潮位原值，只剩块引用
    assert {r["obs_id"]: (r["tide_level"], r["block_id"] is not None) for r in rows} == {
        "o1": (None, True),
        "o2": (None, True),
        "o3": (None, True),
    }
    block = conn.execute("SELECT entry_count, payload FROM tide_blocks").fetchone()
    assert block["entry_count"] == 3
    entries = dict(blocks.decode_block(block["payload"]))
    # 原值无损
    assert entries == {"o1": 1.25, "o2": 3.141592653589793, "o3": -7.5}

    # 详情与供数都仍返回原值
    assert get_obs("o2")["tide_level"] == 3.141592653589793
    assert {p["obs_id"]: p["tide_level"] for p in tide(0, 99999)["items"]} == entries


def test_block_capacity_is_256_and_later_observations_join_blocks():
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 600, "utc_ms": 1600},
    ])
    post_obs([
        {"obs_id": f"o{i}", "boot_segment": "A", "counter": i,
         "tide_level": float(i)}
        for i in range(600)
    ])
    enable("A")

    conn = db.get_conn()
    counts = [r["entry_count"] for r in conn.execute(
        "SELECT entry_count FROM tide_blocks ORDER BY block_id")]
    assert counts == [256, 256, 88]
    assert all(c <= 256 for c in counts)

    # 该段后续潮位也按块保存：追加进未满的末块
    post_obs([
        {"obs_id": "n1", "boot_segment": "A", "counter": 600, "tide_level": 9.0},
        {"obs_id": "n2", "boot_segment": "A", "counter": 601, "tide_level": 9.1},
        {"obs_id": "n3", "boot_segment": "A", "counter": 602, "tide_level": 9.2},
    ])
    counts = [r["entry_count"] for r in conn.execute(
        "SELECT entry_count FROM tide_blocks ORDER BY block_id")]
    assert counts == [256, 256, 91]
    assert conn.execute(
        "SELECT tide_level FROM observations WHERE obs_id = 'n1'"
    ).fetchone()["tide_level"] is None
    assert get_obs("n1")["tide_level"] == 9.0


def test_enable_compression_is_idempotent():
    post_obs([{"obs_id": "o1", "boot_segment": "A", "counter": 1, "tide_level": 1.0}])
    first = enable("A")
    assert first["compressed_count"] == 1 and first["block_count"] == 1
    second = enable("A")
    assert second["compressed_count"] == 0 and second["block_count"] == 1
    # 原值仍只在块内且无损
    assert get_obs("o1")["tide_level"] == 1.0


# ---------------------------------------------------------------- 更正与覆盖

def test_same_id_correction_updates_block_and_all_read_views():
    post_cal([{"boot_segment": "A", "counter": 0, "utc_ms": 1000}])
    post_obs([{"obs_id": "o1", "boot_segment": "A", "counter": 0, "tide_level": 1.0}])
    enable("A")
    assert get_obs("o1")["tide_level"] == 1.0

    # 同 ID 改值（同段）：块内原值更正，详情与供数都返回新值
    resp = post_obs([{"obs_id": "o1", "boot_segment": "A", "counter": 0, "tide_level": 4.25}])
    assert {c["obs_id"] for c in resp["changed"]} == set()  # 时刻/状态未变
    assert get_obs("o1")["tide_level"] == 4.25
    assert tide(0, 99999)["items"][0]["tide_level"] == 4.25

    conn = db.get_conn()
    row = conn.execute("SELECT tide_level, block_id FROM observations WHERE obs_id='o1'").fetchone()
    assert row["tide_level"] is None  # 原值依旧只在块内
    entries = dict(blocks.decode_block(
        conn.execute("SELECT payload FROM tide_blocks WHERE block_id=?",
                     (row["block_id"],)).fetchone()["payload"]))
    assert entries == {"o1": 4.25}


def test_single_correction_only_decompresses_involved_block(monkeypatch):
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 600, "utc_ms": 1600},
    ])
    post_obs([
        {"obs_id": f"o{i}", "boot_segment": "A", "counter": i, "tide_level": float(i)}
        for i in range(600)
    ])
    enable("A")

    calls = {"n": 0}
    real = blocks.decode_block

    def spy(payload):
        calls["n"] += 1
        return real(payload)

    monkeypatch.setattr(blocks, "decode_block", spy)
    # 更正单条只解压它所在的块（replace 一次解压 + 一次重写编码，不涉其他块）
    post_obs([{"obs_id": "o300", "boot_segment": "A", "counter": 300, "tide_level": 99.0}])
    assert calls["n"] == 1
    assert get_obs("o300")["tide_level"] == 99.0


def test_cross_segment_overwrite_moves_value_between_storage_forms():
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "B", "counter": 0, "utc_ms": 5000},
        {"boot_segment": "C", "counter": 0, "utc_ms": 8000},
    ])
    post_obs([{"obs_id": "o1", "boot_segment": "A", "counter": 0, "tide_level": 1.0}])
    enable("A")
    enable("C")
    old_block = db.get_conn().execute(
        "SELECT block_id FROM observations WHERE obs_id='o1'").fetchone()["block_id"]

    # 压缩段 A -> 普通段 B：旧块移除原值，新值入普通列
    post_obs([{"obs_id": "o1", "boot_segment": "B", "counter": 0, "tide_level": 2.0}])
    conn = db.get_conn()
    row = conn.execute("SELECT * FROM observations WHERE obs_id='o1'").fetchone()
    assert row["boot_segment"] == "B" and row["block_id"] is None
    assert row["tide_level"] == 2.0
    old_entries = dict(blocks.decode_block(
        conn.execute("SELECT payload FROM tide_blocks WHERE block_id=?",
                     (old_block,)).fetchone()["payload"]))
    assert "o1" not in old_entries
    assert get_obs("o1")["tide_level"] == 2.0

    # 普通段 B -> 压缩段 C：旧列清空，原值进入 C 的块
    post_obs([{"obs_id": "o1", "boot_segment": "C", "counter": 0, "tide_level": 3.0}])
    row = conn.execute("SELECT * FROM observations WHERE obs_id='o1'").fetchone()
    assert row["boot_segment"] == "C" and row["tide_level"] is None
    assert row["block_id"] is not None
    assert get_obs("o1")["tide_level"] == 3.0
    assert tide(0, 99999)["items"][0]["tide_level"] == 3.0


# ---------------------------------------------------------------- 按需解压

def test_get_one_observation_only_decompresses_its_block(monkeypatch):
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 600, "utc_ms": 1600},
    ])
    post_obs([
        {"obs_id": f"o{i}", "boot_segment": "A", "counter": i, "tide_level": float(i)}
        for i in range(600)
    ])
    enable("A")

    calls = {"n": 0}
    real = blocks.decode_block
    monkeypatch.setattr(blocks, "decode_block",
                        lambda payload: calls.__setitem__("n", calls["n"] + 1) or real(payload))

    body = get_obs("o300")
    assert body["tide_level"] == 300.0
    assert calls["n"] == 1  # 三个块里只解压了 o300 所在的那一个


def test_tide_window_only_decompresses_blocks_touched_by_window(monkeypatch):
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 600, "utc_ms": 1600},
    ])
    post_obs([
        {"obs_id": f"o{i}", "boot_segment": "A", "counter": i, "tide_level": float(i)
         } for i in range(600)
    ])
    enable("A")  # 块：o0..o255 / o256..o511 / o512..o599

    calls = {"n": 0}
    real = blocks.decode_block
    monkeypatch.setattr(blocks, "decode_block",
                        lambda payload: calls.__setitem__("n", calls["n"] + 1) or real(payload))

    # UTC 1300..1400 只落在中间块（utc = 1000 + counter）
    body = tide(1300, 1400)
    assert calls["n"] == 1
    assert {p["obs_id"] for p in body["items"]} == {f"o{i}" for i in range(300, 401)}

    # 跨中间块与末块的短窗：解压两块，不碰第一块
    calls["n"] = 0
    body = tide(1500, 1530)
    assert calls["n"] == 2
    assert body["count"] == 31


def test_calibration_backfill_never_decompresses_tide_blocks(monkeypatch):
    post_obs([{"obs_id": "o1", "boot_segment": "A", "counter": 100, "tide_level": 6.75}])
    enable("A")
    assert get_obs("o1")["status"] == "pending_calibration"

    calls = {"n": 0}
    real = blocks.decode_block
    monkeypatch.setattr(blocks, "decode_block",
                        lambda payload: calls.__setitem__("n", calls["n"] + 1) or real(payload))

    # 补登校时点：只更新 UTC、状态与冲突，不解压潮位
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 200, "utc_ms": 1200},
    ])
    assert calls["n"] == 0
    body = get_obs("o1")
    assert body["status"] == "ok" and body["utc_ms"] == 1100
    assert body["tide_level"] == 6.75  # 原值仍在块内
    assert tide(0, 99999)["items"][0]["tide_level"] == 6.75


# ---------------------------------------------------------------- 失败回滚

def test_failed_compression_keeps_pre_commit_state(monkeypatch):
    post_cal([{"boot_segment": "A", "counter": 0, "utc_ms": 1000}])
    post_obs([
        {"obs_id": f"o{i}", "boot_segment": "A", "counter": 0, "tide_level": float(i)}
        for i in range(3)
    ])
    assert tide(0, 99999)["count"] == 3

    def boom(*_a, **_kw):
        raise RuntimeError("block write failure")

    monkeypatch.setattr(blocks, "create_block", boom)
    r = raising_client.post("/segments/A/compression")
    assert r.status_code == 500

    conn = db.get_conn()
    # 开关、块引用、原值全部保持提交前状态
    assert conn.execute("SELECT COUNT(*) AS n FROM segment_settings").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM tide_blocks").fetchone()["n"] == 0
    rows = conn.execute("SELECT tide_level, block_id FROM observations").fetchall()
    assert {r["tide_level"] is not None and r["block_id"] is None for r in rows} == {True}
    assert tide(0, 99999)["count"] == 3  # 供数不变


def test_failed_observation_batch_rolls_back_blocks_and_rows(monkeypatch):
    post_cal([{"boot_segment": "A", "counter": 0, "utc_ms": 1000}])
    post_obs([{"obs_id": "keep", "boot_segment": "A", "counter": 0, "tide_level": 1.0}])
    enable("A")

    def boom(*_a, **_kw):
        raise RuntimeError("block append failure")

    monkeypatch.setattr(blocks, "append_to_segment", boom)
    r = raising_client.post("/observations", json={"observations": [
        {"obs_id": "new1", "boot_segment": "A", "counter": 1, "tide_level": 2.0},
    ]})
    assert r.status_code == 500

    conn = db.get_conn()
    assert conn.execute(
        "SELECT COUNT(*) AS n FROM observations WHERE obs_id='new1'").fetchone()["n"] == 0
    # 旧观测的原值、块引用与供数保持提交前状态
    assert get_obs("keep")["tide_level"] == 1.0
    assert tide(0, 99999)["count"] == 1


# ---------------------------------------------------------------- 重启与迁移

def _switch_db(path: str) -> None:
    os.environ["BUOY_DB"] = path
    db.reset_for_tests()


def test_reload_after_restart_keeps_mixed_plain_and_compressed_storage():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    old_env = os.environ.get("BUOY_DB")
    _switch_db(path)
    try:
        post_cal([
            {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
            {"boot_segment": "B", "counter": 0, "utc_ms": 5000},
        ])
        post_obs([
            {"obs_id": "a1", "boot_segment": "A", "counter": 0, "tide_level": 1.5},
            {"obs_id": "b1", "boot_segment": "B", "counter": 0, "tide_level": 2.5},
        ])
        enable("A")  # a1 压缩，b1 保持普通列
        assert get_obs("a1")["tide_level"] == 1.5
        assert get_obs("b1")["tide_level"] == 2.5

        # 模拟重启：关闭连接后重新打开同一个 SQLite 卷
        db.reset_for_tests()
        db.get_conn()
        body = tide(0, 99999)
        assert {p["obs_id"]: p["tide_level"] for p in body["items"]} == {
            "a1": 1.5, "b1": 2.5,
        }
        assert get_obs("a1")["tide_level"] == 1.5
        # 重启后压缩段的新观测仍按块保存
        post_obs([{"obs_id": "a2", "boot_segment": "A", "counter": 1, "tide_level": 3.5}])
        assert db.get_conn().execute(
            "SELECT block_id FROM observations WHERE obs_id='a2'").fetchone()["block_id"] is not None
        assert get_obs("a2")["tide_level"] == 3.5
    finally:
        db.reset_for_tests()
        os.environ["BUOY_DB"] = old_env or ":memory:"
        for suffix in ("", "-wal", "-shm"):
            if os.path.exists(path + suffix):
                os.unlink(path + suffix)


def test_legacy_sqlite_volume_is_migrated_and_keeps_fields_and_receipts():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    old_env = os.environ.get("BUOY_DB")
    # 用旧版模式（tide_level NOT NULL、无 block_id）直接造一个旧卷
    legacy = sqlite3.connect(path)
    legacy.executescript("""
    CREATE TABLE calibration_points (
        boot_segment TEXT NOT NULL, counter INTEGER NOT NULL, utc_ms INTEGER NOT NULL,
        PRIMARY KEY (boot_segment, counter));
    CREATE TABLE observations (
        obs_id TEXT PRIMARY KEY, boot_segment TEXT NOT NULL, counter INTEGER NOT NULL,
        tide_level REAL NOT NULL, utc_ms INTEGER, status TEXT NOT NULL, reason TEXT,
        cal_lo_counter INTEGER, cal_lo_utc_ms INTEGER,
        cal_hi_counter INTEGER, cal_hi_utc_ms INTEGER);
    CREATE INDEX idx_observations_utc_ms ON observations(utc_ms);
    CREATE INDEX idx_observations_segment ON observations(boot_segment);
    """)
    legacy.execute(
        "INSERT INTO calibration_points VALUES ('A', 0, 1000)")
    legacy.execute(
        "INSERT INTO observations VALUES "
        "('old1','A',0,4.5,1000,'ok',NULL,0,1000,0,1000)")
    legacy.commit()
    legacy.close()

    _switch_db(path)
    try:
        # 迁移后旧字段与回执规则不变
        body = get_obs("old1")
        assert body["tide_level"] == 4.5 and body["utc_ms"] == 1000
        assert body["status"] == "ok"
        assert body["calibration"] == {
            "lo": {"counter": 0, "utc_ms": 1000},
            "hi": {"counter": 0, "utc_ms": 1000},
        }
        assert tide(0, 99999)["items"][0]["tide_level"] == 4.5
        # 迁移后的卷可正常启用压缩，且同 ID 覆盖回执规则不变
        resp = post_obs([{"obs_id": "old1", "boot_segment": "A", "counter": 0,
                          "tide_level": 4.5}])
        assert resp["changed_count"] == 0
        enable("A")
        assert get_obs("old1")["tide_level"] == 4.5
    finally:
        db.reset_for_tests()
        os.environ["BUOY_DB"] = old_env or ":memory:"
        for suffix in ("", "-wal", "-shm"):
            if os.path.exists(path + suffix):
                os.unlink(path + suffix)


def test_plain_and_compressed_observations_are_served_together():
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "A", "counter": 1, "utc_ms": 1001},
        {"boot_segment": "B", "counter": 0, "utc_ms": 5000},
    ])
    post_obs([
        {"obs_id": "a1", "boot_segment": "A", "counter": 0, "tide_level": 1.1},
        {"obs_id": "a2", "boot_segment": "A", "counter": 1, "tide_level": 1.2},
        {"obs_id": "b1", "boot_segment": "B", "counter": 0, "tide_level": 2.1},
    ])
    enable("A")
    body = tide(0, 99999)
    assert [(p["obs_id"], p["tide_level"]) for p in body["items"]] == [
        ("a1", 1.1), ("a2", 1.2), ("b1", 2.1),
    ]


def test_enable_empty_segment_then_later_observations_go_to_blocks():
    # 段内尚无观测时启用：当次转入 0，开关已记录，后续观测直接按块存
    resp = enable("Z")
    assert resp["compressed_count"] == 0 and resp["block_count"] == 0
    post_cal([{"boot_segment": "Z", "counter": 0, "utc_ms": 1000}])
    post_obs([{"obs_id": "z1", "boot_segment": "Z", "counter": 0, "tide_level": 5.5}])
    row = db.get_conn().execute(
        "SELECT tide_level, block_id FROM observations WHERE obs_id='z1'").fetchone()
    assert row["tide_level"] is None and row["block_id"] is not None
    assert get_obs("z1")["tide_level"] == 5.5


def test_cross_segment_overwrite_between_two_compressed_segments():
    post_cal([
        {"boot_segment": "A", "counter": 0, "utc_ms": 1000},
        {"boot_segment": "C", "counter": 0, "utc_ms": 8000},
    ])
    post_obs([
        {"obs_id": "a1", "boot_segment": "A", "counter": 0, "tide_level": 1.0},
        {"obs_id": "c1", "boot_segment": "C", "counter": 0, "tide_level": 2.0},
    ])
    enable("A")
    enable("C")
    block_a = db.get_conn().execute(
        "SELECT block_id FROM observations WHERE obs_id='a1'").fetchone()["block_id"]

    # 压缩段 A 直接覆盖到压缩段 C：旧块移除、新块追加
    post_obs([{"obs_id": "a1", "boot_segment": "C", "counter": 0, "tide_level": 9.0}])
    conn = db.get_conn()
    old_entries = dict(blocks.decode_block(
        conn.execute("SELECT payload FROM tide_blocks WHERE block_id=?",
                     (block_a,)).fetchone()["payload"]))
    assert "a1" not in old_entries
    row = conn.execute("SELECT * FROM observations WHERE obs_id='a1'").fetchone()
    assert row["boot_segment"] == "C" and row["tide_level"] is None
    assert get_obs("a1")["tide_level"] == 9.0
    assert tide(0, 99999)["items"][0]["tide_level"] == 9.0


def test_compression_endpoint_published_in_openapi():
    paths = client.get("/openapi.json").json()["paths"]
    assert "/segments/{boot_segment}/compression" in paths
