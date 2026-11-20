"""潮位无损分块存储。

压缩段（segment_settings.compression_enabled = 1）的潮位原值不再存于
observations 表，而是按开机段归集到 tide_blocks 块中：

- 每块至多 MAX_ENTRIES_PER_BLOCK（256）条，块内条目为
  ``[obs_id, tide_level]``；
- 载荷用 JSON 保证浮点逐位可逆（repr/parse 往返一致），再经 zlib
  无损压缩后以 BLOB 落库；
- observations 只保留 block_id 块引用与 tide_level = NULL。

所有函数都假定运行在 db.transaction 的同一事务内。读取侧用
BlockReader 对同一次请求中重复访问的块只解压一次；单条查询、UTC
短窗查询、单条更正因此只解压实际涉及的块。
"""

from __future__ import annotations

import json
import sqlite3
import zlib

MAX_ENTRIES_PER_BLOCK = 256
_ZLIB_LEVEL = 6


# ---------------------------------------------------------------- 编解码

def encode_block(entries: list[tuple[str, float]]) -> bytes:
    """块内条目 -> zlib 压缩 BLOB（JSON 保证潮位浮点无损往返）。"""
    return zlib.compress(json.dumps(entries, separators=(",", ":")).encode("utf-8"),
                         _ZLIB_LEVEL)


def decode_block(payload: bytes) -> list[tuple[str, float]]:
    """压缩 BLOB -> 块内条目。集中在此处便于测试统计解压次数。"""
    return [tuple(item) for item in json.loads(zlib.decompress(payload).decode("utf-8"))]


# ---------------------------------------------------------------- 读取

class BlockReader:
    """按请求缓存解压结果：同一块在一次请求里最多解压一次。"""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._cache: dict[int, list[tuple[str, float]]] = {}

    def _load(self, block_id: int) -> list[tuple[str, float]]:
        cached = self._cache.get(block_id)
        if cached is not None:
            return cached
        row = self._conn.execute(
            "SELECT payload FROM tide_blocks WHERE block_id = ?", (block_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"压缩块 {block_id} 不存在")
        entries = decode_block(row["payload"])
        self._cache[block_id] = entries
        return entries

    def tide_level(self, block_id: int, obs_id: str) -> float:
        """只解压该引用所在的块并取回潮位原值。"""
        for oid, level in self._load(block_id):
            if oid == obs_id:
                return level
        raise KeyError(f"观测 {obs_id} 不在压缩块 {block_id} 内")

    def levels_for(self, refs: list[tuple[str, int]]) -> dict[str, float]:
        """批量取 (obs_id, block_id) 引用的潮位；按块去重解压。"""
        wanted: dict[int, set[str]] = {}
        for obs_id, block_id in refs:
            wanted.setdefault(block_id, set()).add(obs_id)
        result: dict[str, float] = {}
        for block_id, ids in wanted.items():
            for oid, level in self._load(block_id):
                if oid in ids:
                    result[oid] = level
        return result


# ---------------------------------------------------------------- 写入

def _write_block(conn: sqlite3.Connection, block_id: int | None,
                 segment: str, entries: list[tuple[str, float]]) -> int:
    """新建或覆盖一个块，返回 block_id。"""
    payload = encode_block(entries)
    if block_id is None:
        cur = conn.execute(
            "INSERT INTO tide_blocks (boot_segment, entry_count, payload) "
            "VALUES (?, ?, ?)",
            (segment, len(entries), payload),
        )
        return int(cur.lastrowid)
    conn.execute(
        "UPDATE tide_blocks SET entry_count = ?, payload = ? WHERE block_id = ?",
        (len(entries), payload, block_id),
    )
    return block_id


def open_block_id(conn: sqlite3.Connection, segment: str) -> int | None:
    """该段当前可追加（未满 256 条）的块；没有则返回 None。"""
    row = conn.execute(
        "SELECT block_id FROM tide_blocks "
        "WHERE boot_segment = ? AND entry_count < ? "
        "ORDER BY block_id DESC LIMIT 1",
        (segment, MAX_ENTRIES_PER_BLOCK),
    ).fetchone()
    return None if row is None else row["block_id"]


def append_to_segment(conn: sqlite3.Connection, segment: str,
                      obs_id: str, tide_level: float) -> int:
    """把一条潮位追加进该段的开放块（满则新建），返回块引用。"""
    block_id = open_block_id(conn, segment)
    if block_id is None:
        return create_block(conn, segment, [(obs_id, tide_level)])
    row = conn.execute(
        "SELECT payload FROM tide_blocks WHERE block_id = ?", (block_id,)
    ).fetchone()
    entries = decode_block(row["payload"])
    entries.append((obs_id, tide_level))
    _write_block(conn, block_id, segment, entries)
    return block_id


def create_block(conn: sqlite3.Connection, segment: str,
                 entries: list[tuple[str, float]]) -> int:
    """以给定条目新建一个压缩块，返回新 block_id。"""
    if len(entries) > MAX_ENTRIES_PER_BLOCK:
        raise ValueError(f"单块不得超过 {MAX_ENTRIES_PER_BLOCK} 条")
    return _write_block(conn, None, segment, entries)


def replace_in_block(conn: sqlite3.Connection, block_id: int,
                     obs_id: str, tide_level: float) -> None:
    """同 ID 更正块内潮位原值；只解压并重写涉及的这一个块。"""
    row = conn.execute(
        "SELECT boot_segment, payload FROM tide_blocks WHERE block_id = ?",
        (block_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"压缩块 {block_id} 不存在")
    entries = decode_block(row["payload"])
    for i, (oid, _level) in enumerate(entries):
        if oid == obs_id:
            entries[i] = (obs_id, tide_level)
            break
    else:
        raise KeyError(f"观测 {obs_id} 不在压缩块 {block_id} 内")
    _write_block(conn, block_id, row["boot_segment"], entries)


def remove_from_block(conn: sqlite3.Connection, block_id: int, obs_id: str) -> None:
    """从块内移除一条（跨段覆盖时旧段块不再保留该原值）。

    空块保留在库中（entry_count=0，仍可被后续追加复用），块引用由
    调用方在 observations 上改写，因此不存在悬空引用。
    """
    row = conn.execute(
        "SELECT boot_segment, payload FROM tide_blocks WHERE block_id = ?",
        (block_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"压缩块 {block_id} 不存在")
    entries = decode_block(row["payload"])
    remaining = [item for item in entries if item[0] != obs_id]
    if len(remaining) == len(entries):
        raise KeyError(f"观测 {obs_id} 不在压缩块 {block_id} 内")
    _write_block(conn, block_id, row["boot_segment"], remaining)
