"""
Landsat 下载状态管理 - SQLite + CSV（文件级追踪）
"""

import csv
import os
import sqlite3
import threading
from datetime import datetime

import config


class DownloadDB:
    """SQLite 下载状态管理，线程安全，按文件级别追踪"""

    _local = threading.local()

    def __init__(self, db_path: str = config.DB_PATH):
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """每个线程使用独立连接"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self._db_path, timeout=30)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def _init_db(self):
        conn = self._get_conn()
        # 检查旧表结构（无 filename 列），如果有则删除重建
        cur = conn.execute("PRAGMA table_info(download_status)")
        columns = [row[1] for row in cur.fetchall()]
        if columns and "filename" not in columns:
            logger_warning("检测到旧版数据库结构，正在重建...")
            conn.execute("DROP TABLE IF EXISTS download_status")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS download_status (
                path       INTEGER NOT NULL,
                row        INTEGER NOT NULL,
                year_month TEXT    NOT NULL,
                scene_id   TEXT    NOT NULL,
                product    TEXT    NOT NULL,
                filename   TEXT    NOT NULL,
                status     TEXT    NOT NULL DEFAULT 'pending',
                s3_key     TEXT,
                local_dir  TEXT,
                cloud_cover REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (path, row, year_month, scene_id, product, filename)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_status
            ON download_status(status)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_scene
            ON download_status(scene_id, product)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scene_selection (
                path       INTEGER NOT NULL,
                row        INTEGER NOT NULL,
                year_month TEXT    NOT NULL,
                scene_id_l2 TEXT   NOT NULL,
                scene_id_l1 TEXT,
                cloud_cover REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (path, row, year_month)
            )
        """)
        conn.commit()

    # ========== 场景选择缓存 ==========

    def save_scene_selection(self, path: int, row: int, year_month: str,
                             scene_id_l2: str, scene_id_l1: str | None,
                             cloud_cover: float):
        """保存某月某 Path/Row 选择的场景信息，避免重复 STAC 查询"""
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO scene_selection
                (path, row, year_month, scene_id_l2, scene_id_l1, cloud_cover, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (path, row, year_month, scene_id_l2, scene_id_l1,
              cloud_cover, datetime.now().isoformat()))
        conn.commit()

    def get_scene_selection(self, path: int, row: int, year_month: str) -> dict | None:
        """获取已保存的场景选择"""
        conn = self._get_conn()
        try:
            cur = conn.execute("""
                SELECT * FROM scene_selection
                WHERE path = ? AND row = ? AND year_month = ?
            """, (path, row, year_month))
            row_data = cur.fetchone()
            return dict(row_data) if row_data else None
        except sqlite3.OperationalError:
            # 表不存在（首次运行）
            return None

    # ========== 文件级状态管理 ==========

    def mark_file_pending(self, path: int, row: int, year_month: str,
                          scene_id: str, product: str, filename: str,
                          s3_key: str = None, cloud_cover: float = None):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO download_status
                (path, row, year_month, scene_id, product, filename, status,
                 s3_key, cloud_cover, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
        """, (path, row, year_month, scene_id, product, filename,
              s3_key, cloud_cover, datetime.now().isoformat()))
        conn.commit()

    def mark_file_downloading(self, path: int, row: int, year_month: str,
                              scene_id: str, product: str, filename: str,
                              local_dir: str = None):
        conn = self._get_conn()
        conn.execute("""
            UPDATE download_status
            SET status = 'downloading', local_dir = ?, updated_at = ?
            WHERE path = ? AND row = ? AND year_month = ? AND scene_id = ?
              AND product = ? AND filename = ?
        """, (local_dir, datetime.now().isoformat(),
              path, row, year_month, scene_id, product, filename))
        conn.commit()

    def mark_file_downloaded(self, path: int, row: int, year_month: str,
                             scene_id: str, product: str, filename: str,
                             local_dir: str):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO download_status
                (path, row, year_month, scene_id, product, filename, status,
                 local_dir, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'downloaded', ?, ?)
        """, (path, row, year_month, scene_id, product, filename,
              local_dir, datetime.now().isoformat()))
        conn.commit()
        _sync_csv(conn)

    def mark_file_failed(self, path: int, row: int, year_month: str,
                         scene_id: str, product: str, filename: str):
        conn = self._get_conn()
        conn.execute("""
            UPDATE download_status
            SET status = 'failed', updated_at = ?
            WHERE path = ? AND row = ? AND year_month = ? AND scene_id = ?
              AND product = ? AND filename = ?
        """, (datetime.now().isoformat(),
              path, row, year_month, scene_id, product, filename))
        conn.commit()
        _sync_csv(conn)

    def is_file_downloaded(self, path: int, row: int, year_month: str,
                           scene_id: str, product: str, filename: str) -> bool:
        """检查某个具体文件是否已下载"""
        conn = self._get_conn()
        cur = conn.execute("""
            SELECT status FROM download_status
            WHERE path = ? AND row = ? AND year_month = ? AND scene_id = ?
              AND product = ? AND filename = ?
        """, (path, row, year_month, scene_id, product, filename))
        row_data = cur.fetchone()
        return row_data is not None and row_data["status"] == "downloaded"

    def get_downloaded_files(self, scene_id: str, product: str) -> set[str]:
        """获取某个场景已下载的文件名集合"""
        conn = self._get_conn()
        cur = conn.execute("""
            SELECT filename FROM download_status
            WHERE scene_id = ? AND product = ? AND status = 'downloaded'
        """, (scene_id, product))
        return {row["filename"] for row in cur.fetchall()}

    def get_all_downloaded_files_for_scene(self, scene_id: str) -> set[str]:
        """获取某个场景所有产品（L1+L2）已下载的文件名集合"""
        conn = self._get_conn()
        cur = conn.execute("""
            SELECT filename FROM download_status
            WHERE scene_id = ? AND status = 'downloaded'
        """, (scene_id,))
        return {row["filename"] for row in cur.fetchall()}

    def get_failed_records(self) -> list[dict]:
        """获取所有失败的记录，用于 --retry"""
        conn = self._get_conn()
        cur = conn.execute("""
            SELECT * FROM download_status WHERE status = 'failed'
        """)
        return [dict(r) for r in cur.fetchall()]

    def reset_failed_to_pending(self):
        """将所有失败记录重置为 pending"""
        conn = self._get_conn()
        conn.execute("""
            UPDATE download_status SET status = 'pending', updated_at = ?
            WHERE status = 'failed'
        """, (datetime.now().isoformat(),))
        conn.commit()

    def reset_downloading_to_pending(self):
        """将所有 downloading 状态重置为 pending（中断恢复）"""
        conn = self._get_conn()
        conn.execute("""
            UPDATE download_status SET status = 'pending', updated_at = ?
            WHERE status = 'downloading'
        """, (datetime.now().isoformat(),))
        conn.commit()

    def get_downloaded_records(self) -> list[dict]:
        """获取所有已下载的记录"""
        conn = self._get_conn()
        cur = conn.execute("""
            SELECT * FROM download_status WHERE status = 'downloaded'
        """)
        return [dict(r) for r in cur.fetchall()]

    def is_scene_fully_downloaded(self, path: int, row: int,
                                  year_month: str) -> bool:
        """检查指定 (path, row, year_month) 的所有文件是否已全部下载完成"""
        conn = self._get_conn()
        cur = conn.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status = 'downloaded' THEN 1 ELSE 0 END) AS done
            FROM download_status
            WHERE path = ? AND row = ? AND year_month = ?
        """, (path, row, year_month))
        row_data = cur.fetchone()
        if row_data["total"] == 0:
            return False
        return row_data["total"] == row_data["done"]


def should_skip_downloaded(db: DownloadDB, path: int, row: int,
                          year_month: str) -> bool:
    """
    判断是否应跳过已下载完成的场景。

    当该 (path, row, year_month) 在之前的下载任务（如浙江省下载）中
    已全部下载完成时返回 True，避免重复下载和重复查询 STAC API。
    """
    return db.is_scene_fully_downloaded(path, row, year_month)


def _sync_csv(conn: sqlite3.Connection):
    """将 SQLite 中所有记录同步到 CSV"""
    csv_path = config.CSV_PATH
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    cur = conn.execute("""
        SELECT path, row, year_month, scene_id, product, filename, status,
               local_dir, cloud_cover, updated_at as download_time
        FROM download_status ORDER BY path, row, year_month, scene_id, product
    """)
    rows = cur.fetchall()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "path", "row", "year_month", "scene_id", "product", "filename",
            "status", "local_dir", "cloud_cover", "download_time"
        ])
        writer.writeheader()
        for r in rows:
            writer.writerow(dict(r))


def logger_warning(msg: str):
    """简单日志输出（避免循环导入）"""
    import logging
    logging.getLogger(__name__).warning(msg)
