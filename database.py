"""
PubMed 引用数急増アラートシステム — データベース操作
SQLite でアラートイベントを記録・管理する。
"""

import sqlite3
import logging

import config

logger = logging.getLogger(__name__)

# =========================================================
# テーブル作成SQL
# =========================================================
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS alerts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    pmid             TEXT    NOT NULL,
    doi              TEXT,
    title            TEXT,
    journal          TEXT,
    published_date   TEXT,
    citation_increase INTEGER NOT NULL,
    detected_month   TEXT    NOT NULL,
    notified         INTEGER NOT NULL DEFAULT 0
);
"""


def _get_connection() -> sqlite3.Connection:
    """DB 接続を返す（row_factory 設定済み）。"""
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """データベースとテーブルを初期化する。"""
    conn = _get_connection()
    try:
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info(f"データベースを初期化しました: {config.DB_PATH}")
    finally:
        conn.close()


def insert_alert(
    pmid: str,
    doi: str,
    title: str,
    journal: str,
    published_date: str,
    citation_increase: int,
    detected_month: str,
) -> bool:
    """
    アラートレコードを挿入する。同じ PMID + detected_month の重複は挿入しない。

    Returns:
        True: 挿入成功、 False: 重複のためスキップ
    """
    conn = _get_connection()
    try:
        # 重複チェック
        existing = conn.execute(
            "SELECT id FROM alerts WHERE pmid = ? AND detected_month = ?",
            (pmid, detected_month),
        ).fetchone()

        if existing:
            logger.debug(f"重複レコードをスキップ: PMID={pmid}, month={detected_month}")
            return False

        conn.execute(
            """
            INSERT INTO alerts
                (pmid, doi, title, journal, published_date, citation_increase, detected_month, notified)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (pmid, doi, title, journal, published_date, citation_increase, detected_month),
        )
        conn.commit()
        logger.info(f"アラート記録: PMID={pmid}, 増加数={citation_increase}")
        return True
    finally:
        conn.close()


def get_pending_alerts(detected_month: str) -> list[dict]:
    """
    指定月の未通知アラートを引用数増加数の降順で取得する。

    Args:
        detected_month: 検知年月（例: '2025-01'）

    Returns:
        アラートレコードの辞書リスト
    """
    conn = _get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, pmid, doi, title, journal, published_date,
                   citation_increase, detected_month, notified
            FROM alerts
            WHERE detected_month = ? AND notified = 0
            ORDER BY citation_increase DESC
            """,
            (detected_month,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def mark_as_notified(alert_ids: list[int]) -> None:
    """
    指定した ID のアラートを通知済み（notified=1）に更新する。
    """
    if not alert_ids:
        return

    conn = _get_connection()
    try:
        placeholders = ','.join('?' for _ in alert_ids)
        conn.execute(
            f"UPDATE alerts SET notified = 1 WHERE id IN ({placeholders})",
            alert_ids,
        )
        conn.commit()
        logger.info(f"通知済みに更新: {len(alert_ids)} 件")
    finally:
        conn.close()
