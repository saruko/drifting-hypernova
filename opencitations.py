"""
PubMed 引用数急増アラートシステム — OpenCitations COCI API 連携
DOI ベースで引用数の月次差分を算出する。
"""

import time
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

import requests

import config

logger = logging.getLogger(__name__)

COCI_API_BASE = "https://opencitations.net/index/coci/api/v1/citations"


def get_citation_increase(doi: str) -> int | None:
    """
    DOI を元に、先月1ヶ月間の引用数増加数を算出する。

    処理:
      1. OpenCitations COCI API から全引用レコードを取得
      2. creation フィールドの日付で先々月末・先月末時点の累計引用数を算出
      3. 差分（= 先月の増加数）を返す

    Args:
        doi: 論文の DOI

    Returns:
        先月の引用数増加数。取得失敗時は None。
    """
    if not doi:
        logger.debug("DOI が未指定のためスキップ")
        return None

    citations = _fetch_citations(doi)
    if citations is None:
        return None

    # 先月末日・先々月末日を算出
    now = datetime.now()
    # 今月の1日から1日引く = 先月末日
    end_of_last_month = date(now.year, now.month, 1) - relativedelta(days=1)
    # 先月の1日から1日引く = 先々月末日
    end_of_prev_month = date(end_of_last_month.year, end_of_last_month.month, 1) - relativedelta(days=1)

    end_count = 0
    start_count = 0
    no_creation_count = 0

    for citation in citations:
        creation = citation.get("creation")
        if not creation:
            no_creation_count += 1
            continue  # creation 未記載のレコードは除外

        citation_date = _parse_creation_date(creation)
        if citation_date is None:
            continue

        if citation_date <= end_of_last_month:
            end_count += 1
        if citation_date <= end_of_prev_month:
            start_count += 1

    increase = end_count - start_count
    logger.info(
        f"DOI={doi}: 引用総数={len(citations)}, "
        f"~{end_of_last_month}={end_count}, ~{end_of_prev_month}={start_count}, "
        f"増加={increase}, creation未記載={no_creation_count}"
    )
    return increase


def _fetch_citations(doi: str) -> list[dict] | None:
    """
    OpenCitations COCI API から引用レコードを取得する。

    Args:
        doi: 論文の DOI

    Returns:
        引用レコードのリスト。失敗時は None。
    """
    url = f"{COCI_API_BASE}/{doi}"

    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"OpenCitations: DOI={doi}, {len(data)} 件の引用レコード取得")
        return data
    except requests.RequestException as e:
        logger.error(f"OpenCitations API リクエスト失敗 (DOI={doi}): {e}")
        return None
    except ValueError as e:
        logger.error(f"OpenCitations JSON パース失敗 (DOI={doi}): {e}")
        return None
    finally:
        # レート制限遵守
        time.sleep(config.OPENCITATIONS_WAIT_SEC)


def _parse_creation_date(creation: str) -> date | None:
    """
    creation フィールドの日付文字列をパースする。

    対応フォーマット:
      - YYYY-MM-DD
      - YYYY-MM（月の1日として扱う）
      - YYYY（年の1月1日として扱う）

    Args:
        creation: 日付文字列

    Returns:
        date オブジェクト。パース不可の場合は None。
    """
    if not creation:
        return None

    parts = creation.strip().split("-")
    try:
        if len(parts) == 3:
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
        elif len(parts) == 2:
            # 年月のみ → その月の1日として扱う
            return date(int(parts[0]), int(parts[1]), 1)
        elif len(parts) == 1:
            return date(int(parts[0]), 1, 1)
        else:
            logger.warning(f"不明な日付フォーマット: '{creation}'")
            return None
    except (ValueError, IndexError) as e:
        logger.warning(f"日付パースエラー ('{creation}'): {e}")
        return None
