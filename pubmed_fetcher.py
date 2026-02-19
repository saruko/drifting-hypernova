"""
PubMed 引用数急増アラートシステム — PubMed API 連携
E-utilities (esearch / efetch) で論文メタデータを取得する。
"""

import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests

import config

logger = logging.getLogger(__name__)

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def _build_date_range() -> tuple[str, str]:
    """
    対象論文の公開日範囲を算出する。
    現在日から PAPER_MAX_MONTHS 前 〜 PAPER_MIN_MONTHS 前。

    Returns:
        (mindate, maxdate) — YYYY/MM/DD 形式
    """
    now = datetime.now()
    min_date = now - relativedelta(months=config.PAPER_MAX_MONTHS)
    max_date = now - relativedelta(months=config.PAPER_MIN_MONTHS)
    return min_date.strftime("%Y/%m/%d"), max_date.strftime("%Y/%m/%d")


def search_pmids(mesh_query: str, retmax: int = 500) -> list[str]:
    """
    MeSH クエリで PubMed を検索し、PMID リストを返す。

    Args:
        mesh_query: MeSH クエリ文字列（例: 'Ophthalmology[MeSH]'）
        retmax: 最大取得件数

    Returns:
        PMID のリスト
    """
    mindate, maxdate = _build_date_range()
    params = {
        "db": "pubmed",
        "term": mesh_query,
        "datetype": "pdat",
        "mindate": mindate,
        "maxdate": maxdate,
        "retmax": retmax,
        "retmode": "xml",
    }

    logger.info(f"PubMed 検索: query='{mesh_query}', 期間={mindate}〜{maxdate}")

    try:
        resp = requests.get(ESEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"PubMed esearch リクエスト失敗: {e}")
        return []

    root = ET.fromstring(resp.text)
    id_list = root.find("IdList")
    if id_list is None:
        logger.warning("PubMed esearch: IdList が見つかりません")
        return []

    pmids = [id_elem.text for id_elem in id_list.findall("Id") if id_elem.text]
    logger.info(f"PubMed 検索結果: {len(pmids)} 件の PMID を取得")
    return pmids


def fetch_article_details(pmids: list[str]) -> list[dict]:
    """
    PMID リストから各論文のメタデータを取得する。

    Args:
        pmids: PMID のリスト

    Returns:
        論文情報の辞書リスト。各辞書は以下のキーを持つ:
        - pmid, doi, title, journal, published_date, abstract
    """
    if not pmids:
        return []

    articles = []
    batch_size = 100  # efetch は一度に最大 100 件

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i : i + batch_size]
        params = {
            "db": "pubmed",
            "id": ",".join(batch),
            "retmode": "xml",
        }

        try:
            resp = requests.get(EFETCH_URL, params=params, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"PubMed efetch リクエスト失敗: {e}")
            continue

        root = ET.fromstring(resp.text)

        for article_elem in root.findall(".//PubmedArticle"):
            article = _parse_article(article_elem)
            if article:
                articles.append(article)

        # レート制限遵守
        time.sleep(config.PUBMED_WAIT_SEC)

    logger.info(f"PubMed efetch: {len(articles)} 件の論文データを取得")
    return articles


def _parse_article(article_elem: ET.Element) -> dict | None:
    """
    PubmedArticle XML 要素から必要なメタデータを抽出する。
    """
    try:
        medline = article_elem.find(".//MedlineCitation")
        if medline is None:
            return None

        # PMID
        pmid_elem = medline.find("PMID")
        pmid = pmid_elem.text if pmid_elem is not None else None
        if not pmid:
            return None

        article = medline.find("Article")
        if article is None:
            return None

        # タイトル
        title_elem = article.find("ArticleTitle")
        title = title_elem.text if title_elem is not None else "N/A"

        # ジャーナル名
        journal_elem = article.find(".//Journal/Title")
        journal = journal_elem.text if journal_elem is not None else "N/A"

        # DOI
        doi = None
        for id_elem in article.findall(".//ELocationID"):
            if id_elem.get("EIdType") == "doi":
                doi = id_elem.text
                break
        # ArticleId からも探索
        if not doi:
            article_data = article_elem.find(".//PubmedData")
            if article_data is not None:
                for id_elem in article_data.findall(".//ArticleId"):
                    if id_elem.get("IdType") == "doi":
                        doi = id_elem.text
                        break

        # 公開日
        published_date = _extract_pub_date(article)

        # アブストラクト
        abstract = _extract_abstract(article)

        return {
            "pmid": pmid,
            "doi": doi,
            "title": title,
            "journal": journal,
            "published_date": published_date,
            "abstract": abstract,
        }

    except Exception as e:
        logger.error(f"論文データのパースに失敗: {e}")
        return None


def _extract_pub_date(article_elem: ET.Element) -> str:
    """公開日を抽出する（YYYY-MM-DD 形式）。"""
    pub_date = article_elem.find(".//Journal/JournalIssue/PubDate")
    if pub_date is None:
        return "N/A"

    year = pub_date.findtext("Year", "")
    month = pub_date.findtext("Month", "01")
    day = pub_date.findtext("Day", "01")

    # 月が英語名の場合を数値に変換
    month_map = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
        "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
        "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
    }
    month = month_map.get(month, month)

    if year:
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    return "N/A"


def _extract_abstract(article_elem: ET.Element) -> str | None:
    """アブストラクトテキストを抽出する。構造化アブストラクトに対応。"""
    abstract_elem = article_elem.find(".//Abstract")
    if abstract_elem is None:
        return None

    parts = []
    for text_elem in abstract_elem.findall("AbstractText"):
        label = text_elem.get("Label", "")
        # テキストと子要素テキストをすべて結合
        text_content = "".join(text_elem.itertext())
        if label:
            parts.append(f"{label}: {text_content}")
        else:
            parts.append(text_content)

    return " ".join(parts) if parts else None
