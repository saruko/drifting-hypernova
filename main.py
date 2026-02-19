"""
PubMed 引用数急増アラートシステム — エントリーポイント

実行フロー（仕様書セクション11）:
  1. config.py から設定読み込み
  2. 日本語キーワードを MeSH クエリに変換
  3. PubMed API で対象期間の PMID 一覧を取得
  4. PMID からメタデータ（DOI・ジャーナル名・アブストラクト）を取得
  5. OpenCitations COCI で先月の引用数増加を算出
  6. 閾値超過論文を SQLite DB に記録
  7. 未通知レコードを取得
  8. Gemini API でアブストラクトを日本語要約
  9. ジャーナル IF を辞書から取得
  10. メール本文を生成して Gmail 送信
  11. 送信済み論文の notified を更新
"""

import argparse
import logging
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta

import config
from dictionary import get_mesh_query
from pubmed_fetcher import search_pmids, fetch_article_details
from opencitations import get_citation_increase
from gemini_summarizer import summarize_abstract
from database import init_db, insert_alert, get_pending_alerts, mark_as_notified
from alert import send_alert_email

# =========================================================
# ロギング設定
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _get_detected_month() -> str:
    """先月の年月文字列（YYYY-MM）を返す。"""
    now = datetime.now()
    last_month = now - relativedelta(months=1)
    return last_month.strftime("%Y-%m")


def run(dry_run: bool = False) -> None:
    """
    メイン実行フロー。

    Args:
        dry_run: True の場合、メール送信をスキップ
    """
    logger.info("=" * 60)
    logger.info("PubMed 引用数急増アラートシステム — 実行開始")
    logger.info("=" * 60)

    if dry_run:
        logger.info("*** ドライランモード: メール送信はスキップされます ***")

    # ステップ 1: 設定読み込み
    logger.info("ステップ 1: 設定読み込み")
    fields = config.DEFAULT_FIELDS
    threshold = config.CITATION_THRESHOLD
    logger.info(f"  対象分野: {fields}")
    logger.info(f"  閾値: {threshold}")

    # DB初期化
    init_db()

    # ステップ 2: MeSH クエリ変換
    logger.info("ステップ 2: 日本語→MeSH クエリ変換")
    mesh_queries = []
    for field in fields:
        query = get_mesh_query(field)
        if query:
            mesh_queries.append(query)
            logger.info(f"  '{field}' → '{query}'")

    if not mesh_queries:
        logger.error("有効な MeSH クエリがありません。終了します。")
        return

    # ステップ 3: PubMed 検索
    logger.info("ステップ 3: PubMed API で PMID 一覧を取得")
    all_pmids = []
    for query in mesh_queries:
        pmids = search_pmids(query)
        all_pmids.extend(pmids)

    # 重複除去
    all_pmids = list(dict.fromkeys(all_pmids))
    logger.info(f"  合計 PMID 数: {len(all_pmids)}")

    if not all_pmids:
        logger.info("対象 PMID がありません。終了します。")
        return

    # ステップ 4: メタデータ取得
    logger.info("ステップ 4: PubMed efetch でメタデータ取得")
    articles = fetch_article_details(all_pmids)
    logger.info(f"  取得した論文数: {len(articles)}")

    # ステップ 5-6: 引用数差分計算 + DB記録
    logger.info("ステップ 5-6: OpenCitations で引用数差分計算 + DB 記録")
    detected_month = _get_detected_month()
    logger.info(f"  検知対象月: {detected_month}")
    spike_count = 0

    # 統計カウンター
    stats_no_doi = 0
    stats_api_fail = 0
    stats_zero_citations = 0
    stats_has_citations = 0
    stats_increase_zero = 0
    stats_increase_positive = 0
    all_increases = []

    total = len(articles)
    for idx, article in enumerate(articles, 1):
        doi = article.get("doi")
        if not doi:
            stats_no_doi += 1
            continue

        if idx % 50 == 0:
            logger.info(f"  進捗: {idx}/{total} 件処理済み...")

        increase = get_citation_increase(doi)
        if increase is None:
            stats_api_fail += 1
            continue

        all_increases.append(increase)
        if increase == 0:
            stats_increase_zero += 1
        elif increase > 0:
            stats_increase_positive += 1

        if increase > threshold:
            spike_count += 1
            logger.info(
                f"  🔔 引用急増検知: PMID={article['pmid']}, "
                f"増加数={increase}, タイトル={article['title'][:60]}..."
            )
            insert_alert(
                pmid=article["pmid"],
                doi=doi,
                title=article["title"],
                journal=article["journal"],
                published_date=article["published_date"],
                citation_increase=increase,
                detected_month=detected_month,
            )

    # 統計サマリーを出力
    logger.info("  === OpenCitations 処理統計 ===")
    logger.info(f"  総論文数:           {total}")
    logger.info(f"  DOI なし:           {stats_no_doi}")
    logger.info(f"  API 失敗:           {stats_api_fail}")
    logger.info(f"  増加数 = 0:         {stats_increase_zero}")
    logger.info(f"  増加数 > 0:         {stats_increase_positive}")
    logger.info(f"  閾値超過 (>{threshold}):    {spike_count}")
    if all_increases:
        logger.info(f"  増加数 最大値:      {max(all_increases)}")
        logger.info(f"  増加数 Top5:        {sorted(all_increases, reverse=True)[:5]}")

    # ステップ 7: 未通知レコード取得
    logger.info("ステップ 7: 未通知レコードの取得")
    pending = get_pending_alerts(detected_month)
    logger.info(f"  未通知レコード数: {len(pending)}")

    if not pending:
        logger.info("通知対象がありません。終了します。")
        return

    # ステップ 8: Gemini 要約
    logger.info("ステップ 8: Gemini API でアブストラクト日本語要約")
    for alert_record in pending:
        pmid = alert_record["pmid"]
        # アブストラクトを取得（articlesから検索）
        abstract = None
        for article in articles:
            if article["pmid"] == pmid:
                abstract = article.get("abstract")
                break

        if abstract:
            summary = summarize_abstract(abstract)
        else:
            summary = "（アブストラクトが存在しないため要約できません）"

        alert_record["summary"] = summary
        logger.info(f"  PMID={pmid}: 要約完了")

    # ステップ 9: IF はメール生成時に dictionary.py から自動取得

    # ステップ 10: メール送信
    logger.info("ステップ 10: アラートメール送信")
    if dry_run:
        logger.info("  [ドライラン] メール送信をスキップ")
        logger.info("  === メール内容プレビュー ===")
        for alert_record in pending:
            logger.info(
                f"  タイトル: {alert_record.get('title', 'N/A')}"
            )
            logger.info(
                f"  増加数: +{alert_record.get('citation_increase', 0)}"
            )
            logger.info(
                f"  要約: {alert_record.get('summary', 'N/A')[:100]}..."
            )
            logger.info("  ---")
    else:
        success = send_alert_email(pending)
        if not success:
            logger.error("メール送信に失敗しました")
            return

    # ステップ 11: notified 更新
    logger.info("ステップ 11: notified フラグを更新")
    alert_ids = [a["id"] for a in pending]
    if not dry_run:
        mark_as_notified(alert_ids)
        logger.info(f"  {len(alert_ids)} 件を通知済みに更新")
    else:
        logger.info(f"  [ドライラン] {len(alert_ids)} 件の更新をスキップ")

    logger.info("=" * 60)
    logger.info("PubMed 引用数急増アラートシステム — 実行完了")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="PubMed 引用数急増アラートシステム"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="メール送信をスキップしてフロー全体をテスト実行",
    )
    args = parser.parse_args()

    try:
        run(dry_run=args.dry_run)
    except Exception as e:
        logger.exception(f"予期せぬエラーが発生: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
