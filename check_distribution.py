"""
引用数増加数の分布調査用スクリプト
config.pyの設定に基づいて、直近の引用増加数の分布を集計する。
"""
import logging
import argparse
from collections import Counter
import signal
import sys
import time

# 既存モジュールをインポート
import config
from pubmed_fetcher import search_pmids, fetch_article_details
from opencitations import get_citation_increase
from dictionary import get_mesh_query

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# 中断シグナルハンドリング
interrupted = False

def signal_handler(sig, frame):
    global interrupted
    logger.info("\n中断シグナルを受信しました。集計を完了して結果を表示します...")
    interrupted = True

signal.signal(signal.SIGINT, signal_handler)

def analyze_distribution(limit: int = 200):
    """
    指定件数の論文について引用増加数を調査し、分布を表示する。
    """
    # 1. PubMed 検索
    fields = config.DEFAULT_FIELDS
    logger.info(f"調査対象分野: {fields}")
    logger.info(f"最大調査件数: {limit}")

    mesh_queries = []
    for field in fields:
        q = get_mesh_query(field)
        if q:
            mesh_queries.append(q)

    if not mesh_queries:
        logger.error("有効なクエリがありません")
        return

    all_pmids = []
    for query in mesh_queries:
        # 検索件数を制限して取得
        pmids = search_pmids(query, retmax=limit*2) # 重複除去等を考慮して多めに
        all_pmids.extend(pmids)
    
    # 重複除去 & 件数制限
    target_pmids = list(dict.fromkeys(all_pmids))[:limit]
    logger.info(f"検索ヒット件数: {len(all_pmids)} -> 調査対象: {len(target_pmids)}件")

    if not target_pmids:
        logger.info("対象論文が見つかりませんでした")
        return

    # 2. メタデータ取得
    logger.info("メタデータ取得中...")
    articles = fetch_article_details(target_pmids)
    logger.info(f"{len(articles)}件のメタデータを取得しました")

    # 3. 引用増加数集計
    increase_counts = []
    stats = {
        "total": len(articles),
        "no_doi": 0,
        "api_error": 0,
        "zero_increase": 0,
        "positive_increase": 0,
        "increases": []
    }

    logger.info("OpenCitations APIで引用数調査中... (Ctrl+Cで中断して結果表示)")
    
    try:
        for i, article in enumerate(articles, 1):
            if interrupted:
                break
                
            doi = article.get("doi")
            if not doi:
                stats["no_doi"] += 1
                logger.debug(f"[{i}/{len(articles)}] DOIなし: {article['pmid']}")
                continue

            increase = get_citation_increase(doi)
            
            if increase is None:
                stats["api_error"] += 1
                logger.debug(f"[{i}/{len(articles)}] APIエラー: {doi}")
                continue

            stats["increases"].append(increase)
            
            if increase == 0:
                stats["zero_increase"] += 1
                # 0件の場合はログを省略（量が多いので）
                if i % 10 == 0:
                     logger.info(f"[{i}/{len(articles)}] PMID:{article['pmid']} Inc:0 (進捗確認用)")
            else:
                stats["positive_increase"] += 1
                logger.info(f"[{i}/{len(articles)}] 📈 増加あり! PMID:{article['pmid']} Inc:+{increase} Title:{article['title'][:30]}...")

            # APIレート制限遵守 (opencitations.py 内でもwaitしてるが念のため)
            # time.sleep(0.5) 
            
    except Exception as e:
        logger.error(f"エラー発生: {e}")
    
    # 4. 集計結果表示
    display_results(stats)

def display_results(stats):
    total_processed = len(stats["increases"]) + stats["no_doi"] + stats["api_error"]
    increases = stats["increases"]
    
    print("\n" + "="*60)
    print("引用増加数 分布調査結果")
    print("="*60)
    print(f"調査総数: {total_processed} 件")
    print(f"  - DOIなし: {stats['no_doi']} 件")
    print(f"  - APIエラー: {stats['api_error']} 件")
    print(f"  - 有効データ数: {len(increases)} 件")
    print("-" * 60)
    
    if not increases:
        print("有効な引用データが得られませんでした。")
        return

    # 分布集計
    counter = Counter(increases)
    sorted_keys = sorted(counter.keys())
    
    print("【引用増加数の分布】")
    for k in sorted_keys:
        count = counter[k]
        bar = "█" * (count * 50 // len(increases)) if len(increases) > 0 else ""
        if count * 50 // len(increases) == 0 and count > 0:
            bar = "▏"
        print(f"  増加数 {k:2d}: {count:3d} 件 ({count/len(increases)*100:5.1f}%) {bar}")

    print("-" * 60)
    print("【閾値別シミュレーション】")
    cumulative = 0
    # 大きい方から累積
    for k in sorted(sorted_keys, reverse=True):
        if k <= 0: break
        cumulative += counter[k]
        print(f"  閾値 {k} 以上: {cumulative:3d} 件 通知対象")

    print("="*60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="引用増加数分布調査")
    parser.add_argument("--limit", type=int, default=100, help="調査する論文の最大数")
    args = parser.parse_args()
    
    analyze_distribution(limit=args.limit)
