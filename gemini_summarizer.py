"""
PubMed 引用数急増アラートシステム — Gemini API 要約
Gemini API でアブストラクトを日本語要約する。
（google-genai SDK を使用）
"""

import logging
import time

from google import genai

import config

logger = logging.getLogger(__name__)

# 要約プロンプト
SUMMARIZE_PROMPT = (
    "以下の医学論文アブストラクトを3〜5文で日本語要約してください。"
    "専門用語はそのまま使用し、簡潔にまとめてください。\n\n"
)

# リトライ設定
MAX_RETRIES = 3
RETRY_WAIT_SEC = 5

# モデル名
MODEL_NAME = "gemini-2.0-flash"


def _get_client() -> genai.Client:
    """Gemini クライアントを初期化して返す。"""
    return genai.Client(api_key=config.GEMINI_API_KEY)


def summarize_abstract(abstract: str) -> str:
    """
    英語アブストラクトを Gemini API で日本語要約する。

    Args:
        abstract: 英語のアブストラクト

    Returns:
        日本語要約テキスト。エラー時はエラーメッセージ。
    """
    if not abstract or not abstract.strip():
        return "（アブストラクトが存在しないため要約できません）"

    if not config.GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY が設定されていません")
        return "（Gemini API キーが未設定のため要約をスキップしました）"

    client = _get_client()
    prompt = SUMMARIZE_PROMPT + abstract

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
            )
            summary = response.text.strip()
            logger.info(f"Gemini 要約完了 ({len(summary)} 文字)")
            return summary
        except Exception as e:
            logger.warning(
                f"Gemini API エラー (試行 {attempt}/{MAX_RETRIES}): {e}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT_SEC * attempt)

    return "（Gemini API による要約に失敗しました）"
