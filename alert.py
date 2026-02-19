"""
PubMed 引用数急増アラートシステム — メール通知
閾値超過論文のメール本文生成・Gmail SMTP 送信。
"""

import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import config
from dictionary import get_impact_factor

logger = logging.getLogger(__name__)


def build_email_body(alerts: list[dict]) -> str:
    """
    アラートリストからHTML形式のメール本文を生成する。

    Args:
        alerts: アラートレコードの辞書リスト。各辞書は以下のキーを持つ:
            - title, journal, published_date, pmid, doi,
              citation_increase, summary (Gemini要約)

    Returns:
        HTML メール本文
    """
    html_parts = []
    html_parts.append("""
    <html>
    <head>
    <meta charset="utf-8">
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; color: #333; max-width: 800px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #1a5276, #2e86c1); color: white; padding: 20px; border-radius: 8px 8px 0 0; }
        .header h1 { margin: 0; font-size: 22px; }
        .header p { margin: 5px 0 0; opacity: 0.9; font-size: 14px; }
        .article { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; margin: 16px 0; padding: 20px; }
        .article h2 { font-size: 16px; color: #1a5276; margin: 0 0 12px; line-height: 1.4; }
        .meta-table { width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 14px; }
        .meta-table td { padding: 4px 8px; vertical-align: top; }
        .meta-table td:first-child { color: #666; width: 160px; font-weight: 600; }
        .badge { display: inline-block; background: #e74c3c; color: white; padding: 2px 10px; border-radius: 12px; font-weight: bold; font-size: 14px; }
        .summary { background: white; border-left: 4px solid #2e86c1; padding: 12px 16px; margin: 12px 0; font-size: 14px; line-height: 1.6; }
        .link { color: #2e86c1; text-decoration: none; }
        .link:hover { text-decoration: underline; }
        .footer { text-align: center; padding: 20px; color: #999; font-size: 12px; }
    </style>
    </head>
    <body>
    <div class="header">
        <h1>📊 PubMed 引用数急増アラート</h1>
        <p>先月の引用数急増論文をお知らせします</p>
    </div>
    """)

    html_parts.append(f'<p style="padding: 10px 0; font-size: 14px;">検知された論文数: <strong>{len(alerts)} 件</strong></p>')

    for i, alert in enumerate(alerts, 1):
        pmid = alert.get("pmid", "N/A")
        doi = alert.get("doi", "N/A")
        title = alert.get("title", "N/A")
        journal = alert.get("journal", "N/A")
        published_date = alert.get("published_date", "N/A")
        increase = alert.get("citation_increase", 0)
        summary = alert.get("summary", "（要約なし）")
        impact_factor = get_impact_factor(journal)
        pubmed_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        html_parts.append(f"""
        <div class="article">
            <h2>{i}. {title}</h2>
            <table class="meta-table">
                <tr><td>ジャーナル</td><td>{journal}</td></tr>
                <tr><td>インパクトファクター</td><td>{impact_factor}</td></tr>
                <tr><td>公開日</td><td>{published_date}</td></tr>
                <tr><td>PMID</td><td>{pmid}</td></tr>
                <tr><td>DOI</td><td>{doi if doi else 'N/A'}</td></tr>
                <tr><td>先月の引用数増加</td><td><span class="badge">+{increase}</span></td></tr>
            </table>
            <div class="summary">
                <strong>📝 日本語要約:</strong><br>
                {summary}
            </div>
            <p><a class="link" href="{pubmed_url}" target="_blank">🔗 PubMed で閲覧</a></p>
        </div>
        """)

    html_parts.append("""
    <div class="footer">
        <p>このメールは PubMed 引用数急増アラートシステムにより自動送信されました。</p>
    </div>
    </body></html>
    """)

    return "\n".join(html_parts)


def send_alert_email(alerts: list[dict]) -> bool:
    """
    アラートメールを Gmail SMTP で送信する。

    Args:
        alerts: アラートレコードのリスト

    Returns:
        True: 送信成功、 False: 送信失敗
    """
    if not alerts:
        logger.info("通知対象の論文がありません。メール送信をスキップします。")
        return False

    if not config.GMAIL_ADDRESS or not config.GMAIL_APP_PASSWORD:
        logger.error("Gmail 認証情報が設定されていません")
        return False

    if not config.RECIPIENT_EMAIL:
        logger.error("送信先メールアドレスが設定されていません")
        return False

    html_body = build_email_body(alerts)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📊 PubMed 引用数急増アラート — {len(alerts)} 件の論文を検知"
    msg["From"] = config.GMAIL_ADDRESS
    msg["To"] = config.RECIPIENT_EMAIL

    # プレーンテキスト版（フォールバック）
    text_body = _build_plain_text(alerts)
    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(config.GMAIL_ADDRESS, config.GMAIL_APP_PASSWORD)
            server.sendmail(
                config.GMAIL_ADDRESS,
                config.RECIPIENT_EMAIL,
                msg.as_string(),
            )
        logger.info(f"アラートメール送信完了: {config.RECIPIENT_EMAIL}")
        return True
    except smtplib.SMTPException as e:
        logger.error(f"メール送信失敗: {e}")
        return False


def _build_plain_text(alerts: list[dict]) -> str:
    """プレーンテキスト版のメール本文を生成する。"""
    lines = [
        "PubMed 引用数急増アラート",
        "=" * 40,
        f"検知された論文数: {len(alerts)} 件",
        "",
    ]

    for i, alert in enumerate(alerts, 1):
        pmid = alert.get("pmid", "N/A")
        doi = alert.get("doi", "N/A")
        title = alert.get("title", "N/A")
        journal = alert.get("journal", "N/A")
        published_date = alert.get("published_date", "N/A")
        increase = alert.get("citation_increase", 0)
        summary = alert.get("summary", "（要約なし）")
        impact_factor = get_impact_factor(journal)

        lines.extend([
            f"--- 論文 {i} ---",
            f"タイトル: {title}",
            f"ジャーナル: {journal}",
            f"インパクトファクター: {impact_factor}",
            f"公開日: {published_date}",
            f"PMID: {pmid}",
            f"DOI: {doi if doi else 'N/A'}",
            f"先月の引用数増加: +{increase}",
            f"",
            f"日本語要約:",
            f"{summary}",
            f"",
            f"PubMed: https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            "",
        ])

    return "\n".join(lines)
