# 📊 PubMed 引用数急増アラートシステム

**Citation Spike Alert System**

PubMed に掲載された論文の引用数急増を自動検知し、Gemini API による日本語要約付きでメール通知するシステムです。

## ✨ 主な機能

- **引用数急増検知** — OpenCitations COCI API で月次の引用数増加を算出
- **日本語キーワード対応** — 日本語で分野を指定するだけで MeSH クエリに自動変換
- **AI 要約** — Gemini 2.0 Flash でアブストラクトを日本語3〜5文に自動要約
- **インパクトファクター表示** — 主要眼科ジャーナルの IF を自動付与
- **HTML メール通知** — リッチなデザインのアラートメールを Gmail で送信
- **GitHub Actions 自動実行** — 毎月1日に自動実行（手動トリガーも可）

## 📁 ファイル構成

```
├── config.py                 # 全設定（閾値・分野・送付間隔など）
├── dictionary.py             # 日本語→MeSH変換辞書 / ジャーナルIF辞書
├── pubmed_fetcher.py         # PubMed API連携（PMID検索・メタデータ取得）
├── opencitations.py          # OpenCitations COCI API（引用数差分算出）
├── gemini_summarizer.py      # Gemini API（アブストラクト日本語要約）
├── database.py               # SQLiteデータベース操作
├── alert.py                  # メール本文生成・Gmail SMTP送信
├── main.py                   # エントリーポイント（実行フロー制御）
├── requirements.txt          # 依存パッケージ
└── .github/workflows/
    └── citation_alert.yml    # GitHub Actionsワークフロー
```

## 🚀 セットアップ

### 1. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

### 2. GitHub Secrets の設定

リポジトリの **Settings > Secrets and variables > Actions** に以下を登録：

| シークレット名 | 内容 |
|---|---|
| `GMAIL_ADDRESS` | 送信元 Gmail アドレス |
| `GMAIL_APP_PASSWORD` | Gmail アプリパスワード（※2段階認証が必要） |
| `RECIPIENT_EMAIL` | 通知先メールアドレス |
| `GEMINI_API_KEY` | Google Gemini API キー |

### 3. 対象分野の設定（任意）

`config.py` の `DEFAULT_FIELDS` を編集して検索分野を変更できます：

```python
# 単一分野
DEFAULT_FIELDS = ['眼科']

# 複数分野
DEFAULT_FIELDS = ['眼科', '緑内障', '網膜']
```

対応キーワード一覧：眼科、緑内障、網膜、黄斑変性、加齢黄斑変性、糖尿病網膜症、白内障、近視、視神経、角膜、網膜剥離、視野、眼圧、人工知能（眼科）

## ▶️ 実行方法

### 自動実行（GitHub Actions）

毎月1日 09:00 JST に自動実行されます。Actions タブの **Run workflow** から手動実行も可能です。

### ローカル実行

```bash
# 通常実行
python main.py

# ドライラン（メール送信スキップ）
python main.py --dry-run
```

ローカル実行時は環境変数を設定してください：

```bash
export GMAIL_ADDRESS="your@gmail.com"
export GMAIL_APP_PASSWORD="your-app-password"
export RECIPIENT_EMAIL="recipient@example.com"
export GEMINI_API_KEY="your-gemini-api-key"
```

## ⚙️ 設定項目

| 設定キー | デフォルト値 | 説明 |
|---|---|---|
| `DEFAULT_FIELDS` | `['眼科']` | 検索対象分野（日本語リスト） |
| `PAPER_MIN_MONTHS` | `6` | 対象論文の最小経過月数 |
| `PAPER_MAX_MONTHS` | `24` | 対象論文の最大経過月数 |
| `CITATION_THRESHOLD` | `5` | 引用数増加の通知閾値 |
| `ALERT_INTERVAL` | `'monthly'` | 通知間隔（`'monthly'` / `'weekly'`） |

## 📋 実行フロー

1. 日本語キーワードを MeSH クエリに変換
2. PubMed API で対象期間（6〜24ヶ月前）の論文を検索
3. 各論文の DOI から OpenCitations で先月の引用数増加を算出
4. 閾値超過の論文を SQLite DB に記録
5. Gemini API でアブストラクトを日本語要約
6. ジャーナル IF を辞書から取得
7. HTML メールを生成して Gmail 送信

## ⚠️ 注意事項

- **OpenCitations カバレッジ**: DOI 未付与・Crossref 未登録の論文は引用数を取得できません
- **API レート制限**: PubMed（0.35秒）、OpenCitations（1秒）のウェイトを設けています
- **IF の更新**: `dictionary.py` の IF 辞書は年1回手動更新が必要です
- **DB 永続化**: GitHub Actions の artifacts 保持期間は90日です（長期運用時は要検討）
- **Gmail 認証**: 2段階認証を有効にしてアプリパスワードを発行してください

## 📄 ライセンス

MIT License
