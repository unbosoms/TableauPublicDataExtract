# Tableau Public Data Extract

Tableau Public に公開しているワークブックのビュー数・いいね数などの指標を毎時自動収集し、AWS S3 に Parquet 形式で蓄積するシステムです。

## 概要

Tableau Public API からワークブックのメトリクスを取得し、時系列で分析できるようにします。
GitHub Actions の60日制限・スケジュール遅延の問題を回避するため、AWS Lambda + EventBridge Scheduler で毎時確実に実行します。

## アーキテクチャ

```
EventBridge Scheduler（毎時0分）
  → Lambda 関数（コンテナイメージ）
    → Tableau Public API
      → S3（Parquet 形式）
        → Athena（SQL クエリ）
          → Tableau Cloud（可視化）
```

### 保存データの構造

| テーブル | 内容 | 保存パス |
|---|---|---|
| `metrics` | ビュー数・いいね数などの時系列スナップショット（毎時全件） | `s3://<bucket>/tableau_public/metrics/year=YYYY/month=MM/` |
| `attributes` | タイトル・説明文などの変更履歴（変更時のみ追記、SCD2） | `s3://<bucket>/tableau_public/attributes/` |

## ファイル構成

```
.
├── get_data.py          # Tableau Public API からデータ取得
├── s3_store.py          # S3 への Parquet 保存・ハッシュ管理
├── lambda_handler.py    # Lambda エントリポイント
├── migrate_to_s3.py     # 既存 CSV → S3 への一括移行スクリプト
├── Dockerfile           # Lambda コンテナイメージ定義
├── requirements.txt     # Python 依存パッケージ
├── athena_ddl.sql       # Athena テーブル定義（Partition Projection）
├── SETUP_S3.md          # S3 + IAM + Athena のセットアップ手順
└── SETUP_LAMBDA.md      # ECR + Lambda + EventBridge のセットアップ手順
```

## セットアップ手順

### 前提条件

- AWS アカウント
- AWS CLI（`brew install awscli`）
- Docker Desktop
- Python 3.11+

---

### ステップ 1：対象プロファイルの設定

`get_data.py` の `profile_name` を自分の Tableau Public プロファイル名に変更します。

```python
profile_name = 'your_tableau_public_username'
```

---

### ステップ 2：AWS リソースのセットアップ

詳細は **`SETUP_S3.md`** を参照してください。

1. S3 バケットを作成
2. IAM ポリシー `tableau-s3-write` を作成（S3 書き込み権限）
3. IAM ユーザー `tableau-athena-user` を作成してポリシーをアタッチ
4. Athena テーブルを `athena_ddl.sql` を使って作成

---

### ステップ 3：Lambda のセットアップ

詳細は **`SETUP_LAMBDA.md`** を参照してください。

1. ECR リポジトリ `tableau-data-extractor` を作成
2. Docker イメージをビルドして ECR にプッシュ
   ```bash
   aws ecr get-login-password --region ap-northeast-1 --profile ecr-deploy \
     | docker login --username AWS \
       --password-stdin <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com

   docker build --platform linux/arm64 --provenance=false -t tableau-data-extractor .
   docker tag tableau-data-extractor:latest \
     <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest
   docker push \
     <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest
   ```
3. Lambda 関数 `tableau-data-extractor` を作成（コンテナイメージ、arm64、512MB、タイムアウト5分）
4. Lambda に環境変数を設定
   | キー | 値 |
   |---|---|
   | `S3_BUCKET` | 作成した S3 バケット名 |
   | `S3_PREFIX` | `tableau_public` |
5. Lambda の実行ロールに `tableau-s3-write` ポリシーをアタッチ

---

### ステップ 4：EventBridge Scheduler の設定

1. EventBridge Scheduler で新しいスケジュールを作成
2. cron 式：`0 * * * ? *`（毎時0分 UTC）
3. ターゲット：Lambda `tableau-data-extractor`
4. 実行ロール：自動作成でOK

---

### ステップ 5：動作確認

Lambda コンソールのテストタブで実行し、以下のログが出れば成功です。

```
metrics: 373行をアップロードしました
attributes: 変更なし
```

---

## コードを更新したときの手順

```bash
# イメージを再ビルド・プッシュ
docker build --platform linux/arm64 --provenance=false -t tableau-data-extractor .
docker tag tableau-data-extractor:latest \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest
docker push \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest

# Lambda に反映
aws lambda update-function-code \
  --function-name tableau-data-extractor \
  --image-uri <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest \
  --region ap-northeast-1
```

または Lambda コンソールの「コード」タブ →「新しいイメージをデプロイ」から手動で反映できます。

---

## 参考

- [Tableau Public API の概要](https://github.com/wjsutton/tableau_public_api)
