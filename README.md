# Tableau Public Data Extract

Tableau Public に公開しているワークブックのビュー数・いいね数などの指標を毎時自動収集するシステムです。
AWS を使わない簡易モードと、S3 + Lambda を使った本格運用モードの2通りで動かせます。

---

## 動作モード

### モード A：GitHub Actions + CSV（簡易版）

AWS の設定が不要で、すぐに使い始められます。

```
GitHub Actions（定期実行）
  → Tableau Public API
    → data/ フォルダに CSV を保存
      → リポジトリにコミット
```

**制限事項**
- リポジトリに変更がない状態が60日続くとスケジュールが自動停止する
- GitHub のスケジュール実行は数時間遅延することがある
- データが CSV として蓄積されると容量が増え続ける（1年で数GB規模になることがある）

---

### モード B：AWS Lambda + S3 + Parquet（本格運用版）

EventBridge Scheduler で毎時確実に実行し、データを効率的に S3 へ蓄積します。

```
EventBridge Scheduler（毎時0分、確実に実行）
  → Lambda 関数（コンテナイメージ）
    → Tableau Public API
      → S3（Parquet 形式）
        → Athena（SQL クエリ）
          → Tableau Cloud（可視化）
```

**メリット**
- 60日制限なし、スケジュール遅延なし
- CSV → Parquet で約99%のストレージ削減
- Athena で SQL 分析、Tableau Cloud から直接接続可能
- 月額費用はほぼ0円（Lambda 無料枠内）

#### 保存データの構造

| テーブル | 内容 | 保存パス |
|---|---|---|
| `metrics` | ビュー数・いいね数などの時系列スナップショット（毎時全件） | `s3://<bucket>/tableau_public/metrics/year=YYYY/month=MM/` |
| `attributes` | タイトル・説明文などの変更履歴（変更時のみ追記、SCD2） | `s3://<bucket>/tableau_public/attributes/` |

---

## ファイル構成

```
.
├── get_data.py          # Tableau Public API からデータ取得（両モード共通）
├── s3_store.py          # S3 への Parquet 保存・ハッシュ管理（モードB）
├── lambda_handler.py    # Lambda エントリポイント（モードB）
├── migrate_to_s3.py     # 既存 CSV → S3 への一括移行スクリプト（モードB移行時）
├── Dockerfile           # Lambda コンテナイメージ定義（モードB）
├── requirements.txt     # Python 依存パッケージ
├── athena_ddl.sql       # Athena テーブル定義（Partition Projection）
├── SETUP_S3.md          # S3 + IAM + Athena のセットアップ手順
├── SETUP_LAMBDA.md      # ECR + Lambda + EventBridge のセットアップ手順
└── .github/workflows/
    ├── github-actions.yml   # 定期実行ワークフロー（モードA）
    └── migrate.yml          # S3 移行用ワークフロー（手動実行）
```

---

## モード A のセットアップ（GitHub Actions + CSV）

### 1. プロファイル名を変更

`get_data.py` の `profile_name` を自分の Tableau Public ユーザー名に変更します。

```python
profile_name = 'your_tableau_public_username'
```

### 2. GitHub Actions を有効化

リポジトリの「Actions」タブからワークフローを有効化するだけで、毎時データ収集が始まります。

収集したデータは `data/` フォルダに CSV 形式で保存され、リポジトリにコミットされます。

---

## モード B のセットアップ（AWS Lambda + S3）

### 前提条件

- AWS アカウント
- AWS CLI（`brew install awscli`）
- Docker Desktop（Mac の場合）
- Apple Silicon Mac（M1/M2/M3）または x86_64 の Linux/Windows

---

### ステップ 1：プロファイル名を変更

`get_data.py` の `profile_name` を自分の Tableau Public ユーザー名に変更します。

```python
profile_name = 'your_tableau_public_username'
```

---

### ステップ 2：S3・IAM・Athena のセットアップ

詳細は **`SETUP_S3.md`** を参照してください。概要は以下の通りです。

1. **S3 バケットを作成**（例：`tableau-data-yourname`）
2. **IAM ポリシー `tableau-s3-write` を作成**（S3 への読み書き権限）
3. **IAM ユーザー `tableau-athena-user` を作成**してポリシーをアタッチ（Athena クエリ用）
4. **Athena テーブルを作成**（`athena_ddl.sql` を Athena コンソールで実行）

---

### ステップ 3：ECR リポジトリの作成

1. ECR コンソールで「リポジトリを作成」
2. リポジトリ名：`tableau-data-extractor`
3. 可視性：プライベート、タグの変更可能性：ミュータブル、暗号化：AES-256

---

### ステップ 4：Docker イメージのビルドと ECR へのプッシュ

ECR 専用の IAM ユーザー（`ecr-deploy-user`）を作成し、`AmazonEC2ContainerRegistryPowerUser` ポリシーをアタッチしてアクセスキーを発行します。

```bash
# ECR へのログイン（アクセスキーは aws configure --profile ecr-deploy で設定済みの想定）
aws ecr get-login-password --region ap-northeast-1 --profile ecr-deploy \
  | docker login --username AWS \
    --password-stdin <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com

# イメージのビルド（Apple Silicon Mac の場合）
docker build --platform linux/arm64 --provenance=false -t tableau-data-extractor .

# タグ付けとプッシュ
docker tag tableau-data-extractor:latest \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest
docker push \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest
```

---

### ステップ 5：Lambda 関数の作成

詳細は **`SETUP_LAMBDA.md`** を参照してください。

1. Lambda コンソールで「関数の作成」→「コンテナイメージ」を選択
2. 設定：
   - 関数名：`tableau-data-extractor`
   - コンテナイメージ URI：ステップ4でプッシュしたイメージ
   - アーキテクチャ：`arm64`（Apple Silicon でビルドした場合）
3. 作成後の設定変更：
   - タイムアウト：**5分**
   - メモリ：**512MB**
4. 環境変数を追加：

   | キー | 値 |
   |---|---|
   | `S3_BUCKET` | 作成した S3 バケット名 |
   | `S3_PREFIX` | `tableau_public` |

5. 実行ロールに `tableau-s3-write` ポリシーをアタッチ

---

### ステップ 6：EventBridge Scheduler の設定

1. EventBridge Scheduler コンソールで「スケジュールを作成」
2. cron 式：`0 * * * ? *`（毎時0分 UTC）
3. フレキシブルな時間枠：**無効**（確実に0分に実行）
4. ターゲット：AWS Lambda → `tableau-data-extractor`
5. 実行ロール：「このスケジュールの新しいロールを作成」でOK

---

### ステップ 7：動作確認

Lambda コンソールの「テスト」タブで実行し、以下のログが出れば成功です。

```
metrics: XXX行をアップロードしました
attributes: 変更なし（または 変更X行をアップロードしました）
```

---

### ステップ 8：既存 CSV データの移行（任意）

モード A で蓄積した CSV データを S3 に移行する場合は、`migrate.yml` ワークフローを GitHub Actions から手動実行してください。

---

## コードを更新したときの手順

```bash
# イメージを再ビルド・プッシュ
docker build --platform linux/arm64 --provenance=false -t tableau-data-extractor .
docker tag tableau-data-extractor:latest \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest
docker push \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest

# Lambda に反映（CLI の場合）
aws lambda update-function-code \
  --function-name tableau-data-extractor \
  --image-uri <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest \
  --region ap-northeast-1
```

または Lambda コンソールの「コード」タブ →「新しいイメージをデプロイ」から手動で反映できます。

---

## 参考

- [Tableau Public API の概要](https://github.com/wjsutton/tableau_public_api)
