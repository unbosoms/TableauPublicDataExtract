# Lambda + EventBridge Scheduler セットアップ手順

GitHub Actionsの代わりにAWS Lambdaで毎時データ取得を行う構成。
スケジュールの遅延・60日制限・実行保証の問題がすべて解消される。

## 構成

```
EventBridge Scheduler（毎時0分、確実に実行）
  → Lambda関数（コンテナイメージ）
    → Tableau Public API
      → S3（metrics / attributes Parquet）
```

## 費用

| 項目 | 月額 |
|---|---|
| Lambda（720回 × 約60秒 × 128MB） | 無料枠内（0円） |
| EventBridge Scheduler | 0.1円未満 |
| ECR（コンテナイメージ保存） | 無料枠500MBまで0円 |
| **合計** | **ほぼ0円** |

---

## ステップ1：ECRリポジトリを作る

1. [ECRコンソール](https://ap-northeast-1.console.aws.amazon.com/ecr/repositories) を開く
2. 「**リポジトリを作成**」
   - 可視性: プライベート
   - リポジトリ名: `tableau-data-extractor`
3. 作成後、リポジトリのURIをコピーしておく
   - 形式: `665556285084.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor`

---

## ステップ2：Dockerイメージをビルドして ECR にプッシュ

ターミナルで以下を実行（`<ACCOUNT_ID>` は自分のAWSアカウントID に置き換え）。

```bash
# AWSにDockerログイン
aws ecr get-login-password --region ap-northeast-1 \
  | docker login --username AWS \
    --password-stdin <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com

# イメージをビルド（M1/M2 MacはARM64ネイティブビルドで高速）
cd /path/to/TableauPublicDataExtract
docker build --platform linux/arm64 -t tableau-data-extractor .

# タグ付けしてプッシュ
docker tag tableau-data-extractor:latest \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest
docker push \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest
```

---

## ステップ3：Lambda用IAMロールを作る

1. [IAMコンソール](https://console.aws.amazon.com/iam/) → ロール → 「ロールを作成」
2. **信頼されたエンティティ**: `AWS のサービス` → `Lambda`
3. **許可ポリシーをアタッチ**:
   - `AWSLambdaBasicExecutionRole`（CloudWatch Logsへの書き込み）
   - `tableau-s3-write`（ステップ3で作成済みのポリシー）
4. **ロール名**: `lambda-tableau-data`
5. 作成後、ロールのARNをコピーしておく

---

## ステップ4：Lambda関数を作る

1. [Lambdaコンソール](https://ap-northeast-1.console.aws.amazon.com/lambda/) → 「関数の作成」
2. **コンテナイメージ**を選択
3. 設定:
   - **関数名**: `tableau-data-extractor`
   - **コンテナイメージURI**: ステップ1のECR URI + `:latest`
     （「イメージを参照」ボタンから選べる）
   - **アーキテクチャ**: `arm64`（ステップ2でarm64でビルドした場合）
4. 「関数の作成」後、設定タブで以下を変更:
   - **タイムアウト**: 5分（デフォルト3秒から変更）
   - **メモリ**: 512MB
   - **実行ロール**: `lambda-tableau-data`
5. **環境変数**を追加（設定 → 環境変数）:
   | キー | 値 |
   |---|---|
   | `S3_BUCKET` | `tableau-data-yuta1985` |
   | `S3_PREFIX` | `tableau_public` |
   | `AWS_DEFAULT_REGION` | `ap-northeast-1` |

---

## ステップ5：テスト実行

1. Lambdaコンソール → 「テスト」タブ
2. イベントJSON はデフォルトの `{}` のままでOK
3. 「テスト」をクリック
4. ログに `metrics: 370行をアップロードしました` が出れば成功

---

## ステップ6：EventBridge Schedulerで毎時実行

1. [EventBridge Scheduler コンソール](https://ap-northeast-1.console.aws.amazon.com/scheduler/home) を開く
2. 「スケジュールを作成」
3. **スケジュールパターン**:
   - 定期的なスケジュール → cron式
   - `0 * * * ? *`（毎時0分、UTC）
   - フレキシブルな時間枠: 無効（確実に0分に実行したい場合）
4. **ターゲット**:
   - AWS Lambda → `tableau-data-extractor`
5. 「スケジュールを作成」

---

## ステップ7：GitHub Actionsのスケジュールを無効化

Lambdaが動き始めたらGitHub Actionsのcronは不要。
`.github/workflows/github-actions.yml` の `schedule:` 行を削除するか、
Actionsタブからワークフローを「Disable workflow」してください。

（`workflow_dispatch` は手動テスト用に残しておくと便利です）

---

## イメージ更新手順（コード変更時）

```bash
docker build --platform linux/arm64 -t tableau-data-extractor .
docker tag tableau-data-extractor:latest \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest
docker push \
  <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest

# Lambdaに最新イメージを反映
aws lambda update-function-code \
  --function-name tableau-data-extractor \
  --image-uri <ACCOUNT_ID>.dkr.ecr.ap-northeast-1.amazonaws.com/tableau-data-extractor:latest \
  --region ap-northeast-1
```
