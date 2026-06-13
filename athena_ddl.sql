-- Athenaで1回だけ実行するDDL。
-- <YOUR_BUCKET> を実際のS3バケット名に置き換えてから実行してください。
-- （S3_PREFIXを変更している場合は tableau_public の部分も合わせて変更）

CREATE DATABASE IF NOT EXISTS tableau_public;

-- 指標の時系列（毎時1スナップショット分が追記される）
CREATE EXTERNAL TABLE tableau_public.metrics (
  snapshot_ts          timestamp,
  workbook_repo_url    string,
  luid                 string,
  view_count           bigint,
  number_of_favorites  bigint,
  size                 bigint,
  revision             string
)
PARTITIONED BY (year string, month string)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/tableau_public/metrics/'
TBLPROPERTIES (
  -- Partition Projection: パーティション追加クエリ(MSCK等)が不要になる
  'projection.enabled' = 'true',
  'projection.year.type' = 'integer',
  'projection.year.range' = '2025,2035',
  'projection.month.type' = 'integer',
  'projection.month.range' = '1,12',
  'projection.month.digits' = '2',
  'storage.location.template' =
    's3://<YOUR_BUCKET>/tableau_public/metrics/year=${year}/month=${month}'
);

-- ワークブック属性の変更履歴（変更があった行だけが追記される / SCD2）
-- 各workbookの時点tでの属性は effective_from <= t の最新行
CREATE EXTERNAL TABLE tableau_public.attributes (
  effective_from         timestamp,
  allow_data_access      string,
  attributions           string,
  author_display_name    string,
  author_profile_name    string,
  created_at             string,
  default_view_luid      string,
  default_view_name      string,
  default_view_repo_url  string,
  description            string,
  external_link          string,
  extract_info           string,
  first_publish_date     string,
  id                     string,
  luid                   string,
  owner_id               string,
  permalink              string,
  show_byline            string,
  show_in_profile        string,
  show_share_options     string,
  show_tabs              string,
  show_toolbar           string,
  show_watermark         string,
  title                  string,
  warn_data_access       string,
  workbook_repo_url      string,
  row_hash               string
)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/tableau_public/attributes/';

-- 動作確認クエリの例:
--
-- 最新スナップショットのviewCount上位10件
--   SELECT workbook_repo_url, view_count
--   FROM tableau_public.metrics
--   WHERE snapshot_ts = (SELECT max(snapshot_ts) FROM tableau_public.metrics)
--   ORDER BY view_count DESC LIMIT 10;
--
-- 各ワークブックの最新属性（Tableauのデータソースに使う形）
--   SELECT *
--   FROM (
--     SELECT *, row_number() OVER (
--       PARTITION BY workbook_repo_url ORDER BY effective_from DESC) AS rn
--     FROM tableau_public.attributes
--   ) WHERE rn = 1;
