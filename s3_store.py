"""S3 + Parquet 出力の共通ロジック。

get_data.py（毎時実行）と migrate_to_s3.py（過去データ一括移行）の両方から使う。

データは2系統に分けて保存する:
- metrics:    毎時変動する指標（viewCount等）の時系列。毎回追記。
- attributes: ワークブックの属性（タイトル・説明文等）。前回からの
              変更があった行だけを追記する（SCD2方式）。

環境変数:
- S3_BUCKET:  出力先バケット名。未設定の場合はローカルの _local_s3/ に
              書き出す（動作確認用）。
- S3_PREFIX:  キーのプレフィックス（デフォルト: tableau_public）
- AWS_REGION: リージョン（デフォルト: ap-northeast-1）
"""

import hashlib
import io
import os
import re
from pathlib import Path

import pandas as pd

S3_BUCKET = os.environ.get('S3_BUCKET')
S3_PREFIX = os.environ.get('S3_PREFIX', 'tableau_public')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-northeast-1')
LOCAL_OUTPUT_DIR = '_local_s3'  # S3_BUCKET未設定時の書き出し先

KEY_COLUMN = 'workbookRepoUrl'

# 毎時変動する指標列（metricsテーブルに入れる）
METRIC_COLUMNS = ['viewCount', 'numberOfFavorites', 'size', 'revision']

# 属性の変更検知ハッシュから除外する列（指標・日時系）
# 注意: APIに新しい列が追加されるとハッシュが変わり、その回だけ全行が
# 「変更あり」として記録される（実害はないが行数が一時的に増える）
HASH_EXCLUDED = METRIC_COLUMNS + ['lastPublishDate', 'lastUpdateDate', 'getDate']


def to_snake(name):
    """camelCase → snake_case（Athenaの列名規約に合わせる）"""
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


def make_client():
    """S3クライアントを返す。S3_BUCKET未設定ならNone（ローカル書き出しモード）"""
    if not S3_BUCKET:
        return None
    import boto3
    return boto3.client('s3', region_name=AWS_REGION)


def upload_parquet(s3, df, key):
    """DataFrameをParquetにしてS3（またはローカル）に書き出す"""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    if s3 is None:
        path = Path(LOCAL_OUTPUT_DIR) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(buf.getvalue())
    else:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())


def metrics_key(snapshot_ts, name=None):
    name = name or f'{snapshot_ts:%Y%m%d_%H%M%S}'
    return (f'{S3_PREFIX}/metrics/year={snapshot_ts:%Y}/month={snapshot_ts:%m}/'
            f'{name}.parquet')


def attributes_key(snapshot_ts, name=None):
    name = name or f'{snapshot_ts:%Y%m%d_%H%M%S}'
    return f'{S3_PREFIX}/attributes/{name}.parquet'


def state_key():
    return f'{S3_PREFIX}/state/latest_attribute_hashes.parquet'


def build_metrics(df, snapshot_ts):
    """スナップショットから指標の時系列レコードを作る"""
    src = df.reindex(columns=[KEY_COLUMN, 'luid'] + METRIC_COLUMNS)
    return pd.DataFrame({
        'snapshot_ts': pd.Timestamp(snapshot_ts),
        'workbook_repo_url': src[KEY_COLUMN].astype(str),
        'luid': src['luid'].astype(str),
        'view_count': pd.to_numeric(src['viewCount'], errors='coerce').astype('Int64'),
        'number_of_favorites': pd.to_numeric(src['numberOfFavorites'], errors='coerce').astype('Int64'),
        'size': pd.to_numeric(src['size'], errors='coerce').astype('Int64'),
        'revision': src['revision'].astype(str),
    })


def attribute_columns(df):
    """ハッシュ・属性テーブルの対象列（順序を固定するためソート）"""
    return sorted(c for c in df.columns
                  if c not in HASH_EXCLUDED and not c.startswith('Unnamed'))


def _stringify(df, cols):
    """欠損値を空文字に統一して全列を文字列化する
    （API由来のNoneとCSV由来のNaNを同じ表現にしてハッシュを安定させる）"""
    return df[cols].astype('string').fillna('')


def compute_row_hashes(df):
    """属性列の内容から行ごとのSHA-256ハッシュを計算する"""
    joined = _stringify(df, attribute_columns(df)).agg('|'.join, axis=1)
    return joined.map(lambda s: hashlib.sha256(s.encode('utf-8')).hexdigest())


def build_attributes(df, snapshot_ts, row_hashes):
    """変更のあった行の属性レコードを作る（全列を文字列として保存）"""
    cols = attribute_columns(df)
    attrs = _stringify(df, cols)
    attrs.columns = [to_snake(c) for c in cols]
    attrs.insert(0, 'effective_from', pd.Timestamp(snapshot_ts))
    attrs['row_hash'] = row_hashes.values
    return attrs


def detect_changes(df, row_hashes, prev_hashes):
    """前回のハッシュと比較して変更（新規含む）行のマスクを返す"""
    urls = df[KEY_COLUMN].astype(str)
    return pd.Series(
        [prev_hashes.get(u) != h for u, h in zip(urls, row_hashes)],
        index=df.index,
    )


def load_previous_hashes(s3):
    """前回実行時の属性ハッシュ（workbookRepoUrl → hash）を読み込む"""
    if s3 is None:
        path = Path(LOCAL_OUTPUT_DIR) / state_key()
        if not path.exists():
            return {}
        df = pd.read_parquet(path)
    else:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=state_key())
        except s3.exceptions.NoSuchKey:
            return {}
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    return dict(zip(df['workbook_repo_url'], df['row_hash']))


def save_hashes(s3, hash_map):
    """最新の属性ハッシュを保存する（次回の変更検知に使う）"""
    df = pd.DataFrame({
        'workbook_repo_url': list(hash_map.keys()),
        'row_hash': list(hash_map.values()),
    })
    upload_parquet(s3, df, state_key())
