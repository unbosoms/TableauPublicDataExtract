"""過去のCSVスナップショット（data/*.csv）をS3のParquet形式に一括移行する。

1回だけ実行するスクリプト。実行後は get_data.py のS3モードが
このデータに追記していく形になる。

- metrics:    全スナップショットの指標を月ごとのParquetにまとめて出力
- attributes: 時系列順にハッシュ比較し、変更があった行だけ出力（SCD2）
- state:      最後のスナップショットのハッシュを保存（毎時実行が引き継ぐ）

使い方:
    S3_BUCKET=<バケット名> python migrate_to_s3.py
    （S3_BUCKET未設定なら _local_s3/ に書き出すドライラン）
"""

import datetime
import glob
import os
import re
import sys

import pandas as pd

import s3_store


def parse_snapshot_ts(path):
    """ファイル名（YYYYMMDD_HHMMSS_data.csv）からスナップショット時刻を得る"""
    m = re.search(r'(\d{8})_(\d{6})_data\.csv$', path)
    if not m:
        return None
    return datetime.datetime.strptime(m.group(1) + m.group(2), '%Y%m%d%H%M%S')


def main():
    csv_files = sorted(glob.glob('./data/*_data.csv'))
    if not csv_files:
        print('data/ にCSVファイルが見つかりません')
        sys.exit(1)

    print(f'{len(csv_files)}個のCSVを移行します')
    s3 = s3_store.make_client()
    if s3 is None:
        print(f'S3_BUCKET未設定のため {s3_store.LOCAL_OUTPUT_DIR}/ に書き出します（ドライラン）')

    monthly_metrics = {}   # (year, month) -> [DataFrame, ...]
    attribute_frames = []
    prev_hashes = {}

    for i, path in enumerate(csv_files):
        snapshot_ts = parse_snapshot_ts(path)
        if snapshot_ts is None:
            print(f'スキップ（ファイル名が想定外）: {path}')
            continue

        df = pd.read_csv(path, index_col=0, low_memory=False)

        monthly_metrics.setdefault(
            (snapshot_ts.year, snapshot_ts.month), []
        ).append(s3_store.build_metrics(df, snapshot_ts))

        row_hashes = s3_store.compute_row_hashes(df)
        changed = s3_store.detect_changes(df, row_hashes, prev_hashes)
        if changed.any():
            attribute_frames.append(
                s3_store.build_attributes(df[changed], snapshot_ts, row_hashes[changed])
            )

        urls = df[s3_store.KEY_COLUMN].astype(str)
        prev_hashes.update(zip(urls, row_hashes))

        if (i + 1) % 500 == 0:
            print(f'  {i + 1}/{len(csv_files)} 処理済み')

    # metricsを月ごとにまとめてアップロード
    total_metric_rows = 0
    for (year, month), frames in sorted(monthly_metrics.items()):
        merged = pd.concat(frames, ignore_index=True)
        total_metric_rows += len(merged)
        ts = datetime.datetime(year, month, 1)
        key = s3_store.metrics_key(ts, name=f'history_{year}{month:02d}')
        s3_store.upload_parquet(s3, merged, key)
        print(f'metrics {year}-{month:02d}: {len(merged)}行 → {key}')

    # attributesをまとめてアップロード
    if attribute_frames:
        merged_attrs = pd.concat(attribute_frames, ignore_index=True)
        first_ts = parse_snapshot_ts(csv_files[0])
        key = s3_store.attributes_key(first_ts, name='history')
        s3_store.upload_parquet(s3, merged_attrs, key)
        print(f'attributes: 変更履歴{len(merged_attrs)}行 → {key}')

    # 毎時実行が引き継ぐ変更検知ステートを保存
    s3_store.save_hashes(s3, prev_hashes)

    print(f'完了: metrics {total_metric_rows}行、'
          f'attributes {sum(len(f) for f in attribute_frames)}行を移行しました')


if __name__ == '__main__':
    main()
