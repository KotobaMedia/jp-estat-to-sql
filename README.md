# jp-estat-to-sql

e-Stat でホスティングしている統計情報や地理情報をSQLデータベースに取り込むツール

GISデータについては、[e-Statのデータ注意情報](https://www.e-stat.go.jp/help/data-definition-information/download) を留意ください。

## 概要

このツールは、e-Stat（政府統計の総合窓口）から統計データと地理情報をダウンロードし、PostgreSQLデータベースに取り込むためのコマンドラインツールです。

## インストール

```shell
cargo install --path .
```

## 使用方法

### 基本構文

```shell
jp-estat-to-sql [OPTIONS] [POSTGRES_URL] <COMMAND>
```

### オプション

- `--tmp-dir <PATH>`: 中間ファイルの保存先（デフォルト: `./tmp`）
- `--help`: ヘルプを表示
- `--version`: バージョンを表示

### データベース接続

`POSTGRES_URL` は PostgreSQL への接続文字列です。`ogr2ogr` に渡される形式で、冒頭の `PG:` は省略してください。

`areamap` / `mesh` サブコマンドでは必須、`mesh-csv` サブコマンドでは不要です。

例:
```shell
"host=127.0.0.1 dbname=jp-estat user=postgres password=mypassword"
```

## サブコマンド

### areamap - 小地域（丁目・字等）の取り込み

国勢調査の小地域境界データをダウンロードし、PostGISに取り込みます。

#### 概要

- **データソース**: [e-Stat 小地域境界データ](https://www.e-stat.go.jp/gis/statmap-search?page=1&type=2&aggregateUnitForBoundary=A&toukeiCode=00200521)
- **対象年度**: 2000, 2005, 2010, 2015, 2020年
- **座標系**: JGD2011（優先）、JGD2000（フォールバック）
- **データ形式**: 緯度経度データ（平面直角座標系は非対応）

#### 使用方法

```shell
jp-estat-to-sql "host=127.0.0.1 dbname=jp-estat" areamap
```

#### 処理内容

1. **データダウンロード**: 47都道府県 × 5年度 = 235ファイルを並行ダウンロード
2. **ファイル展開**: ZIPファイルからShapefileを抽出
3. **PostGIS取り込み**: VRTファイルを作成してPostGISに一括取り込み
4. **データ後処理**:
   - 水面調査区（hcode=8154）の削除
   - メタデータの登録
   - 座標系の設定（JGD2011: SRID 6668, JGD2000: SRID 4621）

#### 作成されるテーブル

- `jp_estat_areamap_2000` - 2000年国勢調査小地域境界データ
- `jp_estat_areamap_2005` - 2005年国勢調査小地域境界データ
- `jp_estat_areamap_2010` - 2010年国勢調査小地域境界データ
- `jp_estat_areamap_2015` - 2015年国勢調査小地域境界データ
- `jp_estat_areamap_2020` - 2020年国勢調査小地域境界データ

#### テーブル構造

| カラム名 | データ型 | 説明 |
|---------|---------|------|
| `ogc_fid` | integer | 主キー |
| `geom` | geometry(polygon, SRID) | 境界ポリゴン |
| `key_code` | varchar(255) | 小地域コード |
| `pref_name` | varchar(255) | 都道府県名 |
| `city_name` | varchar(255) | 市区町村名 |
| `s_name` | varchar(255) | 小地域名 |
| `jinko` | int | 人口 |
| `setai` | int | 世帯数 |

#### 注意事項

- 処理時間はインターネット接続、メモリ、SSD転送速度に依存
- 途中からの再開機能あり（`--help` で詳細確認）
- ダウンロードしたZIPファイルとShapefileは `./tmp` に保存

---

### mesh - メッシュデータの取り込み

国勢調査のメッシュ統計データをダウンロードし、PostgreSQLに取り込みます。

#### 概要

- **データソース**: e-Stat メッシュ統計データ
- **メッシュレベル**: 3次メッシュ（約1km）、4次メッシュ（約500m）、5次メッシュ（約250m）
- **対象年度**: 2020年（現在対応）
- **データ形式**: CSV（Shift_JISエンコーディング）

#### 使用方法

```shell
jp-estat-to-sql "host=127.0.0.1 dbname=jp-estat" mesh --level 3 --year 2020 --survey "人口及び世帯"
```

#### パラメータ

- `--level <LEVEL>`: メッシュレベル（3, 4, または 5）
- `--year <YEAR>`: 調査年度（例: 2020）
- `--survey <SURVEY>`: 調査名

#### 利用可能なデータ

**2020年データ**:

| レベル | 調査名 | 統計ID | 説明 |
|-------|-------|--------|------|
| 3 | 人口及び世帯 | T001140 | 約1kmメッシュの人口・世帯データ |
| 3 | 人口移動、就業状態等及び従業地・通学地 | T001143 | 約1kmメッシュの移動・就業データ |
| 4 | 人口及び世帯 | T001141 | 約500mメッシュの人口・世帯データ |
| 4 | 人口移動、就業状態等及び従業地・通学地 | T001144 | 約500mメッシュの移動・就業データ |
| 5 | 人口及び世帯 | T001142 | 約250mメッシュの人口・世帯データ |
| 5 | 人口移動、就業状態等及び従業地・通学地 | T001145 | 約250mメッシュの移動・就業データ |

#### 処理内容

1. **データ検証**: 指定されたレベル・年度・調査名の組み合わせが利用可能かチェック
2. **データダウンロード**: 全国のメッシュコード（約8,000個）に対応するCSVファイルを並行ダウンロード
3. **スキーマ作成**: CSVヘッダーを解析してテーブルスキーマを自動生成
4. **データ取り込み**: 全CSVファイルをPostgreSQLに取り込み

#### 作成されるテーブル

テーブル名形式: `jp_estat_mesh_{YEAR}_{STATS_ID}_{LEVEL}`

例:
- `jp_estat_mesh_2020_T001140_3` - 2020年3次メッシュ人口・世帯データ

#### データ型の自動判定

| カラム名 | データ型 | 説明 |
|---------|---------|------|
| `KEY_CODE` | BIGINT | メッシュコード |
| `HTKSAKI` | BIGINT | 集計値 |
| `GASSAN` | BIGINT[] | 合算値（配列） |
| `HTKSYORI` | SMALLINT | 処理区分 |
| その他 | INTEGER | 統計値 |

#### メッシュコードについて

`KEY_CODE` フィールドには JIS X 0410 地域メッシュコードが格納されています。このメッシュコードを地理的なジオメトリに変換するには、[jismesh-plpgsql](https://github.com/kotobaMedia/jismesh-plpgsql) ライブラリを使用できます。

このライブラリは、メッシュコードから動的にジオメトリを生成する PL/pgSQL 関数を提供しており、以下のような使い方ができます：

```sql
-- メッシュデータとジオメトリを結合
SELECT
    m."KEY_CODE",
    m."人口（総数）",
    mesh.geom
FROM jp_estat_mesh_2020_T001140_3 m
JOIN jismesh.to_meshcodes(
    ST_SetSRID(ST_MakeBox2D(
        ST_MakePoint(139.7, 35.7),
        ST_MakePoint(135.7, 34.7)
    ), 4326),
    'Lv3'::jismesh.mesh_level
) mesh ON m."KEY_CODE" = mesh.meshcode;
```

#### 使用例

```shell
# 3次メッシュの人口・世帯データを取得
jp-estat-to-sql "host=127.0.0.1 dbname=jp-estat" mesh \
  --level 3 \
  --year 2020 \
  --survey "人口及び世帯"

# 4次メッシュの移動・就業データを取得
jp-estat-to-sql "host=127.0.0.1 dbname=jp-estat" mesh \
  --level 4 \
  --year 2020 \
  --survey "人口移動、就業状態等及び従業地・通学地"
```

#### 注意事項

- メッシュレベルが大きいほどデータ量が増加（5次メッシュは約8,000ファイル）
- CSVファイルはShift_JISエンコーディングで処理
- 空値や `*` は `NULL` として扱われる
- `GASSAN` カラムはセミコロン区切りの配列として保存

---

### mesh-csv - メッシュデータのCSV結合出力

メッシュ統計CSVをダウンロードして、1つのCSVに結合して出力します。データベースへの取り込みは行いません。

#### 使用方法

```shell
jp-estat-to-sql mesh-csv \
  --level 3 \
  --year 2020 \
  --survey "人口及び世帯" \
  --output ./output/mesh_2020_lv3.csv
```

#### パラメータ

- `--level <LEVEL>`: メッシュレベル（3, 4, または 5）
- `--year <YEAR>`: 調査年度（例: 2020）
- `--survey <SURVEY>`: 調査名
- `--output <OUTPUT>`: 結合CSVの出力先パス

## トラブルシューティング

### よくある問題

1. **データベース接続エラー**
   - PostgreSQLが起動していることを確認
   - 接続文字列の形式を確認

2. **ダウンロードエラー**
   - インターネット接続を確認
   - e-Statのサーバー状況を確認

3. **メモリ不足**
   - `--tmp-dir` でSSD上のディレクトリを指定
   - 並行処理数を調整（コード修正が必要）

### ログとデバッグ

- 進捗バーで処理状況を確認
- `./tmp` ディレクトリでダウンロード状況を確認
- エラーメッセージで具体的な問題を特定

## ライセンス

このツールはMITライセンスの下で公開されています。

e-Statのデータ利用については、[e-Stat利用規約](https://www.e-stat.go.jp/terms-of-use)に従ってください。
