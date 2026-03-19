# db-csv の DuckDB 分析例

このファイルは、README の `db-csv` 例と同じ出力が `./output/db_csv` にある前提で書いています。

- `tables.csv`: 取り込んだ統計表の一覧
- `dimension_items.csv`: `time_code` / `area_code` / `cat01_code` などを人間向けラベルに戻す辞書
- `observations/*.csv`: 観測値の fact table

DuckDB ではまず全部を文字列として読み、数値計算するときだけ `TRY_CAST(value AS DOUBLE)` を使うのが安全です。e-Stat の API 由来で空文字や非数値が混ざっても、その行だけ `NULL` として扱えます。

## 1回だけ作るビュー

```shell
duckdb
```

```sql
CREATE OR REPLACE VIEW tables AS
SELECT *
FROM read_csv_auto('./output/db_csv/tables.csv', all_varchar = true);

CREATE OR REPLACE VIEW dimension_items AS
SELECT *
FROM read_csv_auto('./output/db_csv/dimension_items.csv', all_varchar = true);

CREATE OR REPLACE VIEW observations AS
SELECT *
FROM read_csv_auto(
  './output/db_csv/observations/*.csv',
  all_varchar = true,
  union_by_name = true
);
```

こちらのクエリーで読み込まれているデータを確認できます。

```sql
SELECT stats_data_id, table_name
FROM tables
ORDER BY stats_data_id;
```

## 例1: 人口推計から、2020-2024年の年齢3区分の構成比を見る

`0003448228` は README にある人口推計テーブルです。高齢化の進み方を見るには、単年齢を `0-14`, `15-64`, `65+` にまとめると扱いやすくなります。

```sql
WITH pop AS (
  SELECT
    regexp_extract(t.item_name, '([0-9]{4})年', 1) AS year,
    CASE
      WHEN a.item_code BETWEEN '01001' AND '01015' THEN '0-14'
      WHEN a.item_code BETWEEN '01016' AND '01065' THEN '15-64'
      ELSE '65+'
    END AS age_bucket,
    TRY_CAST(o.value AS DOUBLE) AS population_thousand
  FROM observations o
  JOIN dimension_items t
    ON t.stats_data_id = o.stats_data_id
   AND t.dimension_id = 'time'
   AND t.item_code = o.time_code
  JOIN dimension_items a
    ON a.stats_data_id = o.stats_data_id
   AND a.dimension_id = 'cat03'
   AND a.item_code = o.cat03_code
  WHERE o.stats_data_id = '0003448228'
    AND o.cat01_code = '001'  -- 男女計
    AND o.cat02_code = '001'  -- 総人口
    AND o.area_code = '00000' -- 全国
    AND o.cat03_code <> '01000'
)
SELECT
  year,
  age_bucket,
  ROUND(SUM(population_thousand), 0) AS population_thousand,
  ROUND(
    100.0 * SUM(population_thousand)
    / SUM(SUM(population_thousand)) OVER (PARTITION BY year),
    2
  ) AS share_pct
FROM pop
GROUP BY 1, 2
ORDER BY
  year,
  CASE age_bucket
    WHEN '0-14' THEN 1
    WHEN '15-64' THEN 2
    ELSE 3
  END;
```

この README 例の出力では、`65+` の構成比は 2020 年の `28.56%` から 2024 年の `29.27%` へ上がり、`0-14` は `11.92%` から `11.17%` へ下がります。

## 例2: 家計調査から、品目ごとのピーク四半期を探す

`0004023604` は家計調査の数量テーブルです。`cat02_code = '03'` は「二人以上の世帯」、`tab_code = '02'` は「数量」です。ここでは、季節性が見えやすい品目をいくつか選んで、平均数量が最も大きい四半期を出します。

品目コード:

- `010110001`: 米
- `010120010`: 食パン
- `010211060`: さけ
- `010511010`: キャベツ
- `010513040`: なす
- `010610020`: みかん
- `011100030`: ビール
- `070230010`: ガソリン

```sql
WITH seasonal AS (
  SELECT
    c.item_name,
    c.unit,
    CASE right(o.time_code, 4)
      WHEN '0103' THEN 'Q1 (1-3月)'
      WHEN '0406' THEN 'Q2 (4-6月)'
      WHEN '0709' THEN 'Q3 (7-9月)'
      WHEN '1012' THEN 'Q4 (10-12月)'
    END AS quarter,
    AVG(TRY_CAST(o.value AS DOUBLE)) AS avg_quantity
  FROM observations o
  JOIN dimension_items c
    ON c.stats_data_id = o.stats_data_id
   AND c.dimension_id = 'cat01'
   AND c.item_code = o.cat01_code
  WHERE o.stats_data_id = '0004023604'
    AND o.tab_code = '02'
    AND o.cat02_code = '03'
    AND o.area_code = '00000'
    AND c.item_code IN (
      '010110001',
      '010120010',
      '010211060',
      '010511010',
      '010513040',
      '010610020',
      '011100030',
      '070230010'
    )
  GROUP BY 1, 2, 3
),
ranked AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY item_name ORDER BY avg_quantity DESC) AS rn
  FROM seasonal
)
SELECT
  item_name,
  unit,
  quarter,
  ROUND(avg_quantity, 2) AS avg_quantity
FROM ranked
WHERE rn = 1
ORDER BY item_name;
```

この出力では、`みかん` と `米` は `Q4`、`ビール` と `ガソリン` は `Q3`、`なす` は `Q3` にピークが出ます。単位は品目ごとに違うので、数量そのものを品目間で比較するより、同じ品目の季節差を見る用途に向いています。

## 例3: 2024年の勤労者世帯と二人以上世帯を比べる

同じ `0004023604` には `cat02` 軸があり、`03 = 二人以上の世帯`, `04 = 二人以上の世帯のうち勤労者世帯` です。次のクエリでは、2024年の平均数量を比べます。

```sql
WITH base AS (
  SELECT
    c.item_name,
    o.cat02_code AS household_code,
    AVG(TRY_CAST(o.value AS DOUBLE)) AS avg_quantity_2024
  FROM observations o
  JOIN dimension_items c
    ON c.stats_data_id = o.stats_data_id
   AND c.dimension_id = 'cat01'
   AND c.item_code = o.cat01_code
  WHERE o.stats_data_id = '0004023604'
    AND o.tab_code = '02'
    AND o.area_code = '00000'
    AND left(o.time_code, 4) = '2024'
    AND c.item_code IN (
      '010110001',
      '010120010',
      '011100030',
      '070230010'
    )
  GROUP BY 1, 2
)
SELECT
  item_name,
  ROUND(MAX(CASE WHEN household_code = '03' THEN avg_quantity_2024 END), 2) AS all_two_plus,
  ROUND(MAX(CASE WHEN household_code = '04' THEN avg_quantity_2024 END), 2) AS worker_households,
  ROUND(
    MAX(CASE WHEN household_code = '04' THEN avg_quantity_2024 END)
    / MAX(CASE WHEN household_code = '03' THEN avg_quantity_2024 END),
    3
  ) AS worker_vs_all_ratio
FROM base
GROUP BY 1
ORDER BY worker_vs_all_ratio DESC;
```

この README 例の出力では、`ガソリン` の `worker_vs_all_ratio` は `1.248` で、勤労者世帯の方が約 25% 多くなります。こういう比較は、`cat02` のような補助軸がある DB 系テーブルではかなり有効です。
