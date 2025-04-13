# jp-estat-to-sql

e-Stat でホスティングしている統計情報や地理情報をSQLデータベースに取り込むツール

GISデータについては、[e-Statのデータ注意情報](https://www.e-stat.go.jp/help/data-definition-information/download) を留意ください。

## 小地域（丁目・字等）の取り込み

https://www.e-stat.go.jp/gis/statmap-search?page=1&type=2&aggregateUnitForBoundary=A&toukeiCode=00200521

公開している調査年のデータをすべて読み込みます。JGD2011のデータがあれば優先されます。なければJGD2000を利用します。
「世界測地系緯度経度」と「平面直角座標系」のデータが両方ありますが、このツールは「緯度経度」データのみ対応しています。

```shell
$ jp-estat-to-sql "host=127.0.0.1 dbname=jp-estat" areamap
```

インターネット接続、メモリ、SSD転送速度等によって処理時間が大幅に左右します。途中からの続きを再開するために幾つかのオプションがあるので、 `jp-estat-to-sql --help` で確認してください。

ダウンロードした ZIP ファイルや解凍した shapefile をデフォルトで実行ディレクトリ内 `./tmp` に保存されます。

取り込み後、 `jp_estat_areamap_YYYY` (2000, 2005, 2010, 2015, 2020) のテーブルが作成され、全国のデータがインポートされます。
