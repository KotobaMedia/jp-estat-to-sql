use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod areamap;
mod download;
mod gdal;
mod mesh;
mod mesh_csv;
mod mesh_info;
mod mesh_tile;
mod unzip;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// 中間ファイルの保存先 (Zip等)
    /// デフォルトは `./tmp` となります。
    #[arg(long)]
    tmp_dir: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 小地域（丁目・字等）の取り込み
    Areamap {
        /// ogr2ogr に渡す出力先データソース
        /// 例: "PG:host=127.0.0.1 dbname=jp_estat", "./output/areamap.gpkg"
        #[arg(long)]
        output: String,

        /// ogr2ogr の出力フォーマット名 (省略時は ogr2ogr の既定/推測に従います)
        /// 例: PostgreSQL, GPKG, GeoJSON
        #[arg(long)]
        output_format: Option<String>,

        /// 出力座標参照系 (ogr2ogr -t_srs に渡します)
        /// 例: EPSG:4326, EPSG:3857
        #[arg(long)]
        output_crs: Option<String>,

        /// 対象年度で絞り込み (単年のみ。例: --year 2020)
        #[arg(long)]
        year: Option<u32>,
    },

    /// `mesh-csv` と同等の入力でメッシュデータを取り込み（出力先: PostgreSQL）
    Mesh {
        /// PostgreSQLデータベースに接続する文字列
        #[arg(long)]
        postgres_url: String,

        /// メッシュレベル (3, 4, 5, or 6)
        #[arg(long, value_parser = clap::value_parser!(u8).range(3..=6))]
        level: u8,

        /// 年度 (例: 2020)
        #[arg(long)]
        year: u16,

        /// 調査名
        #[arg(long)]
        survey: String,
    },

    /// `mesh` と同等の入力でメッシュデータを取得（出力先: 結合CSV）
    MeshCsv {
        /// メッシュレベル (3, 4, 5, or 6)
        #[arg(long, value_parser = clap::value_parser!(u8).range(3..=6))]
        level: u8,

        /// 年度 (例: 2020)
        #[arg(long)]
        year: u16,

        /// 調査名
        #[arg(long)]
        survey: String,

        /// 出力先CSVファイル
        #[arg(long)]
        output: PathBuf,
    },

    /// メッシュデータを mesh-data-tile 形式で出力
    MeshTile {
        /// メッシュレベル (3, 4, 5, or 6)
        #[arg(long, value_parser = clap::value_parser!(u8).range(3..=6))]
        level: u8,

        /// 年度 (例: 2020)
        #[arg(long)]
        year: u16,

        /// 調査名
        #[arg(long)]
        survey: String,

        /// 出力タイルのメッシュレベル (1..=6)
        /// 省略時は入力データと同じレベルを使います。
        #[arg(long, value_parser = clap::value_parser!(u8).range(1..=6))]
        tile_level: Option<u8>,

        /// 出力する統計項目名の順序 (カンマ区切り)
        /// 例: 人口（総数）,人口（総数）男,人口（総数）女
        /// 省略時は全バンドを元CSV順で出力します。
        #[arg(long, value_delimiter = ',')]
        bands: Option<Vec<String>>,

        /// 出力先ディレクトリ
        #[arg(long)]
        output_dir: PathBuf,
    },

    /// メッシュ統計の利用可能データ一覧を表示
    MeshInfo {
        /// 対象年度で絞り込み (カンマ区切り可。例: --year 2015,2020)
        #[arg(long, value_delimiter = ',')]
        year: Option<Vec<u16>>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let tmp_dir = cli.tmp_dir.unwrap_or_else(|| PathBuf::from("./tmp"));
    tokio::fs::create_dir_all(&tmp_dir).await?;
    match &cli.command {
        Commands::Areamap {
            output,
            output_format,
            output_crs,
            year,
        } => {
            areamap::process_areamap(
                output,
                output_format.as_deref(),
                output_crs.as_deref(),
                &tmp_dir,
                *year,
            )
            .await?;
        }
        Commands::Mesh {
            postgres_url,
            level,
            year,
            survey,
        } => {
            mesh::process_mesh(postgres_url, &tmp_dir, *level, *year, survey).await?;
        }
        Commands::MeshCsv {
            level,
            year,
            survey,
            output,
        } => {
            mesh_csv::process_mesh_csv(&tmp_dir, *level, *year, survey, output).await?;
        }
        Commands::MeshTile {
            level,
            year,
            survey,
            tile_level,
            bands,
            output_dir,
        } => {
            mesh_tile::process_mesh_tile(
                &tmp_dir,
                *level,
                *year,
                survey,
                *tile_level,
                bands.as_deref(),
                output_dir,
            )
            .await?;
        }
        Commands::MeshInfo { year } => {
            mesh_info::process_mesh_info(&tmp_dir, year.as_deref()).await?;
        }
    }

    Ok(())
}
