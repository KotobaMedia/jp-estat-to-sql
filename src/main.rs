use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod areamap;
mod download;
mod gdal;
mod mesh;
mod mesh_csv;
mod unzip;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Postgresデータベースに接続する文字列。 ogr2ogr に渡されます。冒頭の `PG:` は省略してください。
    /// `mesh-csv` サブコマンドでは不要です。
    postgres_url: Option<String>,

    /// 中間ファイルの保存先 (Zip等)
    /// デフォルトは `./tmp` となります。
    #[arg(long)]
    tmp_dir: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 小地域（丁目・字等）の取り込み
    Areamap,

    /// メッシュデータの取り込み
    Mesh {
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

    /// メッシュデータのCSVをダウンロードして1ファイルに結合
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let tmp_dir = cli.tmp_dir.unwrap_or_else(|| PathBuf::from("./tmp"));
    tokio::fs::create_dir_all(&tmp_dir).await?;
    match &cli.command {
        Commands::Areamap => {
            let postgres_url = cli
                .postgres_url
                .as_deref()
                .ok_or(anyhow!("areamap サブコマンドでは POSTGRES_URL が必要です"))?;
            areamap::process_areamap(postgres_url, &tmp_dir).await?;
        }
        Commands::Mesh {
            level,
            year,
            survey,
        } => {
            let postgres_url = cli
                .postgres_url
                .as_deref()
                .ok_or(anyhow!("mesh サブコマンドでは POSTGRES_URL が必要です"))?;
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
    }

    Ok(())
}
