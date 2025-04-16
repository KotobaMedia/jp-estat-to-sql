use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod areamap;
mod gdal;
mod mesh;
mod unzip;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Postgresデータベースに接続する文字列。 ogr2ogr に渡されます。冒頭の `PG:` は省略してください。
    postgres_url: String,

    /// 中間ファイルの保存先 (Zip等)
    /// デフォルトは `./tmp` となります。
    #[arg(long)]
    tmp_dir: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 小地域（丁目・字等）の取り込み
    Areamap,

    /// メッシュデータの取り組み
    Mesh {
        /// メッシュレベル (3, 4, or 5)
        #[arg(long, value_parser = clap::value_parser!(u8).range(3..=5))]
        level: u8,

        /// 年度 (例: 2020)
        #[arg(long)]
        year: u16,

        /// 調査名
        #[arg(long)]
        survey: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let tmp_dir = cli.tmp_dir.unwrap_or_else(|| PathBuf::from("./tmp"));
    tokio::fs::create_dir_all(&tmp_dir).await?;
    match &cli.command {
        Commands::Areamap => areamap::process_areamap(&cli.postgres_url, &tmp_dir).await?,
        Commands::Mesh {
            level,
            year,
            survey,
        } => {
            mesh::process_mesh(&cli.postgres_url, &tmp_dir, *level, *year, survey).await?;
        }
    }

    Ok(())
}
