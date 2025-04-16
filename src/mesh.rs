use crate::unzip;
use anyhow::{Context, Result, anyhow};
use csv::ReaderBuilder;
use encoding_rs::SHIFT_JIS;
use encoding_rs_io::DecodeReaderBytesBuilder;
use futures::{StreamExt as _, stream};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use jismesh::codes::JAPAN_LV1;
use reqwest::Client;
use std::{
    io::BufReader,
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::{fs::File, io::AsyncWriteExt as _};
use tokio_postgres::{NoTls, types::ToSql};
use url::Url;

fn open_shiftjis_csv(path: &str) -> csv::Reader<Box<dyn std::io::Read>> {
    let file = std::fs::File::open(path).expect("failed to open file");
    let reader = BufReader::new(file);

    let transcoded = DecodeReaderBytesBuilder::new()
        .encoding(Some(SHIFT_JIS))
        .build(reader);

    ReaderBuilder::new()
        .has_headers(false) // we'll handle headers ourselves
        .from_reader(Box::new(transcoded))
}

fn parse_nullable<T>(value: &str) -> Result<Option<T>>
where
    T: FromStr,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    let v = value.trim();
    if v.is_empty() || v == "*" {
        Ok(None)
    } else {
        Ok(Some(v.parse::<T>()?))
    }
}

struct MeshStats<'a> {
    name: &'a str,
    year: u16,
    meshlevel: u8,
    stats_id: &'a str,

    /// The EPSG code the mesh code is based on.
    /// Valid values: 4301 (Tokyo Datum), 4612 (JGD2000), 6668 (JGD2011)
    datum: u16,
}

const AVAILABLE: [MeshStats; 6] = [
    // 2020年国勢調査 - 3次メッシュ
    MeshStats {
        name: "人口及び世帯",
        year: 2020,
        meshlevel: 3,
        stats_id: "T001140",
        datum: 6668,
    },
    MeshStats {
        name: "人口移動、就業状態等及び従業地・通学地",
        year: 2020,
        meshlevel: 3,
        stats_id: "T001143",
        datum: 6668,
    },
    // 2020年国勢調査 - 4次メッシュ
    MeshStats {
        name: "人口及び世帯",
        year: 2020,
        meshlevel: 4,
        stats_id: "T001141",
        datum: 6668,
    },
    MeshStats {
        name: "人口移動、就業状態等及び従業地・通学地",
        year: 2020,
        meshlevel: 4,
        stats_id: "T001144",
        datum: 6668,
    },
    // 2020年国勢調査 - 5次メッシュ
    MeshStats {
        name: "人口及び世帯",
        year: 2020,
        meshlevel: 5,
        stats_id: "T001142",
        datum: 6668,
    },
    MeshStats {
        name: "人口移動、就業状態等及び従業地・通学地",
        year: 2020,
        meshlevel: 5,
        stats_id: "T001145",
        datum: 6668,
    },
];

fn get_matching_mesh_stats(
    level: u8,
    year: u16,
    survey: &str,
) -> Option<&'static MeshStats<'static>> {
    for mesh in AVAILABLE.iter() {
        if mesh.meshlevel == level && mesh.year == year && mesh.name == survey {
            return Some(mesh);
        }
    }
    None
}

fn get_all_csv_urls(mesh_stats: &MeshStats) -> Vec<(u64, Url)> {
    let mut out = Vec::with_capacity(JAPAN_LV1.len());
    for mesh in JAPAN_LV1.iter() {
        let url = format!(
            "https://www.e-stat.go.jp/gis/statmap-search/data?statsId={}&code={}&downloadType=2",
            mesh_stats.stats_id, mesh
        );
        out.push((*mesh, Url::parse(&url).unwrap()));
    }
    out
}

async fn download_all_files(
    mesh_stats: &MeshStats<'static>,
    tmp_dir: &Path,
) -> Result<Vec<PathBuf>> {
    let urls = get_all_csv_urls(mesh_stats);

    // Set a limit to how many downloads happen at once
    let concurrency = 10;
    let client = Client::new();

    let multibar = MultiProgress::new();
    let bar_style = ProgressStyle::default_bar()
        .template("{msg} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7}")?
        .progress_chars("##-");
    let dl_pb = multibar.add(ProgressBar::new(urls.len() as u64));
    dl_pb.set_style(bar_style.clone());
    dl_pb.set_message("Downloading CSVs...");

    let zip_pb = multibar.add(ProgressBar::new(urls.len() as u64));
    zip_pb.set_style(bar_style);
    zip_pb.set_message("Extracting CSVs... ");

    let results = stream::iter(urls)
        .map(|(mesh, url)| {
            let client = client.clone();
            let pb = dl_pb.clone();
            let zip_pb = zip_pb.clone();
            async move {
                let filename = format!("{}-{}-{}.zip", mesh_stats.year, mesh_stats.stats_id, mesh,);
                let filepath = tmp_dir.join(&filename);

                if filepath.exists() {
                    pb.inc(1);
                    // println!("Already exists: {:#?}", filepath);
                    return Ok(Some(filepath)) as Result<Option<PathBuf>>;
                }

                let response = client.get(url.clone()).send().await?;
                if response.status().is_success() {
                    let content = response.bytes().await?;
                    let mut file = File::create(&filepath).await?;
                    file.write_all(&content).await?;
                    file.flush().await?;
                    drop(file); // Close the file to ensure it's written
                } else if response.status() == reqwest::StatusCode::NOT_FOUND {
                    pb.inc(1);
                    zip_pb.dec_length(1);
                    // Skip this file
                    return Ok(None) as Result<Option<PathBuf>>;
                } else {
                    println!("Failed to download: {} [{}]", url, response.status());
                    pb.inc(1);
                    return Err(anyhow!("Failed to download")) as Result<_>;
                }

                pb.inc(1);
                Ok(Some(filepath))
            }
        })
        .buffer_unordered(concurrency)
        .filter_map(|result| async {
            match result {
                Ok(Some(path)) => Some(Ok(path)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        })
        .map(|result| {
            let pb = zip_pb.clone();
            async move {
                let zip_path = result?;
                // Unzip the downloaded file
                let mut csv_file = unzip::unzip_archive(&zip_path).await?;
                csv_file = unzip::find_file_with_ext(&csv_file, "txt").await?;
                pb.inc(1);
                Ok(csv_file) as Result<PathBuf>
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>() // collect all the results to await everything
        .await;

    println!("All downloads completed.");

    results.into_iter().collect()
}

fn infer_column_type(col: &str) -> &'static str {
    if col == "KEY_CODE" || col == "HTKSAKI" {
        "BIGINT"
    } else if col == "GASSAN" {
        "BIGINT[]"
    } else if col == "HTKSYORI" {
        "SMALLINT"
    } else {
        "INTEGER"
    }
}

/// Given a path to a CSV file, create a schema in the Postgres database
/// Returns a tuple of (table name, column names)
async fn create_schema(
    client: &tokio_postgres::Client,
    mesh_stats: &MeshStats<'static>,
    file: &Path,
) -> Result<(String, Vec<String>)> {
    let mut rdr = open_shiftjis_csv(file.to_str().unwrap());

    // Read headers
    let header1 = rdr.records().next().unwrap()?; // first header row
    let header2 = rdr.records().next().unwrap()?; // second header row

    // Determine column names
    let columns: Vec<String> = header2
        .iter()
        .enumerate()
        .map(|(i, h2)| {
            let col = if h2.trim().is_empty() {
                // if header2 is empty, use header1
                // if header1 is empty, we probably have a bad CSV file.
                header1.get(i).unwrap().to_string()
            } else {
                h2.to_string()
            };

            col.trim().replace("\u{3000}", "").to_string()
        })
        .collect();

    let column_defs: Vec<String> = columns
        .iter()
        .map(|col| format!("\"{}\" {}", col, infer_column_type(col)))
        .collect();

    let table_name = format!(
        "jp_estat_mesh_{}_{}_{}",
        mesh_stats.year, mesh_stats.stats_id, mesh_stats.meshlevel,
    );
    client
        .execute(&format!("DROP TABLE IF EXISTS {}", &table_name), &[])
        .await?;
    let create_stmt = format!("CREATE TABLE {} ({});", &table_name, column_defs.join(", "));
    client.execute(&create_stmt, &[]).await?;

    Ok((table_name, columns))
}

async fn import_csv_to_postgres(
    client: &mut tokio_postgres::Client,
    file: &Path,
    table_name: &str,
    columns: &[String],
) -> Result<()> {
    let mut rdr = open_shiftjis_csv(file.to_str().unwrap());
    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name,
        columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", "),
        columns
            .iter()
            .enumerate()
            .map(|(i, _)| format!("${}", i + 1))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let insert_stmt = client.prepare(&insert_sql).await?;

    let tx = client.transaction().await?;

    // Skip the first two header rows
    rdr.records().next().unwrap()?;
    rdr.records().next().unwrap()?;

    for result in rdr.records() {
        let record = result?;
        let mut params: Vec<Box<dyn ToSql + Sync>> = Vec::with_capacity(columns.len());
        for (i, col) in columns.iter().enumerate() {
            let value = record.get(i).unwrap_or("");
            if col == "KEY_CODE" || col == "HTKSAKI" {
                params.push(Box::new(parse_nullable::<i64>(value)?));
            } else if col == "HTKSYORI" {
                params.push(Box::new(parse_nullable::<i16>(value)?));
            } else if col == "GASSAN" {
                if value.is_empty() {
                    params.push(Box::new(None::<Vec<i64>>));
                } else {
                    let values: Vec<i64> = value
                        .split(';')
                        .map(|s| s.parse::<_>())
                        .collect::<Result<Vec<_>, _>>()?;
                    params.push(Box::new(values));
                }
            } else {
                params.push(Box::new(parse_nullable::<i32>(value)?));
            }
        }
        tx.execute(
            &insert_stmt,
            &params.iter().map(|p| p.as_ref()).collect::<Vec<_>>(),
        )
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub async fn process_mesh(
    postgres_url: &str,
    tmp_dir: &Path,
    level: u8,
    year: u16,
    survey: &str,
) -> Result<()> {
    let mesh_stats = get_matching_mesh_stats(level, year, survey)
        .ok_or(anyhow!("一致する統計データが見つかりません"))?;
    let files = download_all_files(mesh_stats, tmp_dir).await?;
    println!("Files downloaded and extracted.");

    let first = files.first().ok_or(anyhow!("No files found"))?;

    let (mut client, connection) = tokio_postgres::connect(postgres_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("DB error: {}", e);
        }
    });

    let (table_name, columns) = create_schema(&client, mesh_stats, &first).await?;
    println!("Schema created: {}", table_name);

    let pb_style = ProgressStyle::default_bar()
        .template("{msg} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7}")?
        .progress_chars("##-");
    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(pb_style);
    pb.set_message("Importing CSVs...");
    for file in files.iter() {
        import_csv_to_postgres(&mut client, file, &table_name, &columns)
            .await
            .with_context(|| format!("when importing {}", &file.display()))?;
        pb.inc(1);
    }
    pb.finish();

    Ok(())
}
