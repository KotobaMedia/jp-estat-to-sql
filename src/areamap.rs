use anyhow::{Context as _, Result};
use futures::{StreamExt, stream};
use indicatif::{ProgressBar, ProgressStyle};
use km_to_sql::metadata::{ColumnMetadata, TableMetadata};
use std::path::Path;
use tokio_postgres::NoTls;
use url::Url;

use crate::{gdal, download::{self, DownloadedItem}};

const PREF_CODES: [&str; 47] = [
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16",
    "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32",
    "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47",
];

#[derive(Clone, Debug)]
pub struct DlServey<'a> {
    year: u32,
    id: &'a str,
    datum: &'a str,
}

const DL_SERVEY_IDS: [DlServey; 5] = [
    DlServey {
        year: 2020,
        id: "A002005212020",
        datum: "2011",
    }, // 2020年
    DlServey {
        year: 2015,
        id: "A002005212015",
        datum: "2011",
    }, // 2015年
    DlServey {
        year: 2010,
        id: "A002005212010",
        datum: "2000",
    }, // 2010年
    DlServey {
        year: 2005,
        id: "A002005212005",
        datum: "2000",
    }, // 2005年
    DlServey {
        year: 2000,
        id: "A002005212000",
        datum: "2000",
    }, // 2000年
];

fn get_shape_url(dlservey_id: &str, code: &str, datum: &str) -> String {
    format!(
        "https://www.e-stat.go.jp/gis/statmap-search/data?dlserveyId={}&code={}&coordSys=1&format=shape&downloadType=5&datum={}",
        dlservey_id, code, datum
    )
}

#[derive(Clone, Debug)]
struct ShapeUrlMeta {
    dlservey: DlServey<'static>,
    pref_code: &'static str,
    url: Url,
}

fn get_all_shape_urls() -> Vec<ShapeUrlMeta> {
    let mut urls = Vec::new();
    for code in PREF_CODES.iter() {
        for dlservey in DL_SERVEY_IDS.iter() {
            let url_str = get_shape_url(dlservey.id, code, dlservey.datum);
            urls.push(ShapeUrlMeta {
                dlservey: dlservey.clone(),
                pref_code: code,
                url: Url::parse(&url_str).expect("Failed to parse shape URL"),
            });
        }
    }
    urls
}

async fn import_shapes_to_postgis(
    downloaded_shapes: Vec<DownloadedItem<ShapeUrlMeta>>,
    postgres_url: &str,
    tmp_dir: &Path,
) -> Result<()> {
    let pb = ProgressBar::new(DL_SERVEY_IDS.len() as u64);
    let bar_style = ProgressStyle::default_bar()
        .template("{msg} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7}")?
        .progress_chars("##-");
    pb.set_style(bar_style);
    pb.set_message("Importing shapes to PostGIS...");
    stream::iter(DL_SERVEY_IDS.iter())
        .map(|servey| {
            let pb = pb.clone();
            let postgres_url = postgres_url.to_string();
            let shapes_for_year = downloaded_shapes
                .iter()
                .filter(|item| item.metadata.dlservey.year == servey.year)
                .map(|item| item.extracted_path.clone())
                .collect::<Vec<_>>();
            let tmp_dir = tmp_dir.to_path_buf();
            async move {
                if shapes_for_year.is_empty() {
                    println!("No shapes found for year {}, skipping VRT creation and import.", servey.year);
                    pb.inc(1);
                    return Ok(()) as Result<()>;
                }
                let vrt_path = tmp_dir.join(format!("jp_estat_areamap_{}.vrt", servey.year));
                gdal::create_vrt(&vrt_path, &shapes_for_year).await?;
                gdal::load_to_postgres(&vrt_path, &postgres_url).await?;
                pb.inc(1);
                Ok(()) as Result<()>
            }
        })
        .buffer_unordered(5)
        .collect::<Vec<Result<()>>>()
        .await
        .into_iter()
        .collect::<Result<()>>()?;

    println!("All imports completed.");
    Ok(())
}

async fn data_postprocessing_cleanup(postgres_url: &str) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(postgres_url, NoTls)
        .await
        .with_context(|| "when connecting to PostgreSQL")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            panic!("PostgreSQL connection error: {}", e);
        }
    });

    km_to_sql::postgres::init_schema(&client).await?;

    for servey in DL_SERVEY_IDS.iter() {
        let table_name = format!("jp_estat_areamap_{}", servey.year);
        let mut srid = "6668"; // 日本測地系2011
        if servey.datum == "2000" {
            srid = "4621"; // 日本測地系2000
        }

        // hcode = 8154 は「水面調査区」、今回のデータには不要なので削除する
        let query = format!("DELETE FROM {} WHERE hcode = 8154", table_name);
        client.execute(&query, &[]).await?;

        let columns: Vec<ColumnMetadata> = vec![
            ColumnMetadata {
                name: "ogc_fid".to_string(),
                desc: None,
                data_type: "integer".to_string(),
                foreign_key: None,
                enum_values: None,
            },
            ColumnMetadata {
                name: "geom".to_string(),
                desc: Some("Geometry".to_string()),
                data_type: format!("geometry(polygon, {})", srid),
                foreign_key: None,
                enum_values: None,
            },
            ColumnMetadata {
                name: "key_code".to_string(),
                desc: Some("小地域コード".to_string()),
                data_type: "varchar(255)".to_string(),
                foreign_key: None,
                enum_values: None,
            },
            ColumnMetadata {
                name: "pref_name".to_string(),
                desc: Some("都道府県名".to_string()),
                data_type: "varchar(255)".to_string(),
                foreign_key: None,
                enum_values: None,
            },
            ColumnMetadata {
                name: "city_name".to_string(),
                desc: Some("市区町村名".to_string()),
                data_type: "varchar(255)".to_string(),
                foreign_key: None,
                enum_values: None,
            },
            ColumnMetadata {
                name: "s_name".to_string(),
                desc: Some("小地域名".to_string()),
                data_type: "varchar(255)".to_string(),
                foreign_key: None,
                enum_values: None,
            },
            ColumnMetadata {
                name: "jinko".to_string(),
                desc: Some("人口".to_string()),
                data_type: "int".to_string(),
                foreign_key: None,
                enum_values: None,
            },
            ColumnMetadata {
                name: "setai".to_string(),
                desc: Some("世帯数".to_string()),
                data_type: "int".to_string(),
                foreign_key: None,
                enum_values: None,
            },
        ];

        let metadata = TableMetadata {
            name: format!("国勢調査 {}年 小地域境界データ", servey.year),
            desc: Some(
                "丁目・大字・小字などの境界ポリゴンと、簡易的な人口データが含まれている"
                    .to_string(),
            ),
            source: Some("総務省統計局".to_string()),
            source_url: Some(Url::parse(
                "https://www.e-stat.go.jp/gis/statmap-search?page=1&type=2&aggregateUnitForBoundary=A&toukeiCode=00200521",
            ).unwrap()),
            license: None,
            license_url: Some(Url::parse("https://www.e-stat.go.jp/terms-of-use").unwrap()),
            primary_key: Some("ogc_fid".to_string()),
            columns,
        };
        km_to_sql::postgres::upsert(&client, &table_name, &metadata).await?;
    }

    Ok(())
}

pub async fn process_areamap(postgres_url: &str, tmp_dir: &Path) -> Result<()> {
    // 1. Get URLs and metadata
    let shape_url_metas = get_all_shape_urls();

    // 2. Download all shapes and unzip them using the generic function
    let downloaded_items: Vec<DownloadedItem<ShapeUrlMeta>> = download::download_and_extract_all(
        stream::iter(shape_url_metas),
        |meta| meta.url.clone(),
        |meta| format!("{}-{}.zip", meta.dlservey.year, meta.pref_code),
        "shp", // Target extension is .shp
        tmp_dir,
        "Downloading Shapes...",
        "Extracting Shapes...",
        10, // Concurrency level
    )
    .await?;

    // 3. Import the shapefiles into PostGIS
    import_shapes_to_postgis(downloaded_items, postgres_url, tmp_dir).await?;

    // 4. Clean up the data & update metadata
    data_postprocessing_cleanup(postgres_url).await?;

    Ok(())
}
