use crate::unzip;
use anyhow::{Result, anyhow};
use futures::{Stream, StreamExt as _, stream};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use std::path::{Path, PathBuf};
use tokio::{fs::File, io::AsyncWriteExt as _};
use url::Url;

/// Represents an item successfully downloaded and extracted.
pub struct DownloadedItem<T> {
    /// The original metadata associated with the download.
    pub metadata: T,
    /// The path to the extracted file (e.g., the .csv or .shp file).
    pub extracted_path: PathBuf,
    // /// The path to the original downloaded archive (e.g., the .zip file).
    // pub archive_path: PathBuf,
}

/// Downloads a collection of files, reports progress, extracts them, and returns paths to the extracted files.
///
/// # Arguments
///
/// * `items` - A stream of metadata items (`T`) to be processed.
/// * `get_url` - A function that takes a metadata item (`&T`) and returns the `Url` to download.
/// * `get_filename` - A function that takes a metadata item (`&T`) and returns the desired filename for the download (e.g., "data.zip").
/// * `target_ext` - The file extension to look for within the extracted archive (e.g., "csv", "shp").
/// * `tmp_dir` - The directory where downloaded archives and extracted files will be stored.
/// * `dl_message` - The message to display on the download progress bar.
/// * `extract_message` - The message to display on the extraction progress bar.
/// * `concurrency` - The maximum number of concurrent downloads/extractions.
///
/// # Returns
///
/// A `Result` containing a `Vec` of `DownloadedItem<T>` structs, each representing a successfully downloaded and extracted file.
pub async fn download_and_extract_all<T, S, FUrl, FFilename>(
    items: S,
    get_url: FUrl,
    get_filename: FFilename,
    target_ext: &'static str,
    tmp_dir: &Path,
    dl_message: &'static str,
    extract_message: &'static str,
    concurrency: usize,
) -> Result<Vec<DownloadedItem<T>>>
where
    T: Send + Sync + 'static + Clone,
    S: Stream<Item = T> + Send + 'static,
    FUrl: Fn(&T) -> Url + Send + Sync + 'static + Copy,
    FFilename: Fn(&T) -> String + Send + Sync + 'static + Copy,
{
    let client = Client::new();
    let items_vec: Vec<T> = items.collect().await;
    let total_items = items_vec.len() as u64;

    let multibar = MultiProgress::new();
    let bar_style = ProgressStyle::default_bar()
        .template("{msg} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7}")?
        .progress_chars("##-");

    let dl_pb = multibar.add(ProgressBar::new(total_items));
    dl_pb.set_style(bar_style.clone());
    dl_pb.set_message(dl_message);

    let zip_pb = multibar.add(ProgressBar::new(total_items));
    zip_pb.set_style(bar_style);
    zip_pb.set_message(extract_message);

    let results = stream::iter(items_vec)
        .map(|item| {
            let client = client.clone();
            let pb = dl_pb.clone();
            let zip_pb = zip_pb.clone();
            let tmp_dir = tmp_dir.to_path_buf();
            async move {
                let filename = get_filename(&item);
                let filepath = tmp_dir.join(&filename);
                let url = get_url(&item);

                if filepath.exists() {
                    pb.inc(1);
                    return Ok(Some((item, filepath))) as Result<Option<(T, PathBuf)>>;
                }

                let response = client.get(url.clone()).send().await?;
                if response.status().is_success() {
                    let content = response.bytes().await?;
                    let mut file = File::create(&filepath).await?;
                    file.write_all(&content).await?;
                    file.flush().await?;
                    drop(file); // Close the file
                } else if response.status() == reqwest::StatusCode::NOT_FOUND {
                    pb.inc(1);
                    zip_pb.dec_length(1); // Adjust total for extraction bar
                    return Ok(None) as Result<Option<(T, PathBuf)>>;
                } else {
                    println!("Failed to download: {} [{}]", url, response.status());
                    pb.inc(1);
                    return Err(anyhow!("Failed to download {}", url)) as Result<_>;
                }

                pb.inc(1);
                Ok(Some((item, filepath)))
            }
        })
        .buffer_unordered(concurrency)
        .filter_map(|result| async {
            match result {
                Ok(Some(data)) => Some(Ok(data)),
                Ok(None) => None, // Skip items that were not found (404)
                Err(e) => Some(Err(e)),
            }
        })
        .map(|result| {
            let pb = zip_pb.clone();
            async move {
                let (metadata, archive_path) = result?;
                let mut extracted_path = unzip::unzip_archive(&archive_path).await?;
                extracted_path = unzip::find_file_with_ext(&extracted_path, target_ext).await?;
                pb.inc(1);
                Ok(DownloadedItem {
                    metadata,
                    extracted_path,
                    // archive_path,
                }) as Result<DownloadedItem<T>>
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    dl_pb.finish_with_message(format!("{} completed.", dl_message));
    zip_pb.finish_with_message(format!("{} completed.", extract_message));

    // Collect results, propagating the first error encountered
    results.into_iter().collect()
}
