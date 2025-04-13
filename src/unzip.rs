use anyhow::{Result, anyhow};
use std::path::PathBuf;
use tokio::process::Command;

pub async fn unzip_archive(zip_path: &PathBuf) -> Result<PathBuf> {
    let out_dir = zip_path.clone().with_extension("");
    // Remove the output directory if it exists
    if out_dir.exists() {
        tokio::fs::remove_dir_all(&out_dir).await?;
    }
    let output = Command::new("unzip")
        .arg(zip_path)
        .arg("-d")
        .arg(&out_dir)
        .output()
        .await?;

    if !output.status.success() {
        eprintln!(
            "Failed to unzip: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return Err(anyhow!("Failed to unzip"));
    }

    // Find *.shp file in the output directory
    let mut shape_file = None;
    let mut entries = tokio::fs::read_dir(&out_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let entry = entry.path();
        if entry.extension().map_or(false, |ext| ext == "shp") {
            shape_file = Some(entry);
            break;
        }
    }

    if let Some(shape_file) = shape_file {
        Ok(shape_file)
    } else {
        Err(anyhow!("No .shp file found in the unzipped directory"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unzip_archive() {
        let zip_path = PathBuf::from("./test/2000-31.zip");
        let result = unzip_archive(&zip_path).await;
        assert!(result.is_ok());
        let shape_file = result.unwrap();
        assert!(shape_file.exists());
        assert_eq!(shape_file.extension().unwrap(), "shp");
        assert_eq!(shape_file.file_stem().unwrap(), "h12ka31");

        tokio::fs::remove_dir_all(shape_file.parent().unwrap())
            .await
            .unwrap();
    }
}
