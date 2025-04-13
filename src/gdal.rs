use anyhow::Result;
use std::path::PathBuf;
use tokio::process::Command;

pub async fn create_vrt(out: &PathBuf, shapes: &Vec<PathBuf>) -> Result<()> {
    if shapes.is_empty() {
        anyhow::bail!("No shapefiles found");
    }

    let bare_vrt = out.with_extension("");
    let layer_name = bare_vrt.file_name().unwrap().to_str().unwrap();
    // let vrt_path = shape.with_extension("vrt");

    let mut layers = String::new();
    for shape in shapes {
        let bare_shape = shape.with_extension("");
        let shape_filename = bare_shape.file_name().unwrap().to_str().unwrap();
        let encoding = "CP932"; // detect_encoding(shape).await?;
        layers.push_str(&format!(
            r#"
                <OGRVRTLayer name="{}">
                <SrcDataSource>{}</SrcDataSource>
                <OpenOptions><OOI key="ENCODING">{}</OOI></OpenOptions>
                </OGRVRTLayer>
            "#,
            shape_filename,
            shape.canonicalize().unwrap().to_str().unwrap(),
            encoding,
        ));
    }

    let vrt = format!(
        r#"
        <OGRVRTDataSource>
        <OGRVRTUnionLayer name="{}">
        {}
        </OGRVRTUnionLayer>
        </OGRVRTDataSource>
    "#,
        layer_name, layers
    );

    tokio::fs::write(&out, vrt).await?;

    Ok(())
}

pub async fn load_to_postgres(vrt: &PathBuf, postgres_url: &str) -> Result<()> {
    let mut cmd = Command::new("ogr2ogr");
    let output = cmd
        .arg("-f")
        .arg("PostgreSQL")
        .arg(format!("PG:{}", postgres_url))
        // .arg("-skipfailures")
        .arg("-lco")
        .arg("GEOM_TYPE=geometry")
        .arg("-lco")
        .arg("OVERWRITE=YES")
        .arg("-lco")
        .arg("GEOMETRY_NAME=geom")
        // .arg("-nlt")
        // .arg("PROMOTE_TO_MULTI")
        .arg("--config")
        .arg("PG_USE_COPY=YES")
        .arg(vrt)
        .output()
        .await?;

    if !output.status.success() {
        // the error message may contain malformed UTF8
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ogr2ogr failed: {}", stderr);
    }

    Ok(())
}
