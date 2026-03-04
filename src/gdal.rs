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

fn is_postgresql_output(output: &str, output_format: Option<&str>) -> bool {
    output.starts_with("PG:")
        || output.starts_with("pg:")
        || output_format
            .map(|v| v.eq_ignore_ascii_case("postgresql"))
            .unwrap_or(false)
}

pub async fn load(vrt: &PathBuf, output: &str, output_format: Option<&str>) -> Result<()> {
    let mut cmd = Command::new("ogr2ogr");
    if let Some(format) = output_format {
        cmd.arg("-f").arg(format);
    }
    cmd.arg("-overwrite");

    if is_postgresql_output(output, output_format) {
        cmd.arg("-lco")
            .arg("GEOM_TYPE=geometry")
            .arg("-lco")
            .arg("GEOMETRY_NAME=geom")
            .arg("--config")
            .arg("PG_USE_COPY=YES");
    }

    let output = cmd.arg(output).arg(vrt).output().await?;

    if !output.status.success() {
        // the error message may contain malformed UTF8
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ogr2ogr failed: {}", stderr);
    }

    Ok(())
}
