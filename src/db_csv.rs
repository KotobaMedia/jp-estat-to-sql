use crate::estat_api::EStatApiClient;
use anyhow::{Context, Result, anyhow, bail};
use csv::{ReaderBuilder, WriterBuilder};
use futures::{StreamExt as _, stream};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::{Path, PathBuf},
};

const TABLE_HEADERS: &[&str] = &[
    "stats_data_id",
    "table_name",
    "stat_code",
    "stat_name",
    "gov_org_code",
    "gov_org_name",
    "survey_date",
    "open_date",
    "small_area",
    "collect_area",
    "main_category_code",
    "sub_category_code",
    "link",
    "fetched_at",
];

const DIMENSION_HEADERS: &[&str] = &[
    "stats_data_id",
    "dimension_id",
    "dimension_name",
    "classification_level",
    "is_time",
    "is_area",
    "is_tab",
    "source_order",
];

const DIMENSION_ITEM_HEADERS: &[&str] = &[
    "stats_data_id",
    "dimension_id",
    "item_code",
    "item_name",
    "level",
    "parent_code",
    "unit",
    "note",
    "source_order",
];

const OBSERVATION_HEADERS: &[&str] = &[
    "stats_data_id",
    "value",
    "value_text",
    "annotation",
    "unit",
    "time_code",
    "area_code",
    "tab_code",
    "cat01_code",
    "cat02_code",
    "cat03_code",
    "cat04_code",
    "cat05_code",
    "cat06_code",
    "cat07_code",
    "cat08_code",
    "cat09_code",
    "cat10_code",
    "cat11_code",
    "cat12_code",
    "cat13_code",
    "cat14_code",
    "cat15_code",
    "fetched_at",
];

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TableRow {
    stats_data_id: String,
    table_name: String,
    stat_code: String,
    stat_name: String,
    gov_org_code: String,
    gov_org_name: String,
    survey_date: String,
    open_date: String,
    small_area: String,
    collect_area: String,
    main_category_code: String,
    sub_category_code: String,
    link: String,
    fetched_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DimensionRow {
    stats_data_id: String,
    dimension_id: String,
    dimension_name: String,
    classification_level: String,
    is_time: bool,
    is_area: bool,
    is_tab: bool,
    source_order: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DimensionItemRow {
    stats_data_id: String,
    dimension_id: String,
    item_code: String,
    item_name: String,
    level: String,
    parent_code: String,
    unit: String,
    note: String,
    source_order: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ObservationRow {
    stats_data_id: String,
    value: String,
    value_text: String,
    annotation: String,
    unit: String,
    time_code: String,
    area_code: String,
    tab_code: String,
    cat01_code: String,
    cat02_code: String,
    cat03_code: String,
    cat04_code: String,
    cat05_code: String,
    cat06_code: String,
    cat07_code: String,
    cat08_code: String,
    cat09_code: String,
    cat10_code: String,
    cat11_code: String,
    cat12_code: String,
    cat13_code: String,
    cat14_code: String,
    cat15_code: String,
    fetched_at: String,
}

#[derive(Debug)]
struct NormalizedDataset {
    table: TableRow,
    dimensions: Vec<DimensionRow>,
    dimension_items: Vec<DimensionItemRow>,
    observations: Vec<ObservationRow>,
}

#[derive(Debug, Default)]
struct ExistingOutputs {
    tables: HashMap<String, Vec<TableRow>>,
    dimensions: HashMap<String, Vec<DimensionRow>>,
    dimension_items: HashMap<String, Vec<DimensionItemRow>>,
}

#[derive(Clone, Debug)]
struct DatasetPlan {
    stats_data_id: String,
    observation_path: PathBuf,
    raw_meta_path: Option<PathBuf>,
    raw_data_path: Option<PathBuf>,
    skip_observation_write: bool,
    reuse_existing: bool,
}

#[derive(Debug)]
struct ProcessedDataset {
    stats_data_id: String,
    table: TableRow,
    dimensions: Vec<DimensionRow>,
    dimension_items: Vec<DimensionItemRow>,
    manifest: ManifestDataset,
}

#[derive(Debug)]
struct ReusedDataset {
    table: TableRow,
    dimensions: Vec<DimensionRow>,
    dimension_items: Vec<DimensionItemRow>,
    manifest: ManifestDataset,
}

#[derive(Clone, Debug, Serialize)]
struct Manifest {
    command: &'static str,
    version: &'static str,
    output_dir: String,
    requested_stats_data_ids: Vec<String>,
    resume: bool,
    overwrite: bool,
    raw_json: bool,
    concurrency: usize,
    datasets: Vec<ManifestDataset>,
}

#[derive(Clone, Debug, Serialize)]
struct ManifestDataset {
    stats_data_id: String,
    status: String,
    table_name: String,
    fetched_at: String,
    observation_csv: String,
    observation_count: usize,
    dimension_count: usize,
    dimension_item_count: usize,
    raw_meta_json: Option<String>,
    raw_data_json: Option<String>,
}

trait HasStatsDataId {
    fn stats_data_id(&self) -> &str;
}

impl HasStatsDataId for TableRow {
    fn stats_data_id(&self) -> &str {
        &self.stats_data_id
    }
}

impl HasStatsDataId for DimensionRow {
    fn stats_data_id(&self) -> &str {
        &self.stats_data_id
    }
}

impl HasStatsDataId for DimensionItemRow {
    fn stats_data_id(&self) -> &str {
        &self.stats_data_id
    }
}

pub async fn process_db_csv(
    app_id: &str,
    output_dir: &Path,
    stats_data_ids: &[String],
    resume: bool,
    overwrite: bool,
    concurrency: usize,
    raw_json: bool,
) -> Result<()> {
    if concurrency == 0 {
        bail!("concurrency must be greater than 0");
    }

    ensure_unique_stats_data_ids(stats_data_ids)?;

    tokio::fs::create_dir_all(output_dir).await?;
    tokio::fs::create_dir_all(output_dir.join("observations")).await?;
    if raw_json {
        tokio::fs::create_dir_all(output_dir.join("raw").join("meta")).await?;
        tokio::fs::create_dir_all(output_dir.join("raw").join("data")).await?;
    }

    if !resume && !overwrite {
        ensure_no_conflicting_outputs(output_dir, stats_data_ids, raw_json)?;
    }

    let existing = if resume {
        ExistingOutputs::load(output_dir)?
    } else {
        ExistingOutputs::default()
    };

    let plans = build_dataset_plans(output_dir, stats_data_ids, resume, raw_json, &existing)?;
    let reuse_count = plans.iter().filter(|plan| plan.reuse_existing).count() as u64;

    let pb_style = ProgressStyle::default_bar()
        .template("{msg} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7}")?
        .progress_chars("##-");
    let pb = ProgressBar::new(stats_data_ids.len() as u64);
    pb.set_style(pb_style);
    pb.set_message("Exporting DB tables...");
    pb.inc(reuse_count);

    let api = EStatApiClient::new();
    let fetch_plans: Vec<DatasetPlan> = plans
        .iter()
        .filter(|plan| !plan.reuse_existing)
        .cloned()
        .collect();
    let fetch_results = stream::iter(fetch_plans.into_iter().map(|plan| {
        let api = api.clone();
        let pb = pb.clone();
        let app_id = app_id.to_string();
        async move {
            let result = process_dataset(&api, &app_id, plan).await;
            pb.inc(1);
            result
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;
    let processed: Vec<ProcessedDataset> = fetch_results.into_iter().collect::<Result<_>>()?;
    let processed_by_id: HashMap<String, ProcessedDataset> = processed
        .into_iter()
        .map(|dataset| (dataset.stats_data_id.clone(), dataset))
        .collect();

    let mut reused_by_id = HashMap::new();
    for plan in plans.iter().filter(|plan| plan.reuse_existing) {
        let reused = existing.reuse_dataset(output_dir, plan, raw_json)?;
        reused_by_id.insert(plan.stats_data_id.clone(), reused);
    }

    let mut tables = Vec::new();
    let mut dimensions = Vec::new();
    let mut dimension_items = Vec::new();
    let mut manifest_datasets = Vec::new();

    for stats_data_id in stats_data_ids {
        if let Some(reused) = reused_by_id.remove(stats_data_id) {
            tables.push(reused.table);
            dimensions.extend(reused.dimensions);
            dimension_items.extend(reused.dimension_items);
            manifest_datasets.push(reused.manifest);
            continue;
        }

        let processed = processed_by_id
            .get(stats_data_id)
            .ok_or_else(|| anyhow!("missing processed dataset for {}", stats_data_id))?;
        tables.push(processed.table.clone());
        dimensions.extend(processed.dimensions.clone());
        dimension_items.extend(processed.dimension_items.clone());
        manifest_datasets.push(processed.manifest.clone());
    }

    write_csv(output_dir.join("tables.csv"), TABLE_HEADERS, &tables)?;
    write_csv(
        output_dir.join("dimensions.csv"),
        DIMENSION_HEADERS,
        &dimensions,
    )?;
    write_csv(
        output_dir.join("dimension_items.csv"),
        DIMENSION_ITEM_HEADERS,
        &dimension_items,
    )?;

    let manifest = Manifest {
        command: "db-csv",
        version: env!("CARGO_PKG_VERSION"),
        output_dir: output_dir.display().to_string(),
        requested_stats_data_ids: stats_data_ids.to_vec(),
        resume,
        overwrite,
        raw_json,
        concurrency,
        datasets: manifest_datasets,
    };
    write_json(output_dir.join("manifest.json"), &manifest)?;

    pb.finish_with_message(format!("db-csv output written to {}", output_dir.display()));
    Ok(())
}

impl ExistingOutputs {
    fn load(output_dir: &Path) -> Result<Self> {
        Ok(Self {
            tables: read_rows_by_stats_data_id(&output_dir.join("tables.csv"))?,
            dimensions: read_rows_by_stats_data_id(&output_dir.join("dimensions.csv"))?,
            dimension_items: read_rows_by_stats_data_id(&output_dir.join("dimension_items.csv"))?,
        })
    }

    fn can_reuse(&self, output_dir: &Path, stats_data_id: &str, raw_json: bool) -> bool {
        let observation_exists = observation_path(output_dir, stats_data_id).exists();
        let has_table = self
            .tables
            .get(stats_data_id)
            .is_some_and(|rows| !rows.is_empty());
        let has_dimensions = self
            .dimensions
            .get(stats_data_id)
            .is_some_and(|rows| !rows.is_empty());
        let has_raw = !raw_json
            || (raw_meta_path(output_dir, stats_data_id).exists()
                && raw_data_path(output_dir, stats_data_id).exists());

        observation_exists && has_table && has_dimensions && has_raw
    }

    fn reuse_dataset(
        &self,
        output_dir: &Path,
        plan: &DatasetPlan,
        raw_json: bool,
    ) -> Result<ReusedDataset> {
        let table = self
            .tables
            .get(&plan.stats_data_id)
            .and_then(|rows| rows.first())
            .cloned()
            .ok_or_else(|| anyhow!("missing existing table row for {}", plan.stats_data_id))?;
        let dimensions = self
            .dimensions
            .get(&plan.stats_data_id)
            .cloned()
            .unwrap_or_default();
        let dimension_items = self
            .dimension_items
            .get(&plan.stats_data_id)
            .cloned()
            .unwrap_or_default();
        let manifest = ManifestDataset {
            stats_data_id: plan.stats_data_id.clone(),
            status: "resumed".to_string(),
            table_name: table.table_name.clone(),
            fetched_at: table.fetched_at.clone(),
            observation_csv: relative_path(
                output_dir,
                &observation_path(output_dir, &plan.stats_data_id),
            ),
            observation_count: count_csv_rows(&observation_path(output_dir, &plan.stats_data_id))?,
            dimension_count: dimensions.len(),
            dimension_item_count: dimension_items.len(),
            raw_meta_json: raw_json.then(|| {
                relative_path(output_dir, &raw_meta_path(output_dir, &plan.stats_data_id))
            }),
            raw_data_json: raw_json.then(|| {
                relative_path(output_dir, &raw_data_path(output_dir, &plan.stats_data_id))
            }),
        };

        Ok(ReusedDataset {
            table,
            dimensions,
            dimension_items,
            manifest,
        })
    }
}

fn build_dataset_plans(
    output_dir: &Path,
    stats_data_ids: &[String],
    resume: bool,
    raw_json: bool,
    existing: &ExistingOutputs,
) -> Result<Vec<DatasetPlan>> {
    let mut plans = Vec::with_capacity(stats_data_ids.len());

    for stats_data_id in stats_data_ids {
        let observation_path = observation_path(output_dir, stats_data_id);
        let raw_meta_path = raw_json.then(|| raw_meta_path(output_dir, stats_data_id));
        let raw_data_path = raw_json.then(|| raw_data_path(output_dir, stats_data_id));
        let skip_observation_write = resume && observation_path.exists();
        let reuse_existing = resume && existing.can_reuse(output_dir, stats_data_id, raw_json);

        plans.push(DatasetPlan {
            stats_data_id: stats_data_id.clone(),
            observation_path,
            raw_meta_path,
            raw_data_path,
            skip_observation_write,
            reuse_existing,
        });
    }

    Ok(plans)
}

async fn process_dataset(
    api: &EStatApiClient,
    app_id: &str,
    plan: DatasetPlan,
) -> Result<ProcessedDataset> {
    let meta = api
        .get_meta_info(app_id, &plan.stats_data_id)
        .await
        .with_context(|| format!("failed to fetch getMetaInfo for {}", plan.stats_data_id))?;
    let data_pages = api
        .get_stats_data_pages(app_id, &plan.stats_data_id)
        .await
        .with_context(|| format!("failed to fetch getStatsData for {}", plan.stats_data_id))?;

    if let Some(path) = plan.raw_meta_path.as_ref() {
        write_json(path, &meta)
            .with_context(|| format!("failed to write raw meta JSON for {}", plan.stats_data_id))?;
    }
    if let Some(path) = plan.raw_data_path.as_ref() {
        let raw_value = if data_pages.len() == 1 {
            data_pages[0].clone()
        } else {
            Value::Array(data_pages.clone())
        };
        write_json(path, &raw_value)
            .with_context(|| format!("failed to write raw data JSON for {}", plan.stats_data_id))?;
    }

    let normalized = normalize_dataset(&plan.stats_data_id, &meta, &data_pages)
        .with_context(|| format!("failed to normalize {}", plan.stats_data_id))?;

    if !plan.skip_observation_write {
        write_csv(
            &plan.observation_path,
            OBSERVATION_HEADERS,
            &normalized.observations,
        )
        .with_context(|| {
            format!(
                "failed to write observations CSV for {}",
                plan.stats_data_id
            )
        })?;
    }

    let observation_count = if plan.skip_observation_write {
        count_csv_rows(&plan.observation_path)?
    } else {
        normalized.observations.len()
    };
    let output_dir = plan
        .observation_path
        .parent()
        .and_then(|path| path.parent())
        .ok_or_else(|| anyhow!("invalid observation output path"))?;
    let manifest = ManifestDataset {
        stats_data_id: plan.stats_data_id.clone(),
        status: "fetched".to_string(),
        table_name: normalized.table.table_name.clone(),
        fetched_at: normalized.table.fetched_at.clone(),
        observation_csv: relative_path(output_dir, &plan.observation_path),
        observation_count,
        dimension_count: normalized.dimensions.len(),
        dimension_item_count: normalized.dimension_items.len(),
        raw_meta_json: plan
            .raw_meta_path
            .as_ref()
            .map(|path| relative_path(output_dir, path)),
        raw_data_json: plan
            .raw_data_path
            .as_ref()
            .map(|path| relative_path(output_dir, path)),
    };

    Ok(ProcessedDataset {
        stats_data_id: plan.stats_data_id,
        table: normalized.table,
        dimensions: normalized.dimensions,
        dimension_items: normalized.dimension_items,
        manifest,
    })
}

fn normalize_dataset(
    stats_data_id: &str,
    meta: &Value,
    data_pages: &[Value],
) -> Result<NormalizedDataset> {
    let meta_root = meta
        .get("GET_META_INFO")
        .ok_or_else(|| anyhow!("missing GET_META_INFO root"))?;
    let metadata_inf = meta_root
        .get("METADATA_INF")
        .ok_or_else(|| anyhow!("missing GET_META_INFO.METADATA_INF"))?;
    let table_inf = metadata_inf
        .get("TABLE_INF")
        .ok_or_else(|| anyhow!("missing GET_META_INFO.METADATA_INF.TABLE_INF"))?;
    let class_inf = metadata_inf
        .get("CLASS_INF")
        .ok_or_else(|| anyhow!("missing GET_META_INFO.METADATA_INF.CLASS_INF"))?;

    let table = normalize_table_row(stats_data_id, table_inf, meta_root);
    let bundle = normalize_dimensions(stats_data_id, class_inf);
    let observations = normalize_observations(
        stats_data_id,
        data_pages,
        &bundle.unit_lookup,
        &table.fetched_at,
    )?;

    Ok(NormalizedDataset {
        table,
        dimensions: bundle.rows,
        dimension_items: bundle.items,
        observations,
    })
}

fn normalize_table_row(stats_data_id: &str, table_inf: &Value, meta_root: &Value) -> TableRow {
    TableRow {
        stats_data_id: stats_data_id.to_string(),
        table_name: first_non_empty([
            text_at_path(table_inf, &["TITLE"]),
            text_at_path(table_inf, &["TITLE_SPEC", "TABLE_NAME"]),
        ]),
        stat_code: code_at_path(table_inf, &["STAT_NAME"]),
        stat_name: text_at_path(table_inf, &["STAT_NAME"]),
        gov_org_code: code_at_path(table_inf, &["GOV_ORG"]),
        gov_org_name: text_at_path(table_inf, &["GOV_ORG"]),
        survey_date: text_at_path(table_inf, &["SURVEY_DATE"]),
        open_date: text_at_path(table_inf, &["OPEN_DATE"]),
        small_area: text_at_path(table_inf, &["SMALL_AREA"]),
        collect_area: code_or_text(value_at_path(table_inf, &["COLLECT_AREA"])),
        main_category_code: code_at_path(table_inf, &["MAIN_CATEGORY"]),
        sub_category_code: code_at_path(table_inf, &["SUB_CATEGORY"]),
        link: format!("https://www.e-stat.go.jp/dbview?sid={}", stats_data_id),
        fetched_at: result_date(meta_root),
    }
}

#[derive(Debug)]
struct DimensionBundle {
    rows: Vec<DimensionRow>,
    items: Vec<DimensionItemRow>,
    unit_lookup: HashMap<(String, String), String>,
}

fn normalize_dimensions(stats_data_id: &str, class_inf: &Value) -> DimensionBundle {
    let mut rows = Vec::new();
    let mut items = Vec::new();
    let mut unit_lookup = HashMap::new();

    for (dimension_index, class_obj) in array_like(class_inf.get("CLASS_OBJ"))
        .into_iter()
        .enumerate()
    {
        let dimension_id = attr_text(Some(class_obj), "@id");
        let dimension_name = first_non_empty([
            attr_text(Some(class_obj), "@name"),
            text_value(Some(class_obj)),
        ]);
        let class_values = array_like(class_obj.get("CLASS"));
        let explanations = explanation_lookup(class_obj.get("EXPLANATION"));
        let classification_level = join_non_empty_unique(
            class_values
                .iter()
                .map(|class_value| attr_text(Some(class_value), "@level"))
                .collect::<Vec<_>>(),
            "|",
        );

        rows.push(DimensionRow {
            stats_data_id: stats_data_id.to_string(),
            dimension_id: dimension_id.clone(),
            dimension_name,
            classification_level,
            is_time: dimension_id.eq_ignore_ascii_case("time"),
            is_area: dimension_id.eq_ignore_ascii_case("area"),
            is_tab: dimension_id.eq_ignore_ascii_case("tab"),
            source_order: dimension_index + 1,
        });

        for (item_index, class_value) in class_values.into_iter().enumerate() {
            let item_code = attr_text(Some(class_value), "@code");
            let unit = attr_text(Some(class_value), "@unit");
            let note = first_non_empty([
                explanations.get(&item_code).cloned().unwrap_or_default(),
                attr_text_with_fallbacks(Some(class_value), &["@addInf", "@annotation"]),
            ]);
            if !dimension_id.is_empty() && !item_code.is_empty() && !unit.is_empty() {
                unit_lookup.insert(
                    (dimension_id.to_ascii_lowercase(), item_code.clone()),
                    unit.clone(),
                );
            }

            items.push(DimensionItemRow {
                stats_data_id: stats_data_id.to_string(),
                dimension_id: dimension_id.clone(),
                item_code,
                item_name: first_non_empty([
                    attr_text(Some(class_value), "@name"),
                    text_value(Some(class_value)),
                ]),
                level: attr_text(Some(class_value), "@level"),
                parent_code: attr_text_with_fallbacks(
                    Some(class_value),
                    &["@parentCode", "@parent_code"],
                ),
                unit,
                note,
                source_order: item_index + 1,
            });
        }
    }

    DimensionBundle {
        rows,
        items,
        unit_lookup,
    }
}

fn normalize_observations(
    stats_data_id: &str,
    data_pages: &[Value],
    unit_lookup: &HashMap<(String, String), String>,
    default_fetched_at: &str,
) -> Result<Vec<ObservationRow>> {
    let mut rows = Vec::new();

    for page in data_pages {
        let root = page
            .get("GET_STATS_DATA")
            .ok_or_else(|| anyhow!("missing GET_STATS_DATA root"))?;
        let statistical_data = root
            .get("STATISTICAL_DATA")
            .ok_or_else(|| anyhow!("missing GET_STATS_DATA.STATISTICAL_DATA"))?;
        let Some(data_inf) = statistical_data.get("DATA_INF") else {
            continue;
        };
        let annotations = code_text_lookup(data_inf.get("ANNOTATION"), &["@code", "@annotation"]);
        let notes = code_text_lookup(data_inf.get("NOTE"), &["@char", "@code", "@symbol"]);
        let fetched_at =
            non_empty(result_date(root)).unwrap_or_else(|| default_fetched_at.to_string());

        for value in array_like(data_inf.get("VALUE")) {
            let value_text = text_value(Some(value));
            let value_number = numeric_value_text(&value_text);
            let annotation = observation_annotation(value, &value_text, &annotations, &notes);

            rows.push(ObservationRow {
                stats_data_id: stats_data_id.to_string(),
                value: value_number,
                value_text: value_text.clone(),
                annotation,
                unit: first_non_empty([
                    attr_text(Some(value), "@unit"),
                    infer_observation_unit(value, unit_lookup),
                ]),
                time_code: attr_text(Some(value), "@time"),
                area_code: attr_text(Some(value), "@area"),
                tab_code: attr_text(Some(value), "@tab"),
                cat01_code: attr_text(Some(value), "@cat01"),
                cat02_code: attr_text(Some(value), "@cat02"),
                cat03_code: attr_text(Some(value), "@cat03"),
                cat04_code: attr_text(Some(value), "@cat04"),
                cat05_code: attr_text(Some(value), "@cat05"),
                cat06_code: attr_text(Some(value), "@cat06"),
                cat07_code: attr_text(Some(value), "@cat07"),
                cat08_code: attr_text(Some(value), "@cat08"),
                cat09_code: attr_text(Some(value), "@cat09"),
                cat10_code: attr_text(Some(value), "@cat10"),
                cat11_code: attr_text(Some(value), "@cat11"),
                cat12_code: attr_text(Some(value), "@cat12"),
                cat13_code: attr_text(Some(value), "@cat13"),
                cat14_code: attr_text(Some(value), "@cat14"),
                cat15_code: attr_text(Some(value), "@cat15"),
                fetched_at: fetched_at.clone(),
            });
        }
    }

    Ok(rows)
}

fn observation_annotation(
    value: &Value,
    value_text: &str,
    annotations: &HashMap<String, String>,
    notes: &HashMap<String, String>,
) -> String {
    let mut parts = Vec::new();

    let annotation_code = attr_text(Some(value), "@annotation");
    if !annotation_code.is_empty() {
        parts.push(
            annotations
                .get(&annotation_code)
                .cloned()
                .unwrap_or(annotation_code),
        );
    }

    if let Some(note_text) = notes.get(value_text) {
        parts.push(note_text.clone());
    }

    join_non_empty_unique(parts, " | ")
}

fn infer_observation_unit(
    value: &Value,
    unit_lookup: &HashMap<(String, String), String>,
) -> String {
    for dimension_id in [
        "tab", "cat01", "cat02", "cat03", "cat04", "cat05", "cat06", "cat07", "cat08", "cat09",
        "cat10", "cat11", "cat12", "cat13", "cat14", "cat15",
    ] {
        let code = attr_text(Some(value), &format!("@{}", dimension_id));
        if code.is_empty() {
            continue;
        }
        if let Some(unit) = unit_lookup.get(&(dimension_id.to_string(), code)) {
            return unit.clone();
        }
    }

    String::new()
}

fn ensure_unique_stats_data_ids(stats_data_ids: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    for stats_data_id in stats_data_ids {
        if !seen.insert(stats_data_id) {
            bail!("duplicate stats_data_id: {}", stats_data_id);
        }
    }
    Ok(())
}

fn ensure_no_conflicting_outputs(
    output_dir: &Path,
    stats_data_ids: &[String],
    raw_json: bool,
) -> Result<()> {
    for path in [
        output_dir.join("tables.csv"),
        output_dir.join("dimensions.csv"),
        output_dir.join("dimension_items.csv"),
        output_dir.join("manifest.json"),
    ] {
        if path.exists() {
            bail!(
                "output already exists: {} (use --overwrite or --resume)",
                path.display()
            );
        }
    }

    for stats_data_id in stats_data_ids {
        let observation = observation_path(output_dir, stats_data_id);
        if observation.exists() {
            bail!(
                "output already exists: {} (use --overwrite or --resume)",
                observation.display()
            );
        }
        if raw_json {
            for path in [
                raw_meta_path(output_dir, stats_data_id),
                raw_data_path(output_dir, stats_data_id),
            ] {
                if path.exists() {
                    bail!(
                        "output already exists: {} (use --overwrite or --resume)",
                        path.display()
                    );
                }
            }
        }
    }

    Ok(())
}

fn observation_path(output_dir: &Path, stats_data_id: &str) -> PathBuf {
    output_dir
        .join("observations")
        .join(format!("stats_data_id={}.csv", stats_data_id))
}

fn raw_meta_path(output_dir: &Path, stats_data_id: &str) -> PathBuf {
    output_dir
        .join("raw")
        .join("meta")
        .join(format!("{}.json", stats_data_id))
}

fn raw_data_path(output_dir: &Path, stats_data_id: &str) -> PathBuf {
    output_dir
        .join("raw")
        .join("data")
        .join(format!("{}.json", stats_data_id))
}

fn write_csv<T: Serialize>(path: impl AsRef<Path>, headers: &[&str], rows: &[T]) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(path)?;
    let mut writer = WriterBuilder::new().has_headers(false).from_writer(file);
    writer.write_record(headers)?;
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn write_json(path: impl AsRef<Path>, value: &impl Serialize) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(path)?;
    serde_json::to_writer_pretty(file, value)?;
    Ok(())
}

fn read_rows_by_stats_data_id<T>(path: &Path) -> Result<HashMap<String, Vec<T>>>
where
    T: for<'de> Deserialize<'de> + HasStatsDataId,
{
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let mut reader = ReaderBuilder::new().from_path(path)?;
    let mut rows = HashMap::<String, Vec<T>>::new();
    for row in reader.deserialize::<T>() {
        let row = row?;
        rows.entry(row.stats_data_id().to_string())
            .or_default()
            .push(row);
    }
    Ok(rows)
}

fn count_csv_rows(path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }

    let mut reader = ReaderBuilder::new().from_path(path)?;
    let mut count = 0usize;
    for record in reader.records() {
        record?;
        count += 1;
    }
    Ok(count)
}

fn relative_path(output_dir: &Path, path: &Path) -> String {
    path.strip_prefix(output_dir)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn result_date(root: &Value) -> String {
    value_at_path(root, &["RESULT", "DATE"])
        .and_then(scalar_text)
        .unwrap_or_default()
}

fn value_at_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn array_like(value: Option<&Value>) -> Vec<&Value> {
    match value {
        Some(Value::Array(items)) => items.iter().collect(),
        Some(Value::Null) | None => Vec::new(),
        Some(other) => vec![other],
    }
}

fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(boolean) => Some(boolean.to_string()),
        Value::Number(number) => Some(number.to_string()),
        Value::String(text) => Some(text.clone()),
        _ => None,
    }
}

fn text_value(value: Option<&Value>) -> String {
    value
        .and_then(|value| {
            value
                .get("$")
                .and_then(scalar_text)
                .or_else(|| scalar_text(value))
        })
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn attr_text(value: Option<&Value>, key: &str) -> String {
    value
        .and_then(|value| value.get(key))
        .and_then(scalar_text)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn attr_text_with_fallbacks(value: Option<&Value>, keys: &[&str]) -> String {
    keys.iter()
        .map(|key| attr_text(value, key))
        .find(|text| !text.is_empty())
        .unwrap_or_default()
}

fn text_at_path(value: &Value, path: &[&str]) -> String {
    text_value(value_at_path(value, path))
}

fn code_at_path(value: &Value, path: &[&str]) -> String {
    attr_text(value_at_path(value, path), "@code")
}

fn code_or_text(value: Option<&Value>) -> String {
    first_non_empty([attr_text(value, "@code"), text_value(value)])
}

fn explanation_lookup(value: Option<&Value>) -> HashMap<String, String> {
    code_text_lookup(value, &["@id", "@code"])
}

fn code_text_lookup(value: Option<&Value>, code_keys: &[&str]) -> HashMap<String, String> {
    let mut lookup = HashMap::new();
    for item in array_like(value) {
        let code = code_keys
            .iter()
            .map(|key| attr_text(Some(item), key))
            .find(|text| !text.is_empty())
            .unwrap_or_default();
        let text = text_value(Some(item));
        if !code.is_empty() && !text.is_empty() {
            lookup.insert(code, text);
        }
    }
    lookup
}

fn join_non_empty_unique<I>(values: I, separator: &str) -> String
where
    I: IntoIterator<Item = String>,
{
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            ordered.push(trimmed.to_string());
        }
    }
    ordered.join(separator)
}

fn first_non_empty<const N: usize>(values: [String; N]) -> String {
    values
        .into_iter()
        .find(|value| !value.trim().is_empty())
        .unwrap_or_default()
}

fn non_empty(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn numeric_value_text(value_text: &str) -> String {
    let trimmed = value_text.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.parse::<i64>().is_ok() || trimmed.parse::<f64>().is_ok() {
        trimmed.to_string()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_json_snapshot;

    #[test]
    fn normalizes_singleton_dimension_objects() {
        let meta: Value =
            serde_json::from_str(include_str!("../tests/fixtures/db_csv/meta_singleton.json"))
                .unwrap();
        let class_inf = meta["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"].clone();
        let bundle = normalize_dimensions("0000000001", &class_inf);

        assert_json_snapshot!(
            "normalize_singleton_dimension_objects",
            serde_json::json!({
                "dimensions": bundle.rows,
                "dimension_items": bundle.items,
            })
        );
    }

    #[test]
    fn extracts_code_and_text_fields_for_table_rows() {
        let meta: Value =
            serde_json::from_str(include_str!("../tests/fixtures/db_csv/meta_singleton.json"))
                .unwrap();
        let table = normalize_table_row(
            "0000000001",
            &meta["GET_META_INFO"]["METADATA_INF"]["TABLE_INF"],
            &meta["GET_META_INFO"],
        );

        assert_json_snapshot!("extracts_code_and_text_fields_for_table_rows", table);
    }

    #[test]
    fn normalizes_observations_with_missing_optional_dimensions() {
        let data: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/db_csv/data_missing_optional_dimensions.json"
        ))
        .unwrap();
        let rows = normalize_observations("0000000001", &[data], &HashMap::new(), "")
            .expect("observations should normalize");

        assert_json_snapshot!(
            "normalizes_observations_with_missing_optional_dimensions",
            rows
        );
    }
}
