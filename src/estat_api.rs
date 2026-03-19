use anyhow::{Context, Result, anyhow, bail};
use reqwest::Client;
use serde_json::Value;
use url::Url;

const API_BASE_URL: &str = "https://api.e-stat.go.jp/rest/3.0/app/json";
const DEFAULT_PAGE_LIMIT: usize = 100_000;

#[derive(Clone, Debug)]
pub struct EStatApiClient {
    client: Client,
}

impl Default for EStatApiClient {
    fn default() -> Self {
        Self::new()
    }
}

impl EStatApiClient {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    pub async fn get_meta_info(&self, app_id: &str, stats_data_id: &str) -> Result<Value> {
        let params = vec![
            ("appId", app_id.to_string()),
            ("statsDataId", stats_data_id.to_string()),
            ("explanationGetFlg", "Y".to_string()),
        ];
        self.fetch_json("getMetaInfo", "GET_META_INFO", &params)
            .await
    }

    pub async fn get_stats_data_pages(
        &self,
        app_id: &str,
        stats_data_id: &str,
    ) -> Result<Vec<Value>> {
        let mut pages = Vec::new();
        let mut start_position: Option<String> = None;

        loop {
            let mut params = vec![
                ("appId", app_id.to_string()),
                ("statsDataId", stats_data_id.to_string()),
                ("metaGetFlg", "N".to_string()),
                ("cntGetFlg", "N".to_string()),
                ("limit", DEFAULT_PAGE_LIMIT.to_string()),
            ];
            if let Some(next) = start_position.as_ref() {
                params.push(("startPosition", next.clone()));
            }

            let page = self
                .fetch_json("getStatsData", "GET_STATS_DATA", &params)
                .await?;
            start_position = next_key(&page);
            pages.push(page);

            if start_position.is_none() {
                break;
            }
        }

        Ok(pages)
    }

    async fn fetch_json(
        &self,
        endpoint: &str,
        response_root_key: &str,
        params: &[(&str, String)],
    ) -> Result<Value> {
        let url = Url::parse_with_params(
            &format!("{}/{}", API_BASE_URL, endpoint),
            params.iter().map(|(key, value)| (*key, value.as_str())),
        )?;
        let response = self.client.get(url.clone()).send().await?;
        let status = response.status();
        let body = response.bytes().await?;

        if !status.is_success() {
            let preview = String::from_utf8_lossy(&body);
            bail!(
                "e-Stat API {} failed with HTTP {}: {}",
                url,
                status,
                preview.trim()
            );
        }

        let value: Value = serde_json::from_slice(&body)
            .with_context(|| format!("failed to parse e-Stat API response for {}", endpoint))?;
        ensure_api_success(&value, response_root_key)
            .with_context(|| format!("e-Stat API {} returned an error", endpoint))?;
        Ok(value)
    }
}

fn ensure_api_success(value: &Value, response_root_key: &str) -> Result<()> {
    let root = value
        .get(response_root_key)
        .ok_or_else(|| anyhow!("missing {} root object", response_root_key))?;
    let status = scalar_to_i64(root.get("RESULT").and_then(|result| result.get("STATUS")))
        .ok_or_else(|| anyhow!("missing API status"))?;

    if status >= 100 {
        let message = root
            .get("RESULT")
            .and_then(|result| result.get("ERROR_MSG"))
            .and_then(scalar_to_string)
            .unwrap_or_else(|| "unknown API error".to_string());
        bail!("status={} message={}", status, message);
    }

    Ok(())
}

fn next_key(value: &Value) -> Option<String> {
    value
        .get("GET_STATS_DATA")
        .and_then(|root| root.get("STATISTICAL_DATA"))
        .and_then(|statistical_data| statistical_data.get("RESULT_INF"))
        .and_then(|result_inf| result_inf.get("NEXT_KEY"))
        .and_then(scalar_to_string)
        .filter(|key| !key.trim().is_empty())
}

fn scalar_to_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    })
}

fn scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(boolean) => Some(boolean.to_string()),
        Value::Number(number) => Some(number.to_string()),
        Value::String(text) => Some(text.clone()),
        _ => None,
    }
}
