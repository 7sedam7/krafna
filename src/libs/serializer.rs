use gray_matter::Pod;
use serde_json;

use crate::libs::executor::get_field_value;

pub fn pods_to_json(pods: Vec<Pod>) -> String {
    let json_values: Vec<serde_json::Value> = pods
        .into_iter()
        .filter_map(|pod| pod.deserialize::<serde_json::Value>().ok())
        .collect();

    serde_json::to_string(&json_values).unwrap_or_else(|_| "[]".to_string())
}

pub fn pods_to_tsv(pods: Vec<Pod>) -> String {
    if pods.is_empty() {
        return String::new();
    }

    // Get all unique field names from all pods
    let field_names: Vec<String> = pods[0]
        .as_hashmap()
        .ok()
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default();

    // Build header row
    let header = field_names.join("\t");

    // Build data rows
    let rows: Vec<String> = pods
        .into_iter()
        .map(|pod| {
            field_names
                .iter()
                .map(|field_name| {
                    if let Some(field_value) = get_field_value(field_name, &pod) {
                        field_value.to_string()
                    } else {
                        "".to_string()
                    }
                })
                .collect::<Vec<String>>()
                .join("\t")
        })
        .collect();

    // Combine header and rows
    format!("{}\n{}", header, rows.join("\n"))
}
