use gray_matter::Pod;
use serde_json;

use crate::libs::executor::{get_field_value, get_nested_pod};

pub fn pods_to_json(field_names: Vec<String>, pods: Vec<Pod>) -> String {
    let json_values: Vec<serde_json::Value> = pods
        .into_iter()
        .filter_map(|pod| {
            let mut hash = Pod::new_hash();
            for field_name in &field_names {
                if let Some(nested_pod) = get_nested_pod(field_name, &pod) {
                    let _ = hash.insert(field_name.clone(), nested_pod);
                }
            }
            hash.deserialize::<serde_json::Value>().ok()
        })
        .collect();

    serde_json::to_string(&json_values).unwrap_or_else(|_| "[]".to_string())
}

pub fn pods_to_tsv(field_names: Vec<String>, pods: Vec<Pod>) -> String {
    if pods.is_empty() {
        return String::new();
    }

    // Build header row
    let header = field_names
        .iter()
        .map(|s| s.replace('.', "_"))
        .collect::<Vec<String>>()
        .join("\t");

    // Build data rows
    let rows: Vec<String> = pods
        .into_iter()
        .map(|pod| {
            field_names
                .iter()
                .map(|field_name| get_field_value(field_name, &pod).to_string())
                .collect::<Vec<String>>()
                .join("\t")
        })
        .collect();

    // Combine header and rows
    format!("{}\n{}", header, rows.join("\n"))
}
