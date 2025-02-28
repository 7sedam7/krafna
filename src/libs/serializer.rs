use crate::libs::data_fetcher::pod::Pod;

pub fn pods_to_json(field_names: Vec<String>, pods: Vec<Pod>) -> String {
    let json_values: Vec<String> = pods
        .into_iter()
        .filter_map(|pod| {
            let mut hash = Pod::new_hash();
            for field_name in &field_names {
                if let Some(nested_pod) = pod.nested_get(field_name) {
                    let _ = hash.insert(field_name.clone(), nested_pod.clone());
                }
            }
            hash.to_untagged_json_string().ok()
        })
        .collect();

    format!("[{}]", json_values.join(","))
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
                .map(|field_name| {
                    pod.nested_get(field_name)
                        .map(Pod::to_string)
                        .unwrap_or_default()
                })
                .collect::<Vec<String>>()
                .join("\t")
        })
        .collect();

    // Combine header and rows
    format!("{}\n{}", header, rows.join("\n"))
}
