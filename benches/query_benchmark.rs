use std::{fs, panic, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use rayon::prelude::*;

use krafna::libs::data_fetcher::fetch_code_snippets;
use krafna::libs::executor::execute_query;

const NUMBER_OF_FILES: u32 = 2500;
const PATH_TO_FILES: &str = "benches/bench";
fn setup() -> Result<(), String> {
    let content_bytes = fs::read("benches/example.md").map_err(|_| "File should exist")?;
    let content_arc = Arc::new(content_bytes);

    let content_with_query_bytes =
        fs::read("benches/example_with_query.md").map_err(|_| "File should exist")?;
    let content_with_query_arc = Arc::new(content_with_query_bytes);

    fs::create_dir_all(PATH_TO_FILES).map_err(|_| "Unable to create directory")?;

    panic::catch_unwind(|| {
        (0..NUMBER_OF_FILES)
            .into_par_iter() // Convert to parallel iterator
            .for_each(|i| {
                let file_content = if i % 10 == 0 {
                    content_with_query_arc.as_ref()
                } else {
                    content_arc.as_ref()
                };
                fs::write(format!("{}/file{}.md", PATH_TO_FILES, i), file_content)
                    .expect("Unable to write file");
            })
    })
    .map_err(|_| "Unable to write files")?;

    Ok(())
}

fn teardown() -> Result<(), String> {
    fs::remove_dir_all(PATH_TO_FILES).map_err(|_| "Unable to remove directory")?;
    Ok(())
}

fn benchmark_do_query(c: &mut Criterion) {
    setup().expect("Setup failed");

    c.bench_function("query execution", |b| {
        b.iter(|| execute_query("select file.name, tags from frontmatter_data(\"benches/bench/\") where \"example\" in tags", None, None, None))
    });

    let dir = PATH_TO_FILES.to_string();
    c.bench_function("query finding", |b| {
        b.iter(|| fetch_code_snippets(&dir, "krafna".to_string()))
    });

    teardown().expect("Teardown failed");
}

criterion_group!(benches, benchmark_do_query);
criterion_main!(benches);
