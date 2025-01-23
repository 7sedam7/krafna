use gray_matter::engine::YAML;
use gray_matter::{Matter, Pod};
use rayon::prelude::*;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use walkdir::WalkDir;

use crate::libs::parser::Query;
use crate::libs::PeekableDeque;

pub fn execute_query(query: String, from_query: Option<String>) -> Result<Vec<String>, String> {
    let mut query = match query.parse::<Query>() {
        Ok(q) => q,
        Err(error) => return Err(error),
    };

    if from_query.is_some() {
        let mut peekable_from_query: PeekableDeque<char> =
            PeekableDeque::from_iter(format!("FROM {}", from_query.unwrap()).chars());
        match Query::parse_from(&mut peekable_from_query) {
            Ok(from_function) => query.from_function = Some(from_function),
            Err(error) => {
                return Err(format!(
                    "Error parsing FROM: {}, Query: \"{}\"",
                    error,
                    peekable_from_query.display_state()
                ))
            }
        }
    }

    println!("Parsed query: {:?}", query);

    //let files = get_markdown_files(dir_path);
    //let frontmatters = read_frontmatter(files);

    // for (path, frontmatter) in frontmatters {
    //     println!("File: {}", path.display());
    //     println!("Frontmatter: {:#?}", frontmatter.as_vec()?);
    //     // println!("Frontmatter: {:#?}", frontmatter.as_hashmap()?.get("tags"));
    //     println!("---");
    // }

    Ok(vec![])
}

fn get_markdown_files(dir: &str) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut markdown_files = Vec::new();

    for entry in WalkDir::new(dir)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let path = entry.path();
            if let Some(extension) = path.extension() {
                if extension == "md" {
                    markdown_files.push(path.to_path_buf());
                }
            }
        }
    }

    Ok(markdown_files)
}

// Sequencial
// fn read_frontmatter(files: Vec<PathBuf>) -> Result<Vec<(PathBuf, Pod)>, Box<dyn Error>> {
//     let matter = Matter::<YAML>::new();
//     let mut results = Vec::new();
//
//     for path in files {
//         let content = fs::read_to_string(&path)?;
//         let result = matter.parse(&content);
//
//         if let Some(data) = result.data {
//             results.push((path, data));
//         }
//     }
//
//     Ok(results)
// }

//
fn read_frontmatter(files: Vec<PathBuf>) -> Result<Vec<(PathBuf, Pod)>, Box<dyn Error>> {
    let matter = Matter::<YAML>::new();

    // Convert to parallel iterator and collect results
    let results: Vec<(PathBuf, Pod)> = files
        .par_iter()
        .filter_map(|path| {
            let content = fs::read_to_string(path).ok()?;
            let result = matter.parse(&content);
            result.data.map(|data| (path.clone(), data))
        })
        .collect();

    Ok(results)
}
