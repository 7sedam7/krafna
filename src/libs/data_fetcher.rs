use gray_matter::engine::YAML;
use gray_matter::{Matter, Pod};
use rayon::prelude::*;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use walkdir::WalkDir;

use super::{FieldValue, FunctionArg};
use crate::libs::parser::Function;

pub fn fetch_data(from_function: &Function) -> Result<Vec<(PathBuf, Pod)>, Box<dyn Error>> {
    match from_function.name.to_uppercase().as_str() {
        "FRONTMATTER_DATA" => fetch_frontmatter_data(&from_function.args),
        _ => Err(format!("Unknown function: {}", from_function.name).into()),
    }
}

fn fetch_frontmatter_data(args: &Vec<FunctionArg>) -> Result<Vec<(PathBuf, Pod)>, Box<dyn Error>> {
    if args.len() != 1 {
        return Err(format!(
            "Incorret amount of arguments, 1 String expected, but {} arguments found!",
            args.len()
        )
        .into());
    }
    let dir_path = match args.first() {
        Some(FunctionArg::FieldValue(FieldValue::String(str))) => str,
        _ => {
            return Err(format!("Expected a string argument, but found {:?}", args.first()).into())
        }
    };

    let files = get_markdown_files(&shellexpand::tilde(dir_path).into_owned())?;
    let frontmatters = read_frontmatter(files)?;

    Ok(frontmatters)
}

fn get_markdown_files(dir: &String) -> Result<Vec<PathBuf>, Box<dyn Error>> {
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
            result.data.map(|mut data| {
                data.insert(
                    "file_name".to_string(),
                    Pod::String(path.file_name().unwrap().to_string_lossy().into_owned()),
                );
                data.insert(
                    "file_path".to_string(),
                    Pod::String(path.display().to_string()),
                );
                (path.clone(), data)
            })
        })
        .collect();

    Ok(results)
}
