use gray_matter::engine::YAML;
use gray_matter::{Matter, Pod};
use rayon::prelude::*;
use std::error::Error;
use std::path::PathBuf;
use std::{env, fs};
use walkdir::WalkDir;

mod query_parser;
use query_parser::{QueryParser, QueryStatement};

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

fn main() -> Result<(), Box<dyn Error>> {
    //let args: Vec<String> = env::args().collect();
    //if args.len() < 2 {
    //    println!("Missing directory path!");
    //    return Ok(());
    //}
    //
    //let dir_path = &args[1];

    //if let Some(query) = env::args().nth(2) {
    let dir_path = "~/.dotfiles/";
    if let query = "select test from ( (#kifla or #space  ) and #mifla)".to_string() {
        let parser = QueryParser::new(query);

        match parser.parse() {
            Ok(statement) => println!("Parsed query statement: {:?}", statement),
            Err(e) => {
                eprintln!("Error parsing query: {}", e);
                std::process::exit(1);
            }
        }
    }

    println!("dp: {dir_path}");

    let files = get_markdown_files(dir_path)?;
    let frontmatters = read_frontmatter(files)?;

    // for (path, frontmatter) in frontmatters {
    //     println!("File: {}", path.display());
    //     println!("Frontmatter: {:#?}", frontmatter.as_vec()?);
    //     // println!("Frontmatter: {:#?}", frontmatter.as_hashmap()?.get("tags"));
    //     println!("---");
    // }

    Ok(())
}
