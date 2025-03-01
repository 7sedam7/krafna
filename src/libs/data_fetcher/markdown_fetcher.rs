use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use directories::ProjectDirs;
use gray_matter::{engine::YAML, Matter};
use pulldown_cmark::{Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::libs::data_fetcher::pod::Pod;
use crate::libs::parser::{FieldValue, FunctionArg};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct MarkdownFileInfo {
    modified: String,
    title: String,
    frontmatter: Pod,
    code_blocks: Vec<String>,
    links: Vec<Pod>,
    tasks: Vec<Pod>,
}

pub fn fetch_frontmatter_data(args: &[FunctionArg]) -> Result<Vec<Pod>, Box<dyn Error>> {
    let dir_path = validate_and_fetch_markdown_path_argument(args)?;
    let mdf_files_info = get_markdown_files_info(&dir_path)?;

    Ok(mdf_files_info
        .into_values()
        .map(|mdf_info| mdf_info.frontmatter)
        .collect())
}

pub fn fetch_markdown_links(args: &[FunctionArg]) -> Result<Vec<Pod>, Box<dyn Error>> {
    let dir_path = validate_and_fetch_markdown_path_argument(args)?;
    let mdf_files_info = get_markdown_files_info(&dir_path)?;

    Ok(mdf_files_info
        .into_values()
        .flat_map(|mdf_info| mdf_info.links)
        .collect())
}

pub fn fetch_markdown_tasks(args: &[FunctionArg]) -> Result<Vec<Pod>, Box<dyn Error>> {
    let dir_path = validate_and_fetch_markdown_path_argument(args)?;
    let mdf_files_info = get_markdown_files_info(&dir_path)?;

    Ok(mdf_files_info
        .into_values()
        .flat_map(|mdf_info| mdf_info.tasks)
        .collect())
}

pub fn validate_and_fetch_markdown_path_argument(
    args: &[FunctionArg],
) -> Result<String, Box<dyn Error>> {
    if args.len() != 1 {
        return Err(format!(
            "Incorret amount of arguments, 1 String expected, but {} arguments found!",
            args.len()
        )
        .into());
    }
    match args.first() {
        Some(FunctionArg::FieldValue(FieldValue::String(str))) => Ok(str.clone()),
        _ => Err(format!("Expected a string argument, but found {:?}", args.first()).into()),
    }
}

pub fn fetch_code_snippets(dir_path: &str, _lang: String) -> Result<Vec<String>, Box<dyn Error>> {
    let mdf_files_info = get_markdown_files_info(dir_path)?;

    Ok(mdf_files_info
        .into_values()
        .flat_map(|mdf_info| mdf_info.code_blocks)
        .collect())
}

fn get_markdown_files_info(
    dir_path: &str,
) -> Result<HashMap<String, MarkdownFileInfo>, Box<dyn Error>> {
    let files = get_markdown_files(&shellexpand::tilde(dir_path).into_owned())?;

    // Do caching of markdown files info
    let mut mdf_files_info = load_cache();
    if mdf_files_info.is_empty() {
        let mdf_info = parse_files(files)?;
        save_cache(&mdf_info);
        return Ok(mdf_info);
    }

    let file_paths: HashSet<String> = files
        .iter()
        .map(|path| path.display().to_string())
        .collect();
    // Filter out files that have not been modified
    let files_to_parse: Vec<PathBuf> = files
        .into_iter()
        .filter(|file_path| {
            let mdf_info = mdf_files_info.get(&file_path.display().to_string());
            if mdf_info.is_none() {
                return true;
            }
            let metadata = fs::metadata(file_path);
            match metadata {
                Ok(metadata) => {
                    if let Ok(modified_time) = metadata.modified() {
                        let modified = DateTime::<Utc>::from(modified_time).to_rfc3339();
                        return mdf_info.unwrap().modified < modified;
                    }
                    true
                }
                Err(_) => true,
            }
        })
        .collect();

    if !files_to_parse.is_empty() {
        let new_mdf_files_info = parse_files(files_to_parse)?;
        for (file_path, new_mdf_info) in new_mdf_files_info {
            mdf_files_info.insert(file_path, new_mdf_info);
        }
        save_cache(&mdf_files_info);
    }

    // Filter out files that are not in the requestd directory
    mdf_files_info.retain(|file_path, _| file_paths.contains(file_path));

    Ok(mdf_files_info)
}

static CACHE_FILE_PATH: &str = "markdown.cache";
fn get_cache_file_path() -> Result<PathBuf, Box<dyn Error>> {
    let cache_dir = ProjectDirs::from("com", "7sedam7", "krafna")
        .map(|proj_dirs| proj_dirs.cache_dir().to_path_buf())
        .ok_or("Could not determine cache directory")?;

    // Create the directory if it doesn't exist
    fs::create_dir_all(&cache_dir)?;

    Ok(cache_dir.join(CACHE_FILE_PATH))
}

fn save_cache(mdf_info: &HashMap<String, MarkdownFileInfo>) {
    let file_path = match get_cache_file_path() {
        Ok(path) => path,
        Err(_) => return,
    };
    let file = match File::create(file_path) {
        Ok(file) => file,
        Err(_) => return,
    };
    let mut writer = BufWriter::new(file);
    if bincode::serialize_into(&mut writer, &mdf_info).is_ok() {
        let _ = writer.flush(); // Ensure all data is written to disk
    }
}

fn load_cache() -> HashMap<String, MarkdownFileInfo> {
    let file_path = match get_cache_file_path() {
        Ok(path) => path,
        Err(e) => {
            eprintln!("[LOAD MD CACHE] Error getting file path: {}", e);
            return HashMap::new();
        }
    };
    let file = match File::open(file_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("[LOAD MD CACHE] Error opening a file: {}", e);
            return HashMap::new();
        }
    };
    let reader = BufReader::new(file);
    bincode::deserialize_from::<BufReader<File>, HashMap<String, MarkdownFileInfo>>(reader)
        .unwrap_or_else(|e| {
            eprintln!("[LOAD MD CACHE] Error deserializing: {}", e);
            HashMap::new()
        })
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

fn parse_files(files: Vec<PathBuf>) -> Result<HashMap<String, MarkdownFileInfo>, Box<dyn Error>> {
    let matter = Matter::<YAML>::new();

    // Convert to parallel iterator and collect results
    let results: HashMap<String, MarkdownFileInfo> = files
        .par_iter()
        //.iter()
        .filter_map(|path| {
            let mdf_info = parse_file(path, &matter).ok()?;
            Some((path.display().to_string(), mdf_info))
        })
        .collect();

    Ok(add_link_paths(results))
}

fn add_link_paths(
    mut results: HashMap<String, MarkdownFileInfo>,
) -> HashMap<String, MarkdownFileInfo> {
    // First collect all file paths for matching
    let mut file_paths = Vec::new();
    let mut titles = HashMap::new();
    for (file_path, mdf_info) in &results {
        file_paths.push(file_path.clone());
        titles.insert(mdf_info.title.clone(), file_path.clone());
    }

    // Process each markdown file info
    for info in results.values_mut() {
        // Process links in each file
        for link in &mut info.links {
            if let Pod::Hash(link_data) = link {
                // Only process non-external links
                if let Some(Pod::Boolean(external)) = link_data.get("external") {
                    if !external {
                        if let Some(Pod::String(link_value)) = link_data.get("url") {
                            // Find the best matching file path
                            let link_path = find_matching_path(link_value, &file_paths, &titles);

                            // Add the link_path to the link data
                            if let Some(path) = link_path {
                                link_data.insert("path".to_string(), Pod::String(path));
                            }
                        }
                    }
                }
            }
        }
    }

    results
}

fn find_matching_path(
    link: &str,
    file_paths: &[String],
    titles: &HashMap<String, String>,
) -> Option<String> {
    // Clean the link - handle wiki links, anchors and query params
    let cleaned_link = &link
        .split('#')
        .next()
        .unwrap_or("") // Remove anchor
        .split('?')
        .next()
        .unwrap_or("") // Remove query params
        .trim()
        .replace("%20", " ");

    if cleaned_link.is_empty() {
        return None;
    }

    // First try exact filename match
    let exact_matches: Vec<&String> = file_paths
        .iter()
        .filter(|path| {
            path.ends_with(cleaned_link) || path.ends_with(&format!("{}.md", cleaned_link))
        })
        .collect();

    if !exact_matches.is_empty() {
        // If multiple matches, prefer the one with the shortest path
        return exact_matches
            .into_iter()
            .min_by_key(|path| path.len())
            .cloned();
    }

    // Try partial match
    let partial_matches: Vec<&String> = file_paths
        .iter()
        .filter(|path| path.contains(cleaned_link))
        .collect();

    if !partial_matches.is_empty() {
        // If multiple matches, prefer the one with the shortest path
        return partial_matches
            .into_iter()
            .min_by_key(|path| path.len())
            .cloned();
    }

    // Exact title matches
    let mut title_matches: Vec<&String> = titles
        .iter()
        .filter(|(title, _)| title.eq_ignore_ascii_case(cleaned_link.trim()))
        .map(|(_, path)| path)
        .collect();

    if !title_matches.is_empty() {
        return title_matches
            .into_iter()
            .min_by_key(|path| path.len())
            .cloned();
    }

    // Dashes replaced title matches
    title_matches = titles
        .iter()
        .filter(|(title, _)| title.eq_ignore_ascii_case(cleaned_link.replace('-', " ").trim()))
        .map(|(_, path)| path)
        .collect();

    if !title_matches.is_empty() {
        return title_matches
            .into_iter()
            .min_by_key(|path| path.len())
            .cloned();
    }

    // Dashes and dots replaced title matches
    title_matches = titles
        .iter()
        .filter(|(title, _)| {
            title
                .replace('.', "")
                .eq_ignore_ascii_case(cleaned_link.replace('-', " ").trim())
        })
        .map(|(_, path)| path)
        .collect();

    if !title_matches.is_empty() {
        return title_matches
            .into_iter()
            .min_by_key(|path| path.len())
            .cloned();
    }

    None
}

fn parse_file(path: &PathBuf, matter: &Matter<YAML>) -> Result<MarkdownFileInfo, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;

    // Extract frontmatter
    let result = matter.parse(&content);
    let mut frontmatter = result
        .data
        .as_ref()
        .map(gray_matter_pod_to_pod)
        .unwrap_or_else(Pod::new_hash);
    let markdown_content = result.content;

    let file_data = get_file_info(path);
    let _ = frontmatter.insert("file".to_string(), Pod::Hash(file_data.clone()));

    // Parse the rest of markdfown for title,code, links, and tasks
    let mut mdf_info = parse_markdown_content(&markdown_content, &file_data);
    mdf_info.modified = match file_data.get("modified") {
        Some(modified_pod) => modified_pod.to_string(),
        None => "".to_string(),
    };
    mdf_info.frontmatter = frontmatter;

    Ok(mdf_info)
}

fn parse_markdown_content(
    markdown_content: &str,
    file_data: &HashMap<String, Pod>,
) -> MarkdownFileInfo {
    // Parse markdown for code blocks, links, and tasks
    let parser = Parser::new_ext(
        markdown_content,
        Options::ENABLE_TASKLISTS | Options::ENABLE_WIKILINKS,
    );

    let mut mdf_info = MarkdownFileInfo {
        modified: "".to_string(),
        title: "".to_string(),
        frontmatter: Pod::Null,
        code_blocks: vec![],
        links: vec![],
        tasks: vec![],
    };

    let mut in_title = false;
    let mut title_complete = false;
    let mut title_text = String::new();

    let mut in_code_block = false;
    let mut current_code = String::new();
    let mut current_code_lang = String::new();

    let mut in_link = false;
    let mut current_link = String::new();
    let mut current_link_text = String::new();
    let mut current_link_type = String::new();
    let mut link_ord = 0;

    let mut in_task = false;
    let mut task_level = 0;
    let mut task_ord = Vec::new();
    let mut current_task = String::new();
    let mut task_checked = false;

    for event in parser {
        match event {
            // Title
            Event::Start(Tag::Heading { level, .. }) if !title_complete => {
                if level == HeadingLevel::H1 {
                    in_title = true;
                }
            }
            Event::End(TagEnd::Heading(_)) if !title_complete => {
                if in_title {
                    mdf_info.title.clone_from(&title_text);
                    title_complete = true;
                }
                in_title = false;
                title_text.clear();
            }

            // Code blocks
            Event::Start(Tag::CodeBlock(kind)) => {
                in_code_block = true;
                if let pulldown_cmark::CodeBlockKind::Fenced(lang) = kind {
                    current_code_lang = lang.to_string();
                }
            }
            Event::End(TagEnd::CodeBlock) => {
                in_code_block = false;
                if current_code_lang == "krafna" {
                    mdf_info.code_blocks.push(
                        current_code
                            .chars()
                            .map(|c| if c == '\n' { ' ' } else { c })
                            .collect::<String>()
                            .trim()
                            .to_string(),
                    )
                }
                current_code.clear();
                current_code_lang.clear();
            }

            // Links
            Event::Start(Tag::Link {
                link_type,
                dest_url: url,
                ..
            }) => {
                if link_type == pulldown_cmark::LinkType::Inline
                    || link_type == (pulldown_cmark::LinkType::WikiLink { has_pothole: false })
                {
                    in_link = true;
                    current_link.push_str(&url);
                    current_link_type = match link_type {
                        pulldown_cmark::LinkType::Inline => "inline".to_string(),
                        pulldown_cmark::LinkType::WikiLink { .. } => "wiki".to_string(),
                        _ => "".to_string(),
                    };
                }
            }
            Event::End(TagEnd::Link) => {
                in_link = false;
                link_ord += 1;

                mdf_info.links.push(prepare_link(
                    link_ord,
                    &current_link,
                    &current_link_text,
                    &current_link_type,
                    file_data,
                ));

                current_link.clear();
                current_link_text.clear();
                current_link_type.clear();
            }

            // Tasks
            Event::Start(Tag::List(_)) => {
                if in_task {
                    mdf_info.tasks.push(prepare_task(
                        &current_task,
                        task_checked,
                        &task_ord,
                        file_data,
                    ));
                    current_task.clear();
                }
                task_level += 1;
                task_ord.push(0);
                in_task = false;
            }
            Event::Start(Tag::Item) => {}
            Event::End(TagEnd::Item) => {
                if in_task {
                    mdf_info.tasks.push(prepare_task(
                        &current_task,
                        task_checked,
                        &task_ord,
                        file_data,
                    ));
                    current_task.clear();
                }
                in_task = false;
            }
            Event::TaskListMarker(checked) => {
                task_checked = checked;
                in_task = true;
                task_ord[task_level - 1] += 1;
            }
            Event::End(TagEnd::List(_)) => {
                task_level -= 1;
                task_ord.pop();
                in_task = false;
            }

            // Text content for all
            Event::Text(text) => {
                if in_title {
                    title_text.push_str(&text);
                }
                if in_code_block {
                    current_code.push_str(&text);
                }
                if in_link {
                    current_link_text.push_str(&text);
                }
                if in_task {
                    if in_link {
                        current_task
                            .push_str(&format!("[{}]({})", current_link_text, current_link));
                    } else {
                        current_task.push_str(&text);
                    }
                }
            }

            _ => {}
        }
    }

    mdf_info
}

fn prepare_link(
    link_ord: usize,
    current_link: &str,
    current_link_text: &str,
    current_link_type: &str,
    file_data: &HashMap<String, Pod>,
) -> Pod {
    let mut link_hm = HashMap::new();

    link_hm.insert("file".to_string(), Pod::Hash(file_data.clone()));
    link_hm.insert("ord".to_string(), Pod::Integer(link_ord as i64));
    link_hm.insert(
        "text".to_string(),
        Pod::String(current_link_text.trim().to_owned()),
    );
    link_hm.insert(
        "url".to_string(),
        Pod::String(current_link.trim().to_owned()),
    );
    link_hm.insert(
        "type".to_string(),
        Pod::String(current_link_type.to_owned()),
    );
    link_hm.insert(
        "external".to_string(),
        Pod::Boolean(
            current_link.starts_with("http://")
                || current_link.starts_with("https://")
                || current_link.starts_with("//"),
        ),
    );

    Pod::Hash(link_hm)
}

fn prepare_task(
    current_task: &str,
    task_checked: bool,
    task_ord: &[usize],
    file_data: &HashMap<String, Pod>,
) -> Pod {
    let mut task_hm = HashMap::new();

    task_hm.insert("file".to_string(), Pod::Hash(file_data.clone()));
    task_hm.insert(
        "text".to_string(),
        Pod::String(current_task.trim().to_owned()),
    );
    task_hm.insert("checked".to_string(), Pod::Boolean(task_checked));

    let mut ords: Vec<String> = task_ord.iter().map(|n| n.to_string()).collect();
    task_hm.insert("ord".to_string(), Pod::String(ords.join(".")));

    ords.pop();
    if ords.is_empty() {
        task_hm.insert("parent".to_string(), Pod::Null);
    } else {
        task_hm.insert("parent".to_string(), Pod::String(ords.join(".")));
    }

    Pod::Hash(task_hm)
}

fn gray_matter_pod_to_pod(pod: &gray_matter::Pod) -> Pod {
    match pod {
        gray_matter::Pod::Null => Pod::Null,
        gray_matter::Pod::String(s) => Pod::String(s.clone()),
        gray_matter::Pod::Integer(i) => Pod::Integer(*i),
        gray_matter::Pod::Float(f) => Pod::Float(*f),
        gray_matter::Pod::Boolean(b) => Pod::Boolean(*b),
        gray_matter::Pod::Array(arr) => {
            Pod::Array(arr.iter().map(gray_matter_pod_to_pod).collect())
        }
        gray_matter::Pod::Hash(hm) => {
            let mut new_hm = HashMap::new();
            for (k, v) in hm {
                new_hm.insert(k.clone(), gray_matter_pod_to_pod(v));
            }
            Pod::Hash(new_hm)
        }
    }
}

fn get_file_info(path: &PathBuf) -> HashMap<String, Pod> {
    // NOTE: potential colision with file defined values
    let mut hash = HashMap::new();

    let _ = hash.insert(
        "name".to_string(),
        Pod::String(path.file_name().unwrap().to_string_lossy().into_owned()),
    );
    let _ = hash.insert("path".to_string(), Pod::String(path.display().to_string()));

    if let Ok(metadata) = fs::metadata(path) {
        if let Ok(created_time) = metadata.created() {
            let _ = hash.insert(
                "created".to_string(),
                Pod::String(DateTime::<Utc>::from(created_time).to_rfc3339()),
            );
        }
        if let Ok(modified_time) = metadata.modified() {
            let _ = hash.insert(
                "modified".to_string(),
                Pod::String(DateTime::<Utc>::from(modified_time).to_rfc3339()),
            );
        }
        if let Ok(accessed_time) = metadata.accessed() {
            let _ = hash.insert(
                "accessed".to_string(),
                Pod::String(DateTime::<Utc>::from(accessed_time).to_rfc3339()),
            );
        }
    }

    hash
}
