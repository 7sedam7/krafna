use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
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

    let mdf_files_info = get_markdown_files_info(dir_path)?;

    Ok(mdf_files_info
        .into_values()
        .map(|mdf_info| mdf_info.frontmatter)
        .collect())
}

pub fn fetch_markdown_links(args: &[FunctionArg]) -> Result<Vec<Pod>, Box<dyn Error>> {
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

    let mdf_files_info = get_markdown_files_info(dir_path)?;

    Ok(mdf_files_info
        .into_values()
        .flat_map(|mdf_info| mdf_info.links)
        .collect())
}

pub fn fetch_markdown_tasks(args: &[FunctionArg]) -> Result<Vec<Pod>, Box<dyn Error>> {
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

    let mdf_files_info = get_markdown_files_info(dir_path)?;

    Ok(mdf_files_info
        .into_values()
        .flat_map(|mdf_info| mdf_info.tasks)
        .collect())
}

pub fn fetch_code_snippets(
    dir_path: &String,
    _lang: String,
) -> Result<Vec<String>, Box<dyn Error>> {
    let mdf_files_info = get_markdown_files_info(dir_path)?;

    Ok(mdf_files_info
        .into_values()
        .flat_map(|mdf_info| mdf_info.code_blocks)
        .collect())
}

fn get_markdown_files_info(
    dir_path: &String,
) -> Result<HashMap<String, MarkdownFileInfo>, Box<dyn Error>> {
    let files = get_markdown_files(&shellexpand::tilde(dir_path).into_owned())?;
    // TODO: add cashing here
    parse_files(files)
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
        .filter_map(|path| {
            let mdf_info = parse_file(path, &matter).ok()?;
            Some((path.display().to_string(), mdf_info))
        })
        .collect();

    // TODO: adjust interpreted links

    Ok(results)
}

fn parse_file(path: &PathBuf, matter: &Matter<YAML>) -> Result<MarkdownFileInfo, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;

    // Extract frontmatter
    let result = matter.parse(&content);
    let mut frontmatter = result
        .data
        .as_ref()
        .map(gray_matter_pod_to_pod)
        .unwrap_or(Pod::new_hash());
    let markdown_content = result.content;

    let file_data = get_file_info(path);
    let _ = frontmatter.insert("file".to_string(), Pod::Hash(file_data.clone()));

    // Parse the rest of markdfown for title,code, links, and tasks
    let mut mdf_info = parse_markdown_content(&markdown_content, &file_data);
    // TODO: mdf_info.modified = file_data.get("modified").unwrap().to_string();
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
    let mut link_ord = 0;

    let mut in_task = false;
    let mut task_level = 0;
    let mut task_ord = Vec::new();
    let mut current_task = String::new();
    let mut task_checked = false;

    for event in parser {
        match event {
            // Title
            Event::Start(Tag::Heading {
                level,
                id: _,
                classes: _,
                attrs: _,
            }) if !title_complete => {
                if level == HeadingLevel::H1 {
                    in_title = true;
                }
            }
            Event::End(TagEnd::Heading(_)) if !title_complete => {
                if in_title {
                    mdf_info.title = title_text.clone();
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
                title: _,
                id: _,
            }) => {
                if link_type == pulldown_cmark::LinkType::Inline
                    || link_type == (pulldown_cmark::LinkType::WikiLink { has_pothole: false })
                {
                    in_link = true;
                    current_link.push_str(&url);
                }
            }
            Event::End(TagEnd::Link) => {
                in_link = false;
                link_ord += 1;

                mdf_info.links.push(prepare_link(
                    link_ord,
                    &current_link,
                    &current_link_text,
                    file_data,
                ));

                current_link.clear();
                current_link_text.clear();
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
    file_data: &HashMap<String, Pod>,
) -> Pod {
    let mut link_hm = HashMap::new();

    link_hm.insert("file".to_string(), Pod::Hash(file_data.clone()));
    link_hm.insert("ord".to_string(), Pod::Integer(link_ord as i64));
    link_hm.insert(
        "text".to_string(),
        Pod::String(current_link_text.to_owned()),
    );
    link_hm.insert("link".to_string(), Pod::String(current_link.to_owned()));
    // TODO: later to insert interpreted link
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
    task_hm.insert("text".to_string(), Pod::String(current_task.to_owned()));
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
