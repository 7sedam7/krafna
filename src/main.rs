use std::error::Error;

use clap::{Parser, ValueHint};

use krafna::libs::data_fetcher::markdown_fetcher::fetch_code_snippets;
use krafna::libs::executor::execute_query;
use krafna::libs::serializer::{pods_to_json, pods_to_tsv};

#[derive(Parser, Debug)]
#[command(name = "krafna")]
#[command(about = "Obsidian `dataview` alternative.", long_about = None)]
struct Args {
    /// The query to execute
    #[arg(value_hint = ValueHint::Other)]
    query: Option<String>,

    /// OVERRIDES SELECT fields with "field1,field2"
    #[arg(long)]
    select: Option<String>,

    /// From option in case you are implementing querying for specific FROM that you don't want to
    /// specify every time. This OVERRIDES the FROM part of the query!
    #[arg(long, value_hint = ValueHint::Other)]
    from: Option<String>,

    /// include SELECT fields with "field1,field2"
    #[arg(long)]
    include_fields: Option<String>,

    /// Find option to find all krafna snippets within a dir
    #[arg(long, value_hint = ValueHint::DirPath)]
    find: Option<String>,

    /// Output results in JSON format
    #[arg(long)]
    json: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    match args.query {
        Some(query) => do_query(
            &query,
            args.select,
            args.from,
            args.include_fields,
            args.json,
        ),
        None => {
            if let Some(find) = args.find {
                find_files(&find, args.json);
            } else {
                print_help();
            }
        }
    }

    Ok(())
}

fn do_query(
    query: &str,
    select_fields: Option<String>,
    from: Option<String>,
    include_fields: Option<String>,
    to_json: bool,
) {
    match execute_query(query, select_fields, from, include_fields) {
        Ok((fields, res)) => {
            if to_json {
                let json = pods_to_json(fields, res);
                println!("{}", json);
            } else {
                let tsv = pods_to_tsv(fields, res);
                println!("{}", tsv);
            }
        }
        Err(error) => eprintln!("Error: {}", error),
    }
}

fn find_files(dir: &String, to_json: bool) {
    match fetch_code_snippets(dir, "krafna".to_string()) {
        Ok(snippets) => {
            if to_json {
                println!(
                    "{}",
                    serde_json::to_string(&snippets).unwrap_or_else(|_| "[]".to_string())
                );
            } else {
                println!("{}", snippets.join("\n"));
            }
        }
        Err(error) => eprintln!("{}", error),
    }
}

fn print_help() {
    println!("This does nothing, run `krafna --help` to see how to use the tool!");
}
