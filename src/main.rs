use std::error::Error;

use clap::Parser;

use krafna::libs::data_fetcher::fetch_code_snippets;
use krafna::libs::executor::execute_query;

#[derive(Parser, Debug)]
#[command(name = "krafna")]
#[command(about = "Obsidian `dataview` alternative.", long_about = None)]
struct Args {
    /// The query to execute
    query: Option<String>,

    /// From option in case you are implementing querying for specific FROM that you don't want to
    /// specify every time
    #[arg(long)]
    from: Option<String>,

    /// Find option to find all krafna snippets within a dir
    #[arg(long)]
    find: Option<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    match args.query {
        Some(query) => do_query(&query, args.from),
        None => {
            if let Some(find) = args.find {
                find_files(&find);
            } else {
                print_help();
            }
        }
    }

    Ok(())
}

fn do_query(query: &String, from: Option<String>) {
    match execute_query(query, from) {
        Ok(res) => {
            for element in res {
                println!("{}", element);
            }
        }
        Err(error) => eprintln!("Error: {}", error),
    }
}

fn find_files(dir: &String) {
    // TODO: replace with krafna
    match fetch_code_snippets(dir, "dataview".to_string()) {
        Ok(snippets) => println!("{:?}", snippets),
        Err(error) => eprintln!("{}", error),
    }
}

fn print_help() {
    println!("This does nothing, run `krafna --help` to see how to use the tool!");
}
