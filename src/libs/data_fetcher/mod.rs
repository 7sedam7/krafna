pub mod markdown_fetcher;
pub mod pod;

// Re-export important items from submodules
//pub use data_fetcher::fetch_data;

use std::error::Error;

use crate::libs::data_fetcher::pod::Pod;
use crate::libs::parser::Function;

pub fn fetch_data(from_function: &Function) -> Result<Vec<Pod>, Box<dyn Error>> {
    match from_function.name.to_uppercase().as_str() {
        "FRONTMATTER_DATA" => markdown_fetcher::fetch_frontmatter_data(&from_function.args),
        _ => Err(format!("Unknown function: {}", from_function.name).into()),
    }
}
