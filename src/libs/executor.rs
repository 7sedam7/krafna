use std::error::Error;

use crate::libs::data_fetcher::fetch_data;
use crate::libs::parser::Query;
use crate::libs::PeekableDeque;

pub fn execute_query(
    query: String,
    from_query: Option<String>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let mut query = match query.parse::<Query>() {
        Ok(q) => q,
        Err(error) => return Err(error.into()),
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
                )
                .into())
            }
        }
    }

    println!("Parsed query: {:?}", query);
    let frontmatter_data = fetch_data(&query.from_function.unwrap())?;

    for (path, frontmatter) in frontmatter_data {
        println!("File: {}", path.display());
        // println!("Frontmatter: {:#?}", frontmatter.as_vec()?);
        println!("Frontmatter: {:#?}", frontmatter.as_hashmap()?.get("tags"));
        println!("---");
    }

    Ok(vec![])
}
