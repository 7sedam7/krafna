use std::env;
use std::error::Error;

use krafna::libs::executor::execute_query;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Usage: krafna <query> [--from <from_part>]");
        return Ok(());
    }

    let query = args[1].clone();
    let mut from_query = None;

    let mut i = 2;
    while i < args.len() {
        if args[i] == "--from" {
            if i + 1 < args.len() {
                from_query = Some(args[i + 1].clone());
                i += 2;
                continue;
            } else {
                println!("Error: --from requires a value");
                return Ok(());
            }
        } else {
            println!("Error: Invalid argument '{}'", args[i]);
            println!("Usage: krafna <query> [--from <from_part>]");
            return Ok(());
        }
    }

    match execute_query(query, from_query) {
        Ok(res) => {
            println!("Result:");
            for element in res {
                println!("{}", element);
            }
        }
        Err(error) => println!("Error: {}", error),
    }

    Ok(())
}
