use std::error::Error;
use std::path::PathBuf;

use gray_matter::Pod;

use crate::libs::data_fetcher::fetch_data;
use crate::libs::parser::{ExpressionElement, FieldValue, Operator, Query};
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

    execute_where(&query.where_expression, &frontmatter_data);

    //for (path, frontmatter) in frontmatter_data {
    //    println!("File: {}", path.display());
    //    // println!("Frontmatter: {:#?}", frontmatter.as_vec()?);
    //    println!("Frontmatter: {:#?}", frontmatter.as_hashmap()?.get("tags"));
    //    println!("---");
    //}

    Ok(vec![])
}

fn execute_where(condition: &Vec<ExpressionElement>, data: &Vec<(PathBuf, Pod)>) {
    let queue = infix_to_postfix(condition);

    for element in condition {
        match element {
            ExpressionElement::OpenedBracket => {}
            ExpressionElement::ClosedBracket => {
                // TODO: Handle closing bracket
            }
            ExpressionElement::Operator(op) => {
                // TODO: Handle operator
                match op {
                    Operator::And => println!("AND operator"),
                    Operator::Or => println!("OR operator"),
                    Operator::In => println!("IN operator"),
                    Operator::Lt => println!("LESS THAN operator"),
                    Operator::Lte => println!("LESS THAN OR EQUAL operator"),
                    Operator::Gt => println!("GREATER THAN operator"),
                    Operator::Gte => println!("GREATER THAN OR EQUAL operator"),
                    Operator::Eq => println!("EQUAL operator"),
                    Operator::Neq => println!("NOT EQUAL operator"),
                    Operator::Plus => println!("PLUS operator"),
                    Operator::Minus => println!("MINUS operator"),
                    Operator::Multiply => println!("MULTIPLY operator"),
                    Operator::Divide => println!("DIVIDE operator"),
                    Operator::Power => println!("POWER operator"),
                    Operator::FloorDivide => println!("FLOOR DIVIDE operator"),
                }
            }
            ExpressionElement::FieldName(name) => {
                // Handle field name
                println!("Field name: {}", name);
            }
            ExpressionElement::FieldValue(value) => {
                // Handle field value
                match value {
                    FieldValue::String(s) => println!("String value: {}", s),
                    FieldValue::Number(n) => println!("Number value: {}", n),
                    FieldValue::Bool(b) => println!("Boolean value: {}", b),
                }
            }
            ExpressionElement::Function(func) => {
                // Handle function
                println!("Function: {} with {:?} args", func.name, func.args);
            }
        }
    }
}

fn infix_to_postfix(condition: &Vec<ExpressionElement>) -> Vec<ExpressionElement> {
    let mut stack = Vec::new();
    let mut queue = Vec::new();

    // Define operator precedence
    let operator_precedence = |op: &Operator| match op {
        Operator::And | Operator::Or => 1,
        Operator::Eq
        | Operator::Neq
        | Operator::Lt
        | Operator::Lte
        | Operator::Gt
        | Operator::Gte => 2,
        Operator::Plus | Operator::Minus => 3,
        Operator::Multiply | Operator::Divide | Operator::FloorDivide => 4,
        Operator::Power => 5,
        _ => 0,
    };

    for element in condition {
        match element {
            ExpressionElement::OpenedBracket => {
                stack.push(ExpressionElement::OpenedBracket);
            }
            ExpressionElement::FieldName(_)
            | ExpressionElement::FieldValue(_)
            | ExpressionElement::Function(_) => {
                queue.push(element.clone());
            }
            ExpressionElement::Operator(op) => {
                // op goes on stack unless op on stack has higher priority, then it goes to queue
                if let Some(ExpressionElement::Operator(last_op)) = stack.last() {
                    // compare and higher priority goes to queue
                    if operator_precedence(last_op) > operator_precedence(op) {
                        queue.push(stack.pop().unwrap());
                        stack.push(element.clone());
                    } else {
                        queue.push(element.clone());
                    }
                } else {
                    stack.push(element.clone());
                }
            }
            ExpressionElement::ClosedBracket => {
                while let Some(ExpressionElement::Operator(_)) = stack.last() {
                    queue.push(stack.pop().unwrap());
                }
                stack.pop();
            }
        }
    }

    queue
}
