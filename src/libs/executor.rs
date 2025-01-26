use std::error::Error;
use std::path::PathBuf;
use std::usize;

use gray_matter::Pod;
use hashbrown::HashSet;

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

    execute_where(&query.where_expression, &frontmatter_data)?;

    //for (path, frontmatter) in frontmatter_data {
    //    println!("File: {}", path.display());
    //    // println!("Frontmatter: {:#?}", frontmatter.as_vec()?);
    //    println!("Frontmatter: {:#?}", frontmatter.as_hashmap()?.get("tags"));
    //    println!("---");
    //}

    Ok(vec![])
}

#[derive(Debug, PartialEq, Clone)]
enum Operand {
    QueueElement(ExpressionElement),
    BoolElement(HashSet<usize>),
}

fn execute_where(
    condition: &Vec<ExpressionElement>,
    data: &Vec<(PathBuf, Pod)>,
) -> Result<(), String> {
    let mut stack = Vec::new();
    let mut eval_stack = Vec::new();
    let mut bool_stack = Vec::new();

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
                eval_stack.push(element.clone());
            }
            ExpressionElement::Operator(op) => {
                // op goes on stack, but if stack has equal or higher priority operator, that one
                // goes from stack to the "queue"
                if let Some(ExpressionElement::Operator(last_op)) = stack.last() {
                    if operator_precedence(last_op) >= operator_precedence(op) {
                        handle_operator_to_queue(
                            &mut stack,
                            &mut eval_stack,
                            &mut bool_stack,
                            data,
                        )?;
                    }
                }
                stack.push(element.clone());
            }
            ExpressionElement::ClosedBracket => {
                while let Some(ExpressionElement::Operator(_)) = stack.last() {
                    handle_operator_to_queue(&mut stack, &mut eval_stack, &mut bool_stack, data)?;
                }
                stack.pop();
            }
        }
    }
    while stack.last().is_some() {
        handle_operator_to_queue(&mut stack, &mut eval_stack, &mut bool_stack, data)?;
    }

    Ok(())
}

fn handle_operator_to_queue(
    stack: &mut Vec<ExpressionElement>,
    eval_stack: &mut Vec<ExpressionElement>,
    bool_stack: &mut Vec<HashSet<usize>>,
    data: &Vec<(PathBuf, Pod)>,
) -> Result<(), String> {
    let op;
    let left;
    let right;

    if let Some(should_be_operator) = stack.pop() {
        match should_be_operator {
            ExpressionElement::Operator(_op) => {
                op = _op.clone();

                match _op {
                    Operator::Or | Operator::And => {
                        let _right = bool_stack.pop().ok_or(
                            "Expected operand on the bool stack, but found nothing!".to_string(),
                        )?;
                        let _left = bool_stack
                            .pop()
                            .ok_or("Expected operand on the bool stack, but found nothing!")?;
                        left = Operand::BoolElement(_left);
                        right = Operand::BoolElement(_right);
                    }
                    _ => {
                        let _right = eval_stack
                            .pop()
                            .ok_or("Expected operand on the eval stack, but found nothing!")?;
                        let _left = eval_stack
                            .pop()
                            .ok_or("Expected operand on the eval stack, but found nothing!")?;
                        left = Operand::QueueElement(_left);
                        right = Operand::QueueElement(_right);
                    }
                }
            }
            _ => {
                return Err(format!(
                    "Expected operator, but found: {:?}",
                    should_be_operator
                ))
            }
        }
    } else {
        return Err("Expected operator on top of the stack, but found nothing!".to_string());
    }

    match execute_operation(data, &op, &left, &right)? {
        Operand::BoolElement(set) => {
            bool_stack.push(set);
        }
        Operand::QueueElement(value) => {
            eval_stack.push(value);
        }
    }

    Ok(())
}

fn execute_operation(
    data: &Vec<(PathBuf, Pod)>,
    op: &Operator,
    left: &Operand,
    right: &Operand,
) -> Result<Operand, String> {
    // TODO: Handle operator
    match op {
        // get bools, return bool
        Operator::And => {
            execute_bool_operation(|a, b| a.intersection(b).copied().collect(), left, right)
        }
        Operator::Or => execute_bool_operation(|a, b| a.union(b).copied().collect(), left, right),

        // get values, return bools
        Operator::In => Err("IN operator not implemented!".to_string()),
        Operator::Lt => Err("LESS THAN operator not implemented!".to_string()),
        Operator::Lte => Err("LESS THAN OR EQUAL operator not implemented!".to_string()),
        Operator::Gt => Err("GREATER THAN operator not implemented!".to_string()),
        Operator::Gte => Err("GREATER THAN OR EQUAL operator not implemented!".to_string()),
        Operator::Eq => execute_operator_eq(data, left, right),
        Operator::Neq => Err("NOT EQUAL operator not implemented!".to_string()),

        // get values, return values
        Operator::Plus => Err("PLUS operator not implemented!".to_string()),
        Operator::Minus => Err("MINUS operator not implemented!".to_string()),
        Operator::Multiply => Err("MULTIPLY operator not implemented!".to_string()),
        Operator::Divide => Err("DIVIDE operator not implemented!".to_string()),
        Operator::Power => Err("POWER operator not implemented!".to_string()),
        Operator::FloorDivide => Err("FLOOR DIVIDE operator not imlemented!".to_string()),
    }
}

fn execute_bool_operation(
    op: fn(&HashSet<usize>, &HashSet<usize>) -> HashSet<usize>,
    left: &Operand,
    right: &Operand,
) -> Result<Operand, String> {
    let Operand::BoolElement(left_set) = left else {
        return Err("Operation AND expects operands to be BoolElement, LEFT was not!".to_string());
    };
    let Operand::BoolElement(right_set) = right else {
        return Err("Operation AND expects operands to be BoolElement, LEFT was not!".to_string());
    };

    Ok(Operand::BoolElement(op(left_set, right_set)))
}

fn execute_operator_eq(
    data: &Vec<(PathBuf, Pod)>,
    left: &Operand,
    right: &Operand,
) -> Result<Operand, String> {
    let Operand::QueueElement(left_val) = left else {
        return Err("Operation AND expects operands to be BoolElement, LEFT was not!".to_string());
    };
    let Operand::QueueElement(right_val) = right else {
        return Err("Operation AND expects operands to be BoolElement, LEFT was not!".to_string());
    };

    let indexes: HashSet<usize> = HashSet::new();
    // TODO: fill the indexes mased on data that satisfies the condition

    Ok(Operand::BoolElement(indexes))
}
