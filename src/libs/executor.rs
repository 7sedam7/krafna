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

    let result = execute_where(&query.where_expression, &frontmatter_data)?;

    //for (path, frontmatter) in frontmatter_data {
    for (path, frontmatter) in result {
        println!("File: {}", path.display());
        // println!("Frontmatter: {:#?}", frontmatter.as_vec()?);
        println!("Frontmatter: {:#?}", frontmatter.as_hashmap()?.get("tags"));
        println!("---");
    }

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
) -> Result<Vec<(PathBuf, Pod)>, String> {
    if condition.is_empty() {
        return Ok(data.clone());
    }

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

    if bool_stack.len() != 1 {
        return Err(format!(
            "Expected exactly one element in bool_stack, but found {}!",
            bool_stack.len()
        ));
    }

    let final_indexes = bool_stack.pop().unwrap();

    Ok(data
        .iter()
        .enumerate()
        .filter(|(index, _)| final_indexes.contains(index))
        .map(|(_, item)| item.clone())
        .collect())
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
        Operator::And => execute_bool_comparison_operation(
            |a, b| a.intersection(b).copied().collect(),
            left,
            right,
        ),
        Operator::Or => {
            execute_bool_comparison_operation(|a, b| a.union(b).copied().collect(), left, right)
        }

        // get values, return bools
        Operator::In => execute_val_comparison_operator(data, left, right, |a, b| b.contains(&a)),
        Operator::Lt => execute_val_comparison_operator(data, left, right, |a, b| a < b),
        Operator::Lte => execute_val_comparison_operator(data, left, right, |a, b| a <= b),
        Operator::Gt => execute_val_comparison_operator(data, left, right, |a, b| a > b),
        Operator::Gte => execute_val_comparison_operator(data, left, right, |a, b| a >= b),
        Operator::Eq => execute_val_comparison_operator(data, left, right, |a, b| a == b),
        Operator::Neq => execute_val_comparison_operator(data, left, right, |a, b| a != b),

        // get values, return values
        Operator::Plus => Err("PLUS operator not implemented!".to_string()),
        Operator::Minus => Err("MINUS operator not implemented!".to_string()),
        Operator::Multiply => Err("MULTIPLY operator not implemented!".to_string()),
        Operator::Divide => Err("DIVIDE operator not implemented!".to_string()),
        Operator::Power => Err("POWER operator not implemented!".to_string()),
        Operator::FloorDivide => Err("FLOOR DIVIDE operator not imlemented!".to_string()),
    }
}

fn execute_bool_comparison_operation(
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

fn execute_val_comparison_operator(
    data: &Vec<(PathBuf, Pod)>,
    left: &Operand,
    right: &Operand,
    op: fn(FieldValue, FieldValue) -> bool,
) -> Result<Operand, String> {
    let Operand::QueueElement(left_el) = left else {
        return Err("Operation AND expects operands to be BoolElement, LEFT was not!".to_string());
    };
    let Operand::QueueElement(right_el) = right else {
        return Err("Operation AND expects operands to be BoolElement, LEFT was not!".to_string());
    };

    let mut indexes: HashSet<usize> = HashSet::new();
    // TODO: fill the indexes mased on data that satisfies the condition
    for (index, (_, data_el)) in data.iter().enumerate() {
        let left_val = get_queue_element_value(left_el, data_el)?;
        let right_val = get_queue_element_value(right_el, data_el)?;

        if match (left_val, right_val) {
            (Some(a), Some(b)) => op(a, b),
            _ => false,
        } {
            indexes.insert(index);
        }
    }

    Ok(Operand::BoolElement(indexes))
}

fn get_queue_element_value(
    operand: &ExpressionElement,
    data: &Pod,
) -> Result<Option<FieldValue>, String> {
    match operand {
        ExpressionElement::FieldName(field_name) => {
            // TODO: add nested access with . (test.kifla.smurph)
            let data_el = data.as_hashmap().map_err(|e| e.to_string())?;
            if let Some(field_value) = data_el.get(field_name) {
                match field_value {
                    Pod::Null => Ok(None),
                    Pod::String(str) => Ok(Some(FieldValue::String(str.clone()))),
                    Pod::Float(num) => Ok(Some(FieldValue::Number(*num))),
                    Pod::Integer(num) => Ok(Some(FieldValue::Number(*num as f64))),
                    Pod::Boolean(bool) => Ok(Some(FieldValue::Bool(*bool))),
                    Pod::Array(list) => Ok(Some(pod_array_to_field_value(list))),
                    _ => Ok(None),
                }
            } else {
                Ok(None)
            }
        }
        ExpressionElement::FieldValue(field_value) => Ok(Some(field_value.clone())),
        ExpressionElement::Function(func) => Err("TODO: Implement function execution!".to_string()),
        _ => Err(format!("Unsupported element: {:?}!", operand)),
    }
}

fn pod_array_to_field_value(list: &Vec<Pod>) -> FieldValue {
    let mut fv_list = Vec::new();

    for el in list {
        match el {
            Pod::String(str) => fv_list.push(FieldValue::String(str.clone())),
            Pod::Float(num) => fv_list.push(FieldValue::Number(*num)),
            Pod::Integer(num) => fv_list.push(FieldValue::Number(*num as f64)),
            Pod::Boolean(bool) => fv_list.push(FieldValue::Bool(*bool)),
            _ => {}
        }
    }

    FieldValue::List(fv_list)
}
