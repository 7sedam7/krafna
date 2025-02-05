use std::error::Error;
use std::usize;

use gray_matter::Pod;
use hashbrown::HashSet;
use regex::Regex;

use crate::libs::data_fetcher::fetch_data;
use crate::libs::parser::{ExpressionElement, FieldValue, Operator, OrderDirection, Query};
use crate::libs::PeekableDeque;

use super::parser::OrderByFieldOption;

pub fn execute_query(
    query: &String,
    select_fields: Option<Vec<String>>,
    include_fields: Vec<String>,
    from_query: Option<String>,
) -> Result<(Vec<String>, Vec<Pod>), Box<dyn Error>> {
    let mut query = match query.parse::<Query>() {
        Ok(q) => q,
        Err(error) => return Err(error.into()),
    };

    if let Some(select_fields) = select_fields {
        query.select_fields = select_fields;
    }
    query.select_fields.splice(0..0, include_fields);

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

    //println!("Parsed query: {:?}", query);
    // FROM
    let frontmatter_data = fetch_data(&query.from_function.unwrap())?;
    // WHERE
    let mut result = execute_where(&query.where_expression, &frontmatter_data)?;
    // ORDER BY
    execute_order_by(&query.order_by_fields, &mut result)?;
    // SELECT
    execute_select(&query.select_fields, &mut result);

    Ok((query.select_fields, result))
}

#[derive(Debug, PartialEq, Clone)]
enum Operand {
    QueueElement(ExpressionElement),
    BoolElement(HashSet<usize>),
}

fn execute_select(fields: &Vec<String>, data: &mut Vec<Pod>) {
    // TODO: implement * to select all values
    // TODO: implement function calls in select
    // TODO: implement AS in select
    let check_fields: Vec<String> = fields
        .iter()
        .map(|s| {
            s.split_once('.')
                .map(|(before, _)| before)
                .unwrap_or(s)
                .to_string()
        })
        .collect();

    for pod in data {
        if let Pod::Hash(ref mut hashmap) = *pod {
            hashmap.retain(|k, _| check_fields.contains(k));
        }
    }
}

fn execute_order_by(fields: &Vec<OrderByFieldOption>, data: &mut Vec<Pod>) -> Result<(), String> {
    data.sort_by(|a, b| {
        // do some stuff
        for orderby_field in fields {
            let a_mby = get_field_value(&orderby_field.field_name, a);
            let b_mby = get_field_value(&orderby_field.field_name, b);

            let comparison = match (a_mby, b_mby) {
                (None, _) => std::cmp::Ordering::Less,
                (_, None) => std::cmp::Ordering::Greater,
                (Some(a_val), Some(b_val)) => {
                    if let (FieldValue::String(a_str), FieldValue::String(b_str)) = (&a_val, &b_val)
                    {
                        // TODO: try to compare as dates
                        a_str.cmp(b_str)
                    } else {
                        a_val
                            .partial_cmp(&b_val)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    }
                }
            };

            if comparison.is_ne() {
                if orderby_field.order_direction == OrderDirection::ASC {
                    return comparison;
                } else if comparison.is_lt() {
                    return std::cmp::Ordering::Greater;
                } else {
                    return std::cmp::Ordering::Less;
                }
            }
        }

        std::cmp::Ordering::Equal
    });

    Ok(())
}

fn execute_where(condition: &Vec<ExpressionElement>, data: &Vec<Pod>) -> Result<Vec<Pod>, String> {
    if condition.is_empty() {
        return Ok(data.clone());
    }

    let mut stack = Vec::new();
    let mut eval_stack = Vec::new();
    let mut bool_stack = Vec::new();

    // Define operator precedence
    let operator_precedence = |op: &Operator| match op {
        Operator::And | Operator::Or => 1,
        Operator::In
        | Operator::Like
        | Operator::NotLike
        | Operator::Eq
        | Operator::Neq
        | Operator::Lt
        | Operator::Lte
        | Operator::Gt
        | Operator::Gte => 2,
        Operator::Plus | Operator::Minus => 3,
        Operator::Multiply | Operator::Divide | Operator::FloorDivide => 4,
        Operator::Power => 5,
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
    data: &Vec<Pod>,
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
                        let _right = bool_stack
                            .pop()
                            .ok_or("Expected left operand on the bool stack, but found nothing!")?;
                        let _left = bool_stack.pop().ok_or(
                            "Expected right operand on the bool stack, but found nothing!",
                        )?;
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
    data: &Vec<Pod>,
    op: &Operator,
    left: &Operand,
    right: &Operand,
) -> Result<Operand, String> {
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
        Operator::Like => {
            execute_val_comparison_operator(data, left, right, execute_operation_like)
        }
        Operator::NotLike => {
            execute_val_comparison_operator(data, left, right, |a, b| !execute_operation_like(a, b))
        }
        Operator::In => execute_val_comparison_operator(data, left, right, |a, b| b.contains(&a)),
        Operator::Lt => execute_val_comparison_operator(data, left, right, |a, b| a < b),
        Operator::Lte => execute_val_comparison_operator(data, left, right, |a, b| a <= b),
        Operator::Gt => execute_val_comparison_operator(data, left, right, |a, b| a > b),
        Operator::Gte => execute_val_comparison_operator(data, left, right, |a, b| a >= b),
        Operator::Eq => execute_val_comparison_operator(data, left, right, |a, b| a == b),
        Operator::Neq => execute_val_comparison_operator(data, left, right, |a, b| a != b),

        // get values, return values
        // TODO: Handle operator
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
    data: &Vec<Pod>,
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
    for (index, data_el) in data.iter().enumerate() {
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
        ExpressionElement::FieldName(field_name) => Ok(get_field_value(&field_name, data)),
        ExpressionElement::FieldValue(field_value) => Ok(Some(field_value.clone())),
        ExpressionElement::Function(_func) => {
            Err("TODO: Implement function execution!".to_string())
        }
        _ => Err(format!("Unsupported element: {:?}!", operand)),
    }
}

pub fn get_nested_pod(field_name: &String, data: &Pod) -> Option<Pod> {
    // TODO: think about field case insensitive comparisson (could convert to_lower when parsing
    // the data)
    let mut current = data.clone();
    for key in field_name.split('.') {
        match current.as_hashmap() {
            Ok(hash) => match hash.get(key) {
                Some(pod) => current = pod.clone(),
                None => return None,
            },
            Err(_) => return None,
        }
    }
    Some(current)
}

pub fn get_field_value(field_name: &String, data: &Pod) -> Option<FieldValue> {
    match get_nested_pod(field_name, data) {
        Some(Pod::Null) => None,
        Some(Pod::String(str)) => Some(FieldValue::String(str.clone())),
        Some(Pod::Float(num)) => Some(FieldValue::Number(num)),
        Some(Pod::Integer(num)) => Some(FieldValue::Number(num as f64)),
        Some(Pod::Boolean(bool)) => Some(FieldValue::Bool(bool)),
        Some(Pod::Array(list)) => Some(pod_array_to_field_value(&list)),
        Some(Pod::Hash(hash)) => match Pod::Hash(hash).deserialize::<serde_json::Value>() {
            Ok(val) => Some(FieldValue::String(val.to_string())),
            Err(_) => None,
        },
        None => None,
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

fn execute_operation_like(a: FieldValue, b: FieldValue) -> bool {
    match (a, b) {
        (FieldValue::String(a_str), FieldValue::String(b_str)) => {
            Regex::new(&b_str).map_or(false, |re| re.is_match(&a_str))
        }
        _ => false,
    }
}
