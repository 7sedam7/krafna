use std::error::Error;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use gray_matter::Pod;
use hashbrown::HashSet;
use regex::Regex;

use crate::libs::data_fetcher::fetch_data;
use crate::libs::parser::{
    ExpressionElement, FieldValue, Function, FunctionArg, Operator, OrderByFieldOption,
    OrderDirection, Query,
};
use crate::libs::PeekableDeque;

pub fn execute_query(
    query: &str,
    select: Option<String>,
    from: Option<String>,
    include_fields: Option<String>,
) -> Result<(Vec<String>, Vec<Pod>), Box<dyn Error>> {
    let mut query = match query.parse::<Query>() {
        Ok(q) => q,
        Err(error) => return Err(error.into()),
    };

    // SELECT override if present
    if let Some(select_query) = select {
        let mut peekable_select_query: PeekableDeque<char> =
            PeekableDeque::from_iter(format!("SELECT {}", select_query).chars());
        match Query::parse_select(&mut peekable_select_query) {
            Ok(select_fields) => query.select_fields = select_fields,
            Err(error) => {
                return Err(format!(
                    "Error parsing SELECT: {}, Query: \"{}\"",
                    error, peekable_select_query
                )
                .into())
            }
        }
    }
    // SELECT include/add fields to query SELECT fields
    if let Some(include_select_query) = include_fields {
        let mut peekable_select_query: PeekableDeque<char> =
            PeekableDeque::from_iter(format!("SELECT {}", include_select_query).chars());
        match Query::parse_select(&mut peekable_select_query) {
            Ok(select_fields) => {
                // TODO: Should not filter duplicates, but only append "include_fields" that are not
                // already in "select_fields"
                query.select_fields.retain(|s| !select_fields.contains(s));
                query.select_fields.splice(0..0, select_fields);
            }
            Err(error) => {
                return Err(format!(
                    "Error parsing SELECT: {}, Query: \"{}\"",
                    error, peekable_select_query
                )
                .into())
            }
        }
    }

    if let Some(from_query) = from {
        let mut peekable_from_query: PeekableDeque<char> =
            PeekableDeque::from_iter(format!("FROM {}", from_query).chars());
        match Query::parse_from(&mut peekable_from_query) {
            Ok(from_function) => query.from_function = Some(from_function),
            Err(error) => {
                return Err(format!(
                    "Error parsing FROM: {}, Query: \"{}\"",
                    error, peekable_from_query
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

fn execute_select(fields: &[String], data: &mut Vec<Pod>) {
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

fn execute_order_by(fields: &Vec<OrderByFieldOption>, data: &mut [Pod]) -> Result<(), String> {
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

fn execute_where(condition: &Vec<ExpressionElement>, data: &[Pod]) -> Result<Vec<Pod>, String> {
    if condition.is_empty() {
        return Ok(data.to_vec());
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
    data: &[Pod],
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
    data: &[Pod],
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
    data: &[Pod],
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

/***************************************************************************************************
*************************************** VALUE getters **********************************************
***************************************************************************************************/
fn get_queue_element_value(
    operand: &ExpressionElement,
    data: &Pod,
) -> Result<Option<FieldValue>, String> {
    match operand {
        ExpressionElement::FieldName(field_name) => Ok(get_field_value(field_name, data)),
        ExpressionElement::FieldValue(field_value) => Ok(Some(field_value.clone())),
        ExpressionElement::Function(func) => match func.name.to_uppercase().as_str() {
            "DATEADD" => Ok(Some(execute_function_date_add(func, data)?)),
            _ => Err(format!("TODO: Implement function execution: {:?}!", func)),
        },
        _ => Err(format!("Unsupported element: {:?}!", operand)),
    }
}

pub fn get_nested_pod(field_name: &str, data: &Pod) -> Option<Pod> {
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

pub fn get_field_value(field_name: &str, data: &Pod) -> Option<FieldValue> {
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

/***************************************************************************************************
*************************************** EXECUTE functions ******************************************
***************************************************************************************************/
fn execute_operation_like(a: FieldValue, b: FieldValue) -> bool {
    match (a, b) {
        (FieldValue::String(a_str), FieldValue::String(b_str)) => {
            Regex::new(&b_str).map_or(false, |re| re.is_match(&a_str))
        }
        _ => false,
    }
}

fn execute_function_date_add(func: &Function, data: &Pod) -> Result<FieldValue, String> {
    if func.args.len() != 3 {
        return Err(format!(
            "Function DATEADD expects 3 arguments, but found {}!",
            func.args.len()
        ));
    }

    // FIRST ARGUMENT
    let interval = match &func.args[0] {
        FunctionArg::FieldName(interval) => interval,
        _ => {
            return Err(format!(
                "Function DATEADD expects first argument to be a interval, but found: {:?}",
                func.args[0]
            ))
        }
    };

    // SECOND ARGUMENT
    let number = match &func.args[1] {
        FunctionArg::FieldName(field_name) => match get_field_value(field_name, data) {
            Some(FieldValue::Number(number)) => number,
            _ => {
                return Err(format!(
                    "Function DATEADD expects second argument to be a number, but found: {:?}",
                    func.args[1]
                ))
            }
        },
        FunctionArg::FieldValue(FieldValue::Number(number)) => *number,
        _ => {
            return Err(format!(
                "Function DATEADD expects second argument to be a number, but found: {:?}",
                func.args[1]
            ))
        }
    };

    // THIRD ARGUMENT
    let date_str = match &func.args[2] {
        FunctionArg::FieldName(field_name) => match get_field_value(field_name, data) {
            Some(FieldValue::String(date_str)) => date_str,
            _ => {
                return Err(format!(
                    "Function DATEADD expects third argument to be a date, but found: {:?}",
                    func.args[2]
                ))
            }
        },
        FunctionArg::FieldValue(FieldValue::String(date_str)) => date_str.clone(),
        _ => {
            return Err(format!(
                "Function DATEADD expects third argument to be a date, but found: {:?}",
                func.args[2]
            ))
        }
    };
    let naive_datetime = match parse_naive_datetime(&date_str) {
        Ok(date) => date,
        Err(_) => {
            return Err(format!(
                "Function DATEADD expects third argument to be a date, but found: {:?}",
                func.args[2]
            ))
        }
    };

    let result_date = match match interval.to_uppercase().as_str() {
        "YEAR" => naive_datetime.with_year(naive_datetime.year() + number as i32),
        "MONTH" => {
            let months_to_add = naive_datetime.month() as i32 + number as i32;
            let years_to_add = (months_to_add - 1) / 12;
            let new_month = ((months_to_add - 1) % 12) + 1;
            naive_datetime
                .with_year(naive_datetime.year() + years_to_add)
                .and_then(|d| d.with_month(new_month as u32))
        },
        "WEEK" => naive_datetime.checked_add_signed(chrono::Duration::weeks(number as i64)),
        "DAY" => naive_datetime.checked_add_signed(chrono::Duration::days(number as i64)),
        "HOUR" => naive_datetime.checked_add_signed(chrono::Duration::hours(number as i64)),
        "MINUTE" => naive_datetime.checked_add_signed(chrono::Duration::minutes(number as i64)),
        "SECOND" => naive_datetime.checked_add_signed(chrono::Duration::seconds(number as i64)),
        "MILISECOND" => naive_datetime.checked_add_signed(chrono::Duration::milliseconds(number as i64)),
        "MICROSECOND" => naive_datetime.checked_add_signed(chrono::Duration::microseconds(number as i64)),
        "NANOSECOND" => naive_datetime.checked_add_signed(chrono::Duration::nanoseconds(number as i64)),
        _ => {
            return Err(format!(
                "Function DATEADD expects first argument to be a valid interval, but found: {:?}",
                interval
            ))
        }
    } {
        Some(result_date) => result_date,
        None => {
            return Err(format!(
                "Function DATEADD expects second argument to be a number within `interval` range, but found: {} for interval: {}",
                number,
                interval
            ))
        }
    };

    Ok(FieldValue::String(
        result_date.format("%Y-%m-%dT%H:%M:%S").to_string(),
    ))
}

// TODO: use for `execute_function_date` that parses a date `DATE(<date>, <optional format>)`
fn parse_naive_datetime(input: &str) -> Result<NaiveDateTime, String> {
    // Try to parse as
    if let Ok(date_time) = input.parse::<DateTime<Utc>>() {
        return Ok(date_time.naive_utc());
    }
    // Try to parse as full date-time first
    if let Ok(naive_datetime) = NaiveDateTime::parse_from_str(input, "%Y-%m-%dT%H:%M:%S") {
        Ok(naive_datetime)
    }
    // If that fails, try to parse as a date only
    else if let Ok(naive_date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        // Add a default time of 00:00:00
        Ok(naive_date
            .and_hms_opt(0, 0, 0)
            .expect("Failed to parse date"))
    } else {
        // Return an error if neither format works
        Err(format!("Invalid input: {}", input).to_string())
    }
}
