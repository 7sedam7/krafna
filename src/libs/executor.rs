use std::collections::HashMap;
use std::error::Error;
use std::num::NonZero;
use std::sync::Mutex;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use gray_matter::Pod;
use lru::LruCache;
use once_cell::sync::Lazy;
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
    let mut data = fetch_data(&query.from_function.unwrap())?;
    // WHERE
    execute_where(&query.where_expression, &mut data)?;
    // ORDER BY
    execute_order_by(&query.order_by_fields, &mut data)?;
    // SELECT
    execute_select(&query.select_fields, &mut data);

    Ok((query.select_fields, data))
}

fn execute_select(fields: &[String], data: &mut Vec<Pod>) {
    // TODO: implement * to select all values
    // TODO: implement function calls in select
    // TODO: implement AS in select
    let check_fields: Vec<String> = fields
        .iter()
        .map(|s| {
            s.split_once('.')
                .map_or(s.to_string(), |(before, _)| before.to_string())
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
        // TODO: add support for functions in order by
        for orderby_field in fields {
            let fv_a = get_field_value(&orderby_field.field_name, a);
            let fv_b = get_field_value(&orderby_field.field_name, b);

            let comparison: std::cmp::Ordering = if matches!(fv_a, FieldValue::Null) {
                std::cmp::Ordering::Less
            } else if matches!(fv_b, FieldValue::Null) {
                std::cmp::Ordering::Greater
            } else {
                fv_a.partial_cmp(&fv_b).unwrap_or(std::cmp::Ordering::Equal)
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

fn execute_where(expression: &Vec<ExpressionElement>, data: &mut Vec<Pod>) -> Result<(), String> {
    if expression.is_empty() || data.is_empty() {
        return Ok(());
    }

    // Dry run to return an error if expression is invalid
    let _ = evaluate_expression(expression, data.first().unwrap())?;

    data.retain(|pod| match evaluate_expression(expression, pod) {
        Ok(FieldValue::Bool(bool)) => bool,
        _ => false,
    });

    Ok(())
}

fn evaluate_expression(
    expression: &Vec<ExpressionElement>,
    data: &Pod,
) -> Result<FieldValue, String> {
    // Define operator precedence
    let operator_precedence = |op: &Operator| match op {
        Operator::Or => 0,
        Operator::And => 1,
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

    let mut stack: Vec<ExpressionElement> = Vec::new();
    let mut queue: Vec<FieldValue> = Vec::new();

    for element in expression {
        match element {
            ExpressionElement::OpenedBracket => stack.push(ExpressionElement::OpenedBracket),
            ExpressionElement::FieldName(field_name) => {
                queue.push(get_field_value(field_name, data))
            }
            ExpressionElement::FieldValue(field_value) => queue.push(field_value.clone()),
            ExpressionElement::Function(func) => queue.push(execute_function(func, data)?),
            ExpressionElement::Operator(op) => {
                // op goes on stack, but if stack has equal or higher priority operator on top, that one
                // goes from stack to the "queue"
                while let Some(ExpressionElement::Operator(last_op)) = stack.last() {
                    if operator_precedence(last_op) >= operator_precedence(op) {
                        evaluate_stack_operator(&mut stack, &mut queue)?;
                    } else {
                        break;
                    }
                }
                stack.push(element.clone());
            }
            ExpressionElement::ClosedBracket => {
                while !matches!(stack.last(), Some(ExpressionElement::OpenedBracket)) {
                    evaluate_stack_operator(&mut stack, &mut queue)?;
                }
                stack.pop();
            }
        }
    }
    while stack.last().is_some() {
        evaluate_stack_operator(&mut stack, &mut queue)?;
    }

    if queue.len() != 1 {
        return Err(format!(
            "Expected exactly one element on the queue, but found {:?}!",
            queue
        ));
    }

    Ok(queue.pop().unwrap())
}

fn evaluate_stack_operator(
    stack: &mut Vec<ExpressionElement>,
    queue: &mut Vec<FieldValue>,
) -> Result<(), String> {
    let should_be_operator = stack.pop();
    match should_be_operator {
        Some(ExpressionElement::Operator(operator)) => {
            let right = queue
                .pop()
                .ok_or("Expected operand on the queue, but found nothing!")?;
            let left = queue
                .pop()
                .ok_or("Expected operand on the queue, but found nothing!")?;

            queue.push(execute_operation(&operator, &left, &right)?);
        }
        _ => {
            return Err(format!(
                "Expected operator on top of the stack, but found {:?}!",
                should_be_operator
            ));
        }
    }

    Ok(())
}

fn execute_operation(
    op: &Operator,
    left: &FieldValue,
    right: &FieldValue,
) -> Result<FieldValue, String> {
    match op {
        // get bools, return bool
        Operator::And => match (left, right) {
            (FieldValue::Bool(left), FieldValue::Bool(right)) => {
                Ok(FieldValue::Bool(*left && *right))
            }
            _ => Err("AND operator expects operands to be bools!".to_string()),
        },
        Operator::Or => match (left, right) {
            (FieldValue::Bool(left), FieldValue::Bool(right)) => {
                Ok(FieldValue::Bool(*left || *right))
            }
            _ => Err("OR operator expects operands to be bools!".to_string()),
        },

        // get values, return bools
        Operator::Like => Ok(FieldValue::Bool(execute_operation_like(left, right))),
        Operator::NotLike => Ok(FieldValue::Bool(!execute_operation_like(left, right))),
        Operator::In => Ok(FieldValue::Bool(right.contains(left))),
        Operator::Lt => Ok(FieldValue::Bool(left < right)),
        Operator::Lte => Ok(FieldValue::Bool(left <= right)),
        Operator::Gt => Ok(FieldValue::Bool(left > right)),
        Operator::Gte => Ok(FieldValue::Bool(left >= right)),
        Operator::Eq => Ok(FieldValue::Bool(left == right)),
        Operator::Neq => Ok(FieldValue::Bool(left != right)),

        // get values, return values
        Operator::Plus => left.add(right),
        Operator::Minus => left.subtract(right),
        Operator::Multiply => left.multiply(right),
        Operator::Divide => left.divide(right),
        Operator::Power => left.power(right),
        Operator::FloorDivide => left.floor_divide(right),
    }
}

static REGEX_CACHE: Lazy<Mutex<LruCache<String, Regex>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(LruCache::new(NonZero::new(100).unwrap())));
fn execute_operation_like(a: &FieldValue, b: &FieldValue) -> bool {
    match (a, b) {
        (FieldValue::String(a_str), FieldValue::String(b_str)) => {
            let mut cache = REGEX_CACHE.lock().unwrap();
            match cache.get(b_str) {
                Some(re) => re.is_match(a_str),
                None => {
                    if let Ok(re) = Regex::new(b_str) {
                        let res = re.is_match(a_str);
                        cache.put(b_str.clone(), re);
                        res
                    } else {
                        false
                    }
                }
            }
        }
        _ => false,
    }
}

/***************************************************************************************************
*************************************** VALUE getters **********************************************
***************************************************************************************************/
pub fn get_field_value(field_name: &str, data: &Pod) -> FieldValue {
    match get_nested_pod(field_name, data) {
        Some(Pod::String(str)) => FieldValue::String(str.clone()),
        Some(Pod::Float(num)) => FieldValue::Number(num),
        Some(Pod::Integer(num)) => FieldValue::Number(num as f64),
        Some(Pod::Boolean(bool)) => FieldValue::Bool(bool),
        Some(Pod::Array(list)) => pod_array_to_field_value(&list),
        Some(Pod::Hash(hash)) => pod_hash_to_field_value(&hash),
        _ => FieldValue::Null,
    }
}

pub fn get_nested_pod(field_name: &str, data: &Pod) -> Option<Pod> {
    // TODO: think about field case insensitive comparisson (could convert to_lower when parsing
    // the data)
    let mut current = data.clone();
    for key in field_name.split('.') {
        match current.as_hashmap() {
            Ok(hash) => match hash.get(key) {
                Some(pod) => current.clone_from(pod),
                None => return None,
            },
            Err(_) => return None,
        }
    }
    Some(current)
}

fn pod_array_to_field_value(list: &Vec<Pod>) -> FieldValue {
    let mut fv_list = Vec::new();

    for el in list {
        match el {
            Pod::String(str) => fv_list.push(FieldValue::String(str.clone())),
            Pod::Float(num) => fv_list.push(FieldValue::Number(*num)),
            Pod::Integer(num) => fv_list.push(FieldValue::Number(*num as f64)),
            Pod::Boolean(bool) => fv_list.push(FieldValue::Bool(*bool)),
            Pod::Array(list) => fv_list.push(pod_array_to_field_value(list)),
            Pod::Hash(hash) => fv_list.push(pod_hash_to_field_value(hash)),
            _ => {}
        }
    }

    FieldValue::List(fv_list)
}

fn pod_hash_to_field_value(hash: &HashMap<String, Pod>) -> FieldValue {
    match Pod::Hash(hash.clone()).deserialize::<serde_json::Value>() {
        Ok(val) => FieldValue::String(val.to_string()),
        Err(_) => FieldValue::Null,
    }
}

/***************************************************************************************************
*************************************** EXECUTE functions ******************************************
***************************************************************************************************/
fn execute_function(func: &Function, data: &Pod) -> Result<FieldValue, String> {
    match func.name.to_uppercase().as_str() {
        "DATEADD" => Ok(execute_function_date_add(func, data)?),
        "DATE" => Ok(execute_function_date(func, data)?),
        _ => Err(format!("TODO: Implement function execution: {:?}!", func)),
    }
}

const DATE_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";
fn execute_function_date_add(func: &Function, data: &Pod) -> Result<FieldValue, String> {
    if func.args.len() != 3 && func.args.len() != 4 {
        return Err(format!(
            "Function DATEADD expects 3 or 4 arguments, but found {}!",
            func.args.len()
        ));
    }

    // FIRST ARGUMENT
    let interval: String = match &func.args[0] {
        FunctionArg::FieldName(field_name) => match get_field_value(field_name, data) {
            FieldValue::String(interval) => interval,
            _ => {
                return Err(format!(
                    "Function DATEADD expects first argument to be an interval, but found: {:?}",
                    func.args[0]
                ))
            }
        },
        FunctionArg::FieldValue(FieldValue::String(interval)) => interval.clone(),
        _ => {
            return Err(format!(
                "Function DATEADD expects first argument to be an interval, but found: {:?}",
                func.args[0]
            ))
        }
    };

    // SECOND ARGUMENT
    let number = match &func.args[1] {
        FunctionArg::FieldName(field_name) => match get_field_value(field_name, data) {
            FieldValue::Number(number) => number,
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
            FieldValue::String(date_str) => date_str,
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

    // FOURTH ARGUMENT
    let format_str = match &func.args.get(3) {
        Some(FunctionArg::FieldName(field_name)) => match get_field_value(field_name, data) {
            FieldValue::String(format_str) => Some(format_str),
            FieldValue::Null => None,
            _ => {
                return Err(format!(
                    "Function DATEADD expects fourth argument to be a format, but found: {:?}",
                    func.args[3]
                ))
            }
        },
        Some(FunctionArg::FieldValue(FieldValue::String(format_str))) => Some(format_str.clone()),
        None => None,
        _ => {
            return Err(format!(
                "Function DATEADD expects fourth argument to be a format, but found: {:?}",
                func.args[3]
            ))
        }
    };
    let naive_datetime = match parse_naive_datetime(&date_str, &format_str) {
        Ok(date) => date,
        Err(_) => {
            return Err(format!(
                "Function DATEADD did not succeed to parse {:?} into a date with format \"{:?}\"",
                date_str, format_str
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
        result_date.format(DATE_FORMAT).to_string(),
    ))
}

fn execute_function_date(func: &Function, data: &Pod) -> Result<FieldValue, String> {
    if func.args.len() != 1 && func.args.len() != 2 {
        return Err(format!(
            "Function DATE expects 1 or 2 arguments, but found {}!",
            func.args.len()
        ));
    }

    // FIRST ARGUMENT
    let date_str = match &func.args[0] {
        FunctionArg::FieldName(field_name) => match get_field_value(field_name, data) {
            FieldValue::String(date_str) => date_str,
            _ => {
                return Err(format!(
                    "Function DATE expects first argument to be a date, but found: {:?}",
                    func.args[0]
                ))
            }
        },
        FunctionArg::FieldValue(FieldValue::String(date_str)) => date_str.clone(),
        _ => {
            return Err(format!(
                "Function DATE expects first argument to be a date, but found: {:?}",
                func.args[0]
            ))
        }
    };

    // SECOND ARGUMENT
    let format_str = match &func.args.get(1) {
        Some(FunctionArg::FieldName(field_name)) => match get_field_value(field_name, data) {
            FieldValue::String(format_str) => Some(format_str),
            FieldValue::Null => None,
            _ => {
                return Err(format!(
                    "Function DATE expects second argument to be a format, but found: {:?}",
                    func.args[1]
                ))
            }
        },
        Some(FunctionArg::FieldValue(FieldValue::String(format_str))) => Some(format_str.clone()),
        None => None,
        _ => {
            return Err(format!(
                "Function DATE expects second argument to be a format, but found: {:?}",
                func.args[1]
            ))
        }
    };

    let naive_datetime = match parse_naive_datetime(&date_str, &format_str) {
        Ok(date) => date,
        Err(_) => {
            return Err(format!(
                "Function DATE did not succeed to parse {:?} into a date with format \"{:?}\"",
                date_str, format_str
            ))
        }
    };

    Ok(FieldValue::String(
        naive_datetime.format(DATE_FORMAT).to_string(),
    ))
}

// TODO: use for `execute_function_date` that parses a date `DATE(<date>, <optional format>)`
fn parse_naive_datetime(input: &str, format: &Option<String>) -> Result<NaiveDateTime, String> {
    if let Some(format) = format {
        if let Ok(naive_date) = NaiveDate::parse_from_str(input, format) {
            return Ok(naive_date
                .and_hms_opt(0, 0, 0)
                .expect("Failed to parse date"));
        };
        return match NaiveDateTime::parse_from_str(input, format) {
            Ok(naive_datetime) => Ok(naive_datetime),
            Err(err) => Err(format!("Invalid input: {}; {}", input, err)),
        };
    }
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
        Err(format!("Invalid input: {}", input))
    }
}

/***************************************************************************************************
* TESTS
* *************************************************************************************************/
#[cfg(test)]
mod tests {
    use super::*;
    use gray_matter::Pod;

    /***************************************************************************************************
     * TESTS for execute_select
     * *************************************************************************************************/
    #[test]
    fn test_execute_select_retains_specified_field() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();
        let searched_field = "field2".to_string();
        let field3 = "field3".to_string();
        let non_existant_searched_field = "field4".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = pod1.insert(searched_field.clone(), Pod::String("value2".to_string()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String("value4".to_string()));
        let _ = pod2.insert(searched_field.clone(), Pod::String("value5".to_string()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1, pod2];
        let expected_data_len = data.len();

        // Execute select with field2
        execute_select(
            &[searched_field.clone(), non_existant_searched_field.clone()],
            &mut data,
        );

        // Verify results
        assert_eq!(
            expected_data_len,
            data.len(),
            "Data length should remain the same"
        );
        for pod in data {
            if let Pod::Hash(hash) = pod {
                assert_eq!(1, hash.len(), "Pod should have exactly 1 field");
                assert!(
                    hash.contains_key(&searched_field),
                    "Pod should retain field2"
                );
                assert!(
                    !hash.contains_key(&non_existant_searched_field),
                    "Pod should remove field1"
                );
                assert!(!hash.contains_key(&field1), "Pod should remove field1");
                assert!(!hash.contains_key(&field3), "Pod should remove field3");
            } else {
                panic!("Expectek Pod::Hash");
            }
        }
    }

    #[test]
    fn test_execute_select_retains_nested_field() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();

        let nest2 = "nest2".to_string();
        let nest2_value = "nest2_value".to_string();

        let nest3 = "nest3".to_string();
        let nest3_value = "nest3_value".to_string();

        let searched_field1 = format!("{}.{}", nest2, nest2);
        let searched_field2 = format!("{}.{}.{}", nest3, nest3, nest3);

        // setup pods
        let mut setup_pod = Pod::new_hash();
        let _ = setup_pod.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = setup_pod.insert(nest2.clone(), {
            let mut nest_pod = Pod::new_hash();
            let _ = nest_pod.insert(nest2.clone(), Pod::String(nest2_value.clone()));
            nest_pod
        });
        let _ = setup_pod.insert(nest3.clone(), {
            let mut nest_pod = Pod::new_hash();
            let _ = nest_pod.insert(nest3.clone(), {
                let mut nest_pod = Pod::new_hash();
                let _ = nest_pod.insert(nest3.clone(), Pod::String(nest3_value.clone()));
                nest_pod
            });
            nest_pod
        });

        let mut data = vec![setup_pod.clone()];
        let expected_data_len = data.len();

        // Execute select with field2
        execute_select(&[searched_field1, searched_field2], &mut data);

        // Verify results
        assert_eq!(
            expected_data_len,
            data.len(),
            "Data length should remain the same"
        );
        for pod in data {
            if let Pod::Hash(hash) = pod {
                assert_eq!(2, hash.len(), "Pod should have exactly 2 field");
                assert!(!hash.contains_key(&field1), "Pod should remove field1");

                assert!(hash.contains_key(&nest2), "Pod should retain nest2");
                assert_eq!(
                    setup_pod.as_hashmap().unwrap().get(&nest2).unwrap(),
                    hash.get(&nest2).unwrap()
                );

                assert!(hash.contains_key(&nest3), "Pod should retain nest3");
                assert_eq!(
                    setup_pod.as_hashmap().unwrap().get(&nest3).unwrap(),
                    hash.get(&nest3).unwrap()
                );
            } else {
                panic!("Expectek Pod::Hash");
            }
        }
    }

    /***************************************************************************************************
     * TESTS for execute_order_by
     * *************************************************************************************************/
    #[test]
    fn test_execute_order_by_null_values() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();

        let field2 = "field2".to_string();
        let field2_value1 = "value1".to_string();

        let field3 = "field3".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = pod1.insert(field2.clone(), Pod::String(field2_value1.clone()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String("value4".to_string()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1.clone(), pod2.clone()];

        // Execute order by field2
        assert!(
            execute_order_by(
                &vec![OrderByFieldOption {
                    field_name: field2.clone(),
                    order_direction: OrderDirection::ASC,
                }],
                &mut data,
            )
            .is_ok(),
            "Order by should be successful"
        );

        // Verify results
        assert_eq!(2, data.len(), "Data length should remain the same");
        assert_eq!(pod2, data[0], "First element should be pod2");
        assert_eq!(pod1, data[1], "Second element should be pod1");
    }

    #[test]
    fn test_execute_order_by_no_change() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();

        let field2 = "field2".to_string();
        let field2_value1 = "value1".to_string();
        let field2_value2 = "value2".to_string();

        let field3 = "field3".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = pod1.insert(field2.clone(), Pod::String(field2_value1.clone()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String("value4".to_string()));
        let _ = pod2.insert(field2.clone(), Pod::String(field2_value2.clone()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1.clone(), pod2.clone()];

        // Execute order by field2
        assert!(
            execute_order_by(
                &vec![OrderByFieldOption {
                    field_name: field2.clone(),
                    order_direction: OrderDirection::ASC,
                }],
                &mut data,
            )
            .is_ok(),
            "Order by should be successful"
        );

        // Verify results
        assert_eq!(2, data.len(), "Data length should remain the same");
        assert_eq!(pod1, data[0], "First element should be pod1");
        assert_eq!(pod2, data[1], "Second element should be pod2");
    }

    #[test]
    fn test_execute_order_by_asc() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();

        let field2 = "field2".to_string();
        let field2_value1 = "value2".to_string();
        let field2_value2 = "value1".to_string();

        let field3 = "field3".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = pod1.insert(field2.clone(), Pod::String(field2_value1.clone()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String("value4".to_string()));
        let _ = pod2.insert(field2.clone(), Pod::String(field2_value2.clone()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1.clone(), pod2.clone()];

        // Execute order by field2
        assert!(
            execute_order_by(
                &vec![OrderByFieldOption {
                    field_name: field2.clone(),
                    order_direction: OrderDirection::ASC,
                }],
                &mut data,
            )
            .is_ok(),
            "Order by should be successful"
        );

        // Verify results
        assert_eq!(2, data.len(), "Data length should remain the same");
        assert_eq!(pod2, data[0], "First element should be pod2");
        assert_eq!(pod1, data[1], "Second element should be pod1");
    }

    #[test]
    fn test_execute_order_by_desc() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();

        let field2 = "field2".to_string();
        let field2_value1 = "value1".to_string();
        let field2_value2 = "value2".to_string();

        let field3 = "field3".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = pod1.insert(field2.clone(), Pod::String(field2_value1.clone()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String("value4".to_string()));
        let _ = pod2.insert(field2.clone(), Pod::String(field2_value2.clone()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1.clone(), pod2.clone()];

        // Execute order by field2
        assert!(
            execute_order_by(
                &vec![OrderByFieldOption {
                    field_name: field2.clone(),
                    order_direction: OrderDirection::DESC,
                }],
                &mut data,
            )
            .is_ok(),
            "Order by should be successful"
        );

        // Verify results
        assert_eq!(2, data.len(), "Data length should remain the same");
        assert_eq!(pod2, data[0], "First element should be pod2");
        assert_eq!(pod1, data[1], "Second element should be pod1");
    }

    #[test]
    fn test_execute_order_multi() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();
        let field1_value1 = "value1".to_string();
        let field1_value2 = "value2".to_string();
        let field1_value3 = "value3".to_string();

        let field2 = "field2".to_string();
        let field2_value1 = "value1".to_string();
        let field2_value2 = "value2".to_string();
        let field2_value3 = "value2".to_string();

        let field3 = "field3".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String(field1_value1.clone()));
        let _ = pod1.insert(field2.clone(), Pod::String(field2_value1.clone()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String(field1_value2.clone()));
        let _ = pod2.insert(field2.clone(), Pod::String(field2_value2.clone()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut pod3 = Pod::new_hash();
        let _ = pod3.insert(field1.clone(), Pod::String(field1_value3.clone()));
        let _ = pod3.insert(field2.clone(), Pod::String(field2_value3.clone()));
        let _ = pod3.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1.clone(), pod2.clone(), pod3.clone()];

        // Execute order by field2
        assert!(
            execute_order_by(
                &vec![
                    OrderByFieldOption {
                        field_name: field2.clone(),
                        order_direction: OrderDirection::DESC,
                    },
                    OrderByFieldOption {
                        field_name: field1.clone(),
                        order_direction: OrderDirection::ASC,
                    }
                ],
                &mut data,
            )
            .is_ok(),
            "Order by should be successful"
        );

        // Verify results
        assert_eq!(3, data.len(), "Data length should remain the same");
        assert_eq!(pod2, data[0], "First element should be pod2");
        assert_eq!(pod3, data[1], "Second element should be pod3");
        assert_eq!(pod1, data[2], "Second element should be pod1");
    }

    /***************************************************************************************************
     * TESTS for execute_where
     * *************************************************************************************************/
    #[test]
    fn test_execute_where_equals() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();
        let field2 = "field2".to_string();
        let field2_value = "value2".to_string();
        let field3 = "field3".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = pod1.insert(field2.clone(), Pod::String(field2_value.clone()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String("value4".to_string()));
        let _ = pod2.insert(field2.clone(), Pod::String("value5".to_string()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1.clone(), pod2.clone()];

        // Execute where field2 == "value2"
        assert!(
            execute_where(
                &vec![
                    ExpressionElement::FieldName(field2.clone()),
                    ExpressionElement::Operator(Operator::Eq),
                    ExpressionElement::FieldValue(FieldValue::String(field2_value.clone())),
                ],
                &mut data,
            )
            .is_ok(),
            "Where should be successful"
        );

        // Verify results
        assert_eq!(1, data.len(), "There should be 1 element in data");
        assert_eq!(pod1, data[0], "Result should be pod1");
    }

    #[test]
    fn test_execute_where_equals_no_field() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();
        let field2 = "field2".to_string();
        let field2_value = "value2".to_string();
        let field3 = "field3".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String("value4".to_string()));
        let _ = pod2.insert(field2.clone(), Pod::String(field2_value.clone()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1.clone(), pod2.clone()];

        // Execute where field2 == "value2"
        assert!(
            execute_where(
                &vec![
                    ExpressionElement::FieldName(field2.clone()),
                    ExpressionElement::Operator(Operator::Eq),
                    ExpressionElement::FieldValue(FieldValue::String(field2_value.clone())),
                ],
                &mut data,
            )
            .is_ok(),
            "Where should be successful"
        );

        // Verify results
        assert_eq!(1, data.len(), "There should be 1 element in data");
        assert_eq!(pod2, data[0], "Result should be pod2");
    }

    #[test]
    fn test_execute_where_func() {
        // Create sample Pod data with 3 fields
        let date_value = "2021-01-01".to_string();
        let date_value_plus_1_year = "2022-01+01".to_string();

        let field1 = "field1".to_string();
        let field2 = "field2".to_string();
        let field3 = "field3".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = pod1.insert(field2.clone(), Pod::String(date_value_plus_1_year.clone()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String("value4".to_string()));
        let _ = pod2.insert(field2.clone(), Pod::String("value5".to_string()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1.clone(), pod2.clone()];

        // Execute where field2 LIKE "val.*"
        assert!(
            execute_where(
                &vec![
                    ExpressionElement::Function(Function {
                        name: "DATE".to_string(),
                        args: vec![
                            FunctionArg::FieldName(field2.clone()),
                            FunctionArg::FieldValue(FieldValue::String("%Y-%m+%d".to_string()))
                        ]
                    }),
                    ExpressionElement::Operator(Operator::Eq),
                    ExpressionElement::Function(Function {
                        name: "DATEADD".to_string(),
                        args: vec![
                            FunctionArg::FieldValue(FieldValue::String("YEAR".to_string())),
                            FunctionArg::FieldValue(FieldValue::Number(1.0)),
                            FunctionArg::FieldValue(FieldValue::String(date_value))
                        ]
                    }),
                ],
                &mut data,
            )
            .is_ok(),
            "Where should be successful"
        );

        // Verify results
        assert_eq!(1, data.len(), "There should be 1 element in data");
        assert_eq!(pod1, data[0], "Result should be pod1");
    }

    #[test]
    fn test_execute_where_like() {
        // Create sample Pod data with 3 fields
        let field1 = "field1".to_string();

        let field2 = "field2".to_string();
        let field2_value1 = "smurph".to_string();
        let field2_value2 = "value2".to_string();

        let field3 = "field3".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::String("value1".to_string()));
        let _ = pod1.insert(field2.clone(), Pod::String(field2_value1.clone()));
        let _ = pod1.insert(field3.clone(), Pod::String("value3".to_string()));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::String("value4".to_string()));
        let _ = pod2.insert(field2.clone(), Pod::String(field2_value2.clone()));
        let _ = pod2.insert(field3.clone(), Pod::String("value6".to_string()));

        let mut data = vec![pod1.clone(), pod2.clone()];

        // Execute where field2 LIKE "val.*"
        assert!(
            execute_where(
                &vec![
                    ExpressionElement::FieldName(field2.clone()),
                    ExpressionElement::Operator(Operator::Like),
                    ExpressionElement::FieldValue(FieldValue::String("val.*".to_string())),
                ],
                &mut data,
            )
            .is_ok(),
            "Where should be successful"
        );

        // Verify results
        assert_eq!(1, data.len(), "There should be 1 element in data");
        assert_eq!(pod2, data[0], "Result should be pod2");
    }

    #[test]
    fn test_execute_where_complex() {
        // Create sample Pod data with 3 fields
        let value1 = 1.0;
        let value2 = 2.0;
        let value3 = 3.0;
        let value4 = 4.0;

        let field1 = "field1".to_string();
        let field2 = "field2".to_string();
        let field3 = "field3".to_string();
        let field4 = "field4".to_string();

        let mut pod1 = Pod::new_hash();
        let _ = pod1.insert(field1.clone(), Pod::Float(value4));
        let _ = pod1.insert(field2.clone(), Pod::Float(value2));
        let _ = pod1.insert(field3.clone(), Pod::Float(value3));
        let _ = pod1.insert(field4.clone(), Pod::Float(value4));

        let mut pod2 = Pod::new_hash();
        let _ = pod2.insert(field1.clone(), Pod::Float(value1));
        let _ = pod2.insert(field2.clone(), Pod::Float(value2));
        let _ = pod2.insert(field3.clone(), Pod::Float(value2));
        let _ = pod2.insert(field4.clone(), Pod::Float(value3));

        let mut pod3 = Pod::new_hash();
        let _ = pod3.insert(field1.clone(), Pod::Float(value1));
        let _ = pod3.insert(field2.clone(), Pod::Float(value1));
        let _ = pod3.insert(field3.clone(), Pod::Float(value3));
        let _ = pod3.insert(field4.clone(), Pod::Float(value4));

        let mut pod4 = Pod::new_hash();
        let _ = pod4.insert(field1.clone(), Pod::Float(value1));
        let _ = pod4.insert(field2.clone(), Pod::Float(value1));
        let _ = pod4.insert(field3.clone(), Pod::Float(value2));
        let _ = pod4.insert(field4.clone(), Pod::Float(value4));

        let mut pod5 = Pod::new_hash();
        let _ = pod5.insert(field1.clone(), Pod::Float(value1));
        let _ = pod5.insert(field2.clone(), Pod::Float(value1));
        let _ = pod5.insert(field3.clone(), Pod::Float(value3));
        let _ = pod5.insert(field4.clone(), Pod::Float(value3));

        let mut data = vec![
            pod1.clone(),
            pod2.clone(),
            pod3.clone(),
            pod4.clone(),
            pod5.clone(),
        ];

        // Execute where f1 == v4 or f2 == v1 and (f3 == v2 or f4 == v3)
        assert!(
            execute_where(
                &vec![
                    ExpressionElement::FieldName(field1.clone()),
                    ExpressionElement::Operator(Operator::Eq),
                    ExpressionElement::FieldValue(FieldValue::Number(value4)),
                    ExpressionElement::Operator(Operator::Or),
                    ExpressionElement::FieldName(field2.clone()),
                    ExpressionElement::Operator(Operator::Eq),
                    ExpressionElement::FieldValue(FieldValue::Number(value1)),
                    ExpressionElement::Operator(Operator::And),
                    ExpressionElement::OpenedBracket,
                    ExpressionElement::FieldName(field3.clone()),
                    ExpressionElement::Operator(Operator::Eq),
                    ExpressionElement::FieldValue(FieldValue::Number(value2)),
                    ExpressionElement::Operator(Operator::Or),
                    ExpressionElement::FieldName(field4.clone()),
                    ExpressionElement::Operator(Operator::Eq),
                    ExpressionElement::FieldValue(FieldValue::Number(value3)),
                    ExpressionElement::ClosedBracket,
                ],
                &mut data,
            )
            .is_ok(),
            "Where should be successful"
        );

        // Verify results
        assert_eq!(3, data.len(), "There should be 3 elements in data");
        assert_eq!(pod1, data[0], "Result should have pod1");
        assert_eq!(pod4, data[1], "Result should have pod4");
        assert_eq!(pod5, data[2], "Result should have pod5");
    }

    /***************************************************************************************************
     * TESTS for evaluate_expression
     * *************************************************************************************************/
    #[test]
    fn test_evaluate_expression() {
        let expression = vec![
            ExpressionElement::FieldValue(FieldValue::Number(1.0)),
            ExpressionElement::Operator(Operator::Plus),
            ExpressionElement::FieldValue(FieldValue::Number(2.0)),
            ExpressionElement::Operator(Operator::Multiply),
            ExpressionElement::FieldValue(FieldValue::Number(3.0)),
            ExpressionElement::Operator(Operator::Eq),
            ExpressionElement::FieldValue(FieldValue::Number(7.0)),
        ];
        let pod = Pod::new_hash();

        assert_eq!(
            Ok(FieldValue::Bool(true)),
            evaluate_expression(&expression, &pod)
        );
    }

    /***************************************************************************************************
     * TESTS for evaluate_stack_operator
     * *************************************************************************************************/
    #[test]
    fn test_evaluate_stack_operator_empty() {
        let mut stack = vec![];
        let mut queue = vec![];

        assert!(evaluate_stack_operator(&mut stack, &mut queue).is_err());
        assert_eq!(0, stack.len(), "Stack should stay empty");
        assert_eq!(0, queue.len(), "Queue should stay empty");
    }

    #[test]
    fn test_evaluate_stack_operator_no_operator() {
        let mut stack = vec![ExpressionElement::OpenedBracket];
        let mut queue = vec![FieldValue::Number(1.0), FieldValue::Number(2.0)];

        assert!(evaluate_stack_operator(&mut stack, &mut queue).is_err());
        assert_eq!(0, stack.len(), "Stack should stay empty");
        assert_eq!(2, queue.len(), "Queue should have 2 elements");
    }

    #[test]
    fn test_evaluate_stack_operator_with_operator() {
        let mut stack = vec![
            ExpressionElement::OpenedBracket,
            ExpressionElement::Operator(Operator::Eq),
        ];
        let mut queue = vec![FieldValue::Number(1.0), FieldValue::Number(2.0)];

        assert!(evaluate_stack_operator(&mut stack, &mut queue).is_ok());

        assert_eq!(1, stack.len(), "Stack should have 1 element");
        assert_eq!(
            ExpressionElement::OpenedBracket,
            stack.last().unwrap().clone(),
            "Top of the stack should be ("
        );

        assert_eq!(1, queue.len(), "Queue should have 1 elements");
        assert_eq!(
            FieldValue::Bool(false),
            queue.last().unwrap().clone(),
            "Top of the queue should be false"
        );
    }

    #[test]
    fn test_evaluate_stack_operator_no_operands() {
        let mut stack = vec![ExpressionElement::Operator(Operator::Eq)];
        let mut queue = vec![];

        assert!(evaluate_stack_operator(&mut stack, &mut queue).is_err());
        assert_eq!(0, stack.len(), "Stack should stay empty");
        assert_eq!(0, queue.len(), "Queue should be empty");
    }

    #[test]
    fn test_evaluate_stack_operator_one_operand() {
        let mut stack = vec![ExpressionElement::Operator(Operator::Eq)];
        let mut queue = vec![FieldValue::Number(1.0)];

        assert!(evaluate_stack_operator(&mut stack, &mut queue).is_err());
        assert_eq!(0, stack.len(), "Stack should stay empty");
        assert_eq!(0, queue.len(), "Queue should be empty");
    }

    /***************************************************************************************************
     * TESTS for execute_operation
     * *************************************************************************************************/
    #[test]
    fn test_execute_operation_and() {
        assert_eq!(
            Ok(FieldValue::Bool(true)),
            execute_operation(
                &Operator::And,
                &FieldValue::Bool(true),
                &FieldValue::Bool(true)
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::And,
                &FieldValue::Bool(true),
                &FieldValue::Bool(false)
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::And,
                &FieldValue::Bool(false),
                &FieldValue::Bool(true)
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::And,
                &FieldValue::Bool(false),
                &FieldValue::Bool(false)
            )
        );
    }

    #[test]
    fn test_execute_operation_or() {
        assert_eq!(
            Ok(FieldValue::Bool(true)),
            execute_operation(
                &Operator::Or,
                &FieldValue::Bool(true),
                &FieldValue::Bool(true)
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(true)),
            execute_operation(
                &Operator::Or,
                &FieldValue::Bool(true),
                &FieldValue::Bool(false)
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(true)),
            execute_operation(
                &Operator::Or,
                &FieldValue::Bool(false),
                &FieldValue::Bool(true)
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::Or,
                &FieldValue::Bool(false),
                &FieldValue::Bool(false)
            )
        );
    }

    #[test]
    fn test_execute_operation_like() {
        assert_eq!(
            Ok(FieldValue::Bool(true)),
            execute_operation(
                &Operator::Like,
                &FieldValue::String("value".to_string()),
                &FieldValue::String("val.*".to_string())
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::Like,
                &FieldValue::String("value".to_string()),
                &FieldValue::String("[val.*".to_string())
            )
        );
    }

    #[test]
    fn test_execute_operation_not_like() {
        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::NotLike,
                &FieldValue::String("value".to_string()),
                &FieldValue::String("val.*".to_string())
            )
        );
    }

    #[test]
    fn test_execute_operation_in_list() {
        assert_eq!(
            Ok(FieldValue::Bool(true)),
            execute_operation(
                &Operator::In,
                &FieldValue::String("value".to_string()),
                &FieldValue::List(vec![
                    FieldValue::Number(1.0),
                    FieldValue::String("value".to_string())
                ])
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::In,
                &FieldValue::String("value".to_string()),
                &FieldValue::List(vec![
                    FieldValue::Number(1.0),
                    FieldValue::String("valu".to_string())
                ])
            )
        );
    }

    #[test]
    fn test_execute_operation_in_str() {
        assert_eq!(
            Ok(FieldValue::Bool(true)),
            execute_operation(
                &Operator::In,
                &FieldValue::String("lu".to_string()),
                &FieldValue::String("value".to_string()),
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::In,
                &FieldValue::String("ul".to_string()),
                &FieldValue::String("value".to_string()),
            )
        );
    }

    #[test]
    fn test_execute_operation_lt() {
        let smaller = [
            FieldValue::Number(1.0),
            FieldValue::String("aaa".to_string()),
            FieldValue::Bool(false),
        ];
        let greater = [
            FieldValue::Number(2.0),
            FieldValue::String("aab".to_string()),
            FieldValue::Bool(true),
        ];

        for (small, large) in smaller.iter().zip(greater.iter()) {
            assert_eq!(
                Ok(FieldValue::Bool(true)),
                execute_operation(&Operator::Lt, small, large,)
            );

            assert_eq!(
                Ok(FieldValue::Bool(false)),
                execute_operation(&Operator::Lt, large, small,)
            );

            assert_eq!(
                Ok(FieldValue::Bool(false)),
                execute_operation(&Operator::Lt, small, small,)
            );
        }
    }

    #[test]
    fn test_execute_operation_lte() {
        let smaller = [
            FieldValue::Number(1.0),
            FieldValue::String("aaa".to_string()),
            FieldValue::Bool(false),
        ];
        let greater = [
            FieldValue::Number(2.0),
            FieldValue::String("aab".to_string()),
            FieldValue::Bool(true),
        ];

        for (small, large) in smaller.iter().zip(greater.iter()) {
            assert_eq!(
                Ok(FieldValue::Bool(true)),
                execute_operation(&Operator::Lte, small, large)
            );

            assert_eq!(
                Ok(FieldValue::Bool(false)),
                execute_operation(&Operator::Lte, large, small)
            );

            assert_eq!(
                Ok(FieldValue::Bool(true)),
                execute_operation(&Operator::Lte, small, small)
            );
        }
    }

    #[test]
    fn test_execute_operation_gt() {
        let smaller = [
            FieldValue::Number(1.0),
            FieldValue::String("aaa".to_string()),
            FieldValue::Bool(false),
        ];
        let greater = [
            FieldValue::Number(2.0),
            FieldValue::String("aab".to_string()),
            FieldValue::Bool(true),
        ];

        for (small, large) in smaller.iter().zip(greater.iter()) {
            assert_eq!(
                Ok(FieldValue::Bool(true)),
                execute_operation(&Operator::Gt, large, small,)
            );

            assert_eq!(
                Ok(FieldValue::Bool(false)),
                execute_operation(&Operator::Gt, small, large,)
            );

            assert_eq!(
                Ok(FieldValue::Bool(false)),
                execute_operation(&Operator::Gt, small, small,)
            );
        }
    }

    #[test]
    fn test_execute_operation_gte() {
        let smaller = [
            FieldValue::Number(1.0),
            FieldValue::String("aaa".to_string()),
            FieldValue::Bool(false),
        ];
        let greater = [
            FieldValue::Number(2.0),
            FieldValue::String("aab".to_string()),
            FieldValue::Bool(true),
        ];

        for (small, large) in smaller.iter().zip(greater.iter()) {
            assert_eq!(
                Ok(FieldValue::Bool(true)),
                execute_operation(&Operator::Gte, large, small,)
            );

            assert_eq!(
                Ok(FieldValue::Bool(false)),
                execute_operation(&Operator::Gte, small, large,)
            );

            assert_eq!(
                Ok(FieldValue::Bool(true)),
                execute_operation(&Operator::Gte, small, small,)
            );
        }
    }

    #[test]
    fn test_execute_operation_eq() {
        let elements = [
            FieldValue::Number(1.0),
            FieldValue::String("value".to_string()),
            FieldValue::Bool(true),
        ];
        let different_elements = [
            FieldValue::Number(2.0),
            FieldValue::String("different value".to_string()),
            FieldValue::Bool(false),
        ];

        for (el, diff_el) in elements.iter().zip(different_elements.iter()) {
            assert_eq!(
                Ok(FieldValue::Bool(true)),
                execute_operation(&Operator::Eq, &el.clone(), &el.clone())
            );

            assert_eq!(
                Ok(FieldValue::Bool(false)),
                execute_operation(&Operator::Eq, &el.clone(), diff_el)
            );
        }
    }

    #[test]
    fn test_execute_operation_eq_null() {
        assert_eq!(
            Ok(FieldValue::Bool(true)),
            execute_operation(&Operator::Eq, &FieldValue::Null, &FieldValue::Null)
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(&Operator::Eq, &FieldValue::Null, &FieldValue::Number(1.0))
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(&Operator::Eq, &FieldValue::Number(1.0), &FieldValue::Null)
        );
    }

    #[test]
    fn test_execute_operation_eq_list() {
        assert_eq!(
            Ok(FieldValue::Bool(true)),
            execute_operation(
                &Operator::Eq,
                &FieldValue::List(vec![
                    FieldValue::Number(1.0),
                    FieldValue::String("test".to_string())
                ]),
                &FieldValue::List(vec![
                    FieldValue::Number(1.0),
                    FieldValue::String("test".to_string())
                ]),
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::Eq,
                &FieldValue::List(vec![
                    FieldValue::Number(1.0),
                    FieldValue::String("test".to_string())
                ]),
                &FieldValue::List(vec![
                    FieldValue::Number(2.0),
                    FieldValue::String("test".to_string())
                ]),
            )
        );

        assert_eq!(
            Ok(FieldValue::Bool(false)),
            execute_operation(
                &Operator::Eq,
                &FieldValue::List(vec![
                    FieldValue::Number(1.0),
                    FieldValue::String("test".to_string())
                ]),
                &FieldValue::List(vec![
                    FieldValue::Number(1.0),
                    FieldValue::String("bla".to_string())
                ]),
            )
        );
    }

    #[test]
    fn test_execute_operation_neq() {
        let elements = [
            FieldValue::Number(1.0),
            FieldValue::String("value".to_string()),
            FieldValue::Bool(true),
        ];
        let different_elements = [
            FieldValue::Number(2.0),
            FieldValue::String("different value".to_string()),
            FieldValue::Bool(false),
        ];

        for (el, diff_el) in elements.iter().zip(different_elements.iter()) {
            assert_eq!(
                Ok(FieldValue::Bool(false)),
                execute_operation(&Operator::Neq, &el.clone(), &el.clone())
            );

            assert_eq!(
                Ok(FieldValue::Bool(true)),
                execute_operation(&Operator::Neq, &el.clone(), diff_el)
            );
        }
    }

    #[test]
    fn test_execute_operation_plus() {
        let elements = [
            FieldValue::Number(1.0),
            FieldValue::String("value".to_string()),
            FieldValue::List(vec![
                FieldValue::Number(1.0),
                FieldValue::String("value".to_string()),
            ]),
        ];
        let different_elements = [
            FieldValue::Number(2.0),
            FieldValue::String("different value".to_string()),
            FieldValue::List(vec![
                FieldValue::Number(2.0),
                FieldValue::String("different value".to_string()),
            ]),
        ];
        let results = [
            FieldValue::Number(3.0),
            FieldValue::String("valuedifferent value".to_string()),
            FieldValue::List(vec![
                FieldValue::Number(1.0),
                FieldValue::String("value".to_string()),
                FieldValue::Number(2.0),
                FieldValue::String("different value".to_string()),
            ]),
        ];

        for ((el, diff_el), res) in elements
            .iter()
            .zip(different_elements.iter())
            .zip(results.iter())
        {
            assert_eq!(
                Ok(res.clone()),
                execute_operation(&Operator::Plus, &el.clone(), diff_el)
            );
        }

        assert!(execute_operation(
            &Operator::Plus,
            &FieldValue::Bool(true),
            &FieldValue::Bool(false)
        )
        .is_err());
    }

    #[test]
    fn test_execute_operation_minus() {
        let elements = [
            FieldValue::Number(1.0),
            FieldValue::List(vec![
                FieldValue::Number(1.0),
                FieldValue::String("value".to_string()),
            ]),
        ];
        let different_elements = [
            FieldValue::Number(2.0),
            FieldValue::List(vec![
                FieldValue::Number(2.0),
                FieldValue::String("value".to_string()),
            ]),
        ];
        let results = [
            FieldValue::Number(-1.0),
            FieldValue::List(vec![FieldValue::Number(1.0)]),
        ];

        for ((el, diff_el), res) in elements
            .iter()
            .zip(different_elements.iter())
            .zip(results.iter())
        {
            assert_eq!(
                Ok(res.clone()),
                execute_operation(&Operator::Minus, &el.clone(), diff_el)
            );
        }

        assert!(execute_operation(
            &Operator::Minus,
            &FieldValue::Bool(true),
            &FieldValue::Bool(false)
        )
        .is_err());

        assert!(execute_operation(
            &Operator::Minus,
            &FieldValue::String("value".to_string()),
            &FieldValue::String("value".to_string()),
        )
        .is_err());
    }

    #[test]
    fn test_execute_operation_multiply() {
        assert_eq!(
            Ok(FieldValue::Number(2.0)),
            execute_operation(
                &Operator::Multiply,
                &FieldValue::Number(1.0),
                &FieldValue::Number(2.0)
            )
        );

        let elements = [
            FieldValue::String("value".to_string()),
            FieldValue::Bool(true),
            FieldValue::List(vec![
                FieldValue::Number(1.0),
                FieldValue::String("value".to_string()),
            ]),
        ];

        for el in elements.iter() {
            assert!(execute_operation(&Operator::Multiply, &el.clone(), &el.clone()).is_err());
        }
    }

    #[test]
    fn test_execute_operation_divide() {
        assert_eq!(
            Ok(FieldValue::Number(2.5)),
            execute_operation(
                &Operator::Divide,
                &FieldValue::Number(5.0),
                &FieldValue::Number(2.0)
            )
        );

        let elements = [
            FieldValue::String("value".to_string()),
            FieldValue::Bool(true),
            FieldValue::List(vec![
                FieldValue::Number(1.0),
                FieldValue::String("value".to_string()),
            ]),
        ];

        for el in elements.iter() {
            assert!(execute_operation(&Operator::Divide, &el.clone(), &el.clone()).is_err());
        }
    }

    #[test]
    fn test_execute_operation_power() {
        assert_eq!(
            Ok(FieldValue::Number(16.0)),
            execute_operation(
                &Operator::Power,
                &FieldValue::Number(4.0),
                &FieldValue::Number(2.0)
            )
        );

        let elements = [
            FieldValue::String("value".to_string()),
            FieldValue::Bool(true),
            FieldValue::List(vec![
                FieldValue::Number(1.0),
                FieldValue::String("value".to_string()),
            ]),
        ];

        for el in elements.iter() {
            assert!(execute_operation(&Operator::Power, &el.clone(), &el.clone()).is_err());
        }
    }

    #[test]
    fn test_execute_operation_floor_divide() {
        assert_eq!(
            Ok(FieldValue::Number(2.0)),
            execute_operation(
                &Operator::FloorDivide,
                &FieldValue::Number(5.0),
                &FieldValue::Number(2.0)
            )
        );

        let elements = [
            FieldValue::String("value".to_string()),
            FieldValue::Bool(true),
            FieldValue::List(vec![
                FieldValue::Number(1.0),
                FieldValue::String("value".to_string()),
            ]),
        ];

        for el in elements.iter() {
            assert!(execute_operation(&Operator::FloorDivide, &el.clone(), &el.clone()).is_err());
        }
    }

    /***************************************************************************************************
     * TESTS for get_field_value
     * *************************************************************************************************/
    #[test]
    fn test_get_field_value() {
        let mut pod = Pod::new_hash();
        let key: String = "a".to_string();
        let value = 1;
        let _ = pod.insert(key.clone(), value);

        assert_eq!(
            FieldValue::Number(value as f64),
            get_field_value(&key, &pod)
        );

        assert_eq!(FieldValue::Null, get_field_value("b", &pod));
    }

    /***************************************************************************************************
     * TESTS for get_nested_pod
     * *************************************************************************************************/
    #[test]
    fn test_get_nested_pod() {
        let mut nested_pod = Pod::new_hash();
        let nested_key = "b".to_string();
        let nested_value = 2;
        let _ = nested_pod.insert(nested_key.clone(), nested_value);

        let mut pod = Pod::new_hash();
        let key = "a".to_string();
        let _ = pod.insert(key.clone(), nested_pod.clone());

        assert_eq!(Some(nested_pod), get_nested_pod("a", &pod));
        assert_eq!(
            Some(Pod::Integer(nested_value)),
            get_nested_pod(&format!("{}.{}", key, nested_key), &pod)
        );

        assert_eq!(None, get_nested_pod("b", &pod));
        assert_eq!(None, get_nested_pod("a.c", &pod));
    }

    /***************************************************************************************************
     * TESTS for pod_array_to_field_value
     * *************************************************************************************************/
    #[test]
    fn test_pod_array_to_field_value() {
        let mut pod = Pod::new_array();
        let value1 = 1;
        let value2 = 2;
        let _ = pod.push(Pod::Integer(value1));
        let _ = pod.push(Pod::Integer(value2));

        assert_eq!(
            FieldValue::List(vec![
                FieldValue::Number(value1 as f64),
                FieldValue::Number(value2 as f64)
            ]),
            pod_array_to_field_value(&pod.as_vec().unwrap())
        );

        assert_ne!(
            FieldValue::List(vec![
                FieldValue::Number(value1 as f64),
                FieldValue::Number(value1 as f64)
            ]),
            pod_array_to_field_value(&pod.as_vec().unwrap())
        );
    }

    #[test]
    fn test_pod_array_to_field_value_nested() {
        let value1 = 1;
        let value2 = 2;

        let mut nested_pod = Pod::new_array();
        let _ = nested_pod.push(Pod::Integer(value1));
        let _ = nested_pod.push(Pod::Integer(value2));

        let mut nested_pod2 = Pod::new_hash();
        let _ = nested_pod2.insert("a".to_string(), Pod::Integer(value1));

        let mut pod = Pod::new_array();
        let _ = pod.push(nested_pod.clone());
        let _ = pod.push(nested_pod2.clone());

        assert_eq!(
            FieldValue::List(vec![
                FieldValue::List(vec![
                    FieldValue::Number(value1 as f64),
                    FieldValue::Number(value2 as f64)
                ]),
                FieldValue::String(format!("{{\"a\":{}}}", value1))
            ]),
            pod_array_to_field_value(&pod.as_vec().unwrap())
        );
    }

    /***************************************************************************************************
     * TESTS for pod_hash_to_field_value
     * *************************************************************************************************/
    #[test]
    fn test_pod_hash_to_field_value() {
        let key1 = "a".to_string();
        let key2 = "b".to_string();
        let value1 = 1;
        let value2 = 2;

        let mut nested_pod = Pod::new_hash();
        let _ = nested_pod.insert(key1.clone(), Pod::Integer(value1));
        let _ = nested_pod.insert(key2.clone(), Pod::Integer(value2));

        let mut pod = Pod::new_hash();
        let _ = pod.insert(key1.clone(), nested_pod.clone());

        assert_eq!(
            FieldValue::String(format!(
                "{{\"{}\":{{\"{}\":{},\"{}\":{}}}}}",
                key1, key1, value1, key2, value2
            )),
            pod_hash_to_field_value(&pod.as_hashmap().unwrap())
        );
    }

    /***************************************************************************************************
     * TESTS for execute_function
     * *************************************************************************************************/
    #[test]
    fn test_execute_function() {
        let pod = Pod::new_hash();

        let func = Function {
            name: "DATE".to_string(),
            args: vec![FunctionArg::FieldValue(FieldValue::String(
                "2024-12-30".to_string(),
            ))],
        };

        assert_eq!(
            Ok(FieldValue::String("2024-12-30T00:00:00".to_string())),
            execute_function(&func, &pod)
        );

        assert!(execute_function(
            &Function {
                name: "UNKNOWN".to_string(),
                args: vec![],
            },
            &pod
        )
        .is_err());
    }

    /***************************************************************************************************
     * TESTS for execute_function_date_add
     * *************************************************************************************************/
    #[test]
    fn test_execute_function_date_add() {
        let pod = Pod::new_hash();

        let func = Function {
            name: "DATEADD".to_string(),
            args: vec![
                FunctionArg::FieldValue(FieldValue::String("YEAR".to_string())),
                FunctionArg::FieldValue(FieldValue::Number(1.0)),
                FunctionArg::FieldValue(FieldValue::String("2024-12-30".to_string())),
            ],
        };

        assert_eq!(
            Ok(FieldValue::String("2025-12-30T00:00:00".to_string())),
            execute_function_date_add(&func, &pod)
        );
    }

    #[test]
    fn test_execute_function_date_add_with_pod() {
        let mut pod = Pod::new_hash();
        let _ = pod.insert("interval".to_string(), Pod::String("YEAR".to_string()));
        let _ = pod.insert("value".to_string(), Pod::Integer(1));
        let _ = pod.insert("date".to_string(), Pod::String("2024-12-30".to_string()));

        let func = Function {
            name: "DATEADD".to_string(),
            args: vec![
                FunctionArg::FieldName("interval".to_string()),
                FunctionArg::FieldName("value".to_string()),
                FunctionArg::FieldName("date".to_string()),
            ],
        };

        assert_eq!(
            Ok(FieldValue::String("2025-12-30T00:00:00".to_string())),
            execute_function_date_add(&func, &pod)
        );
    }

    #[test]
    fn test_execute_function_date_add_with_pod_and_format() {
        let mut pod = Pod::new_hash();
        let _ = pod.insert("interval".to_string(), Pod::String("YEAR".to_string()));
        let _ = pod.insert("value".to_string(), Pod::Integer(1));
        let _ = pod.insert("date".to_string(), Pod::String("2024-12+30".to_string()));
        let _ = pod.insert("format".to_string(), Pod::String("%Y-%m+%d".to_string()));

        let func = Function {
            name: "DATEADD".to_string(),
            args: vec![
                FunctionArg::FieldName("interval".to_string()),
                FunctionArg::FieldName("value".to_string()),
                FunctionArg::FieldName("date".to_string()),
                FunctionArg::FieldName("format".to_string()),
            ],
        };

        assert_eq!(
            Ok(FieldValue::String("2025-12-30T00:00:00".to_string())),
            execute_function_date_add(&func, &pod)
        );
    }

    #[test]
    fn test_execute_function_date_add_invalid_first_arg() {
        let pod = Pod::new_hash();
        let func = Function {
            name: "DATEADD".to_string(),
            args: vec![
                FunctionArg::FieldValue(FieldValue::Number(1.0)),
                FunctionArg::FieldValue(FieldValue::Number(1.0)),
                FunctionArg::FieldValue(FieldValue::String("2024-12-30".to_string())),
            ],
        };

        assert!(execute_function_date_add(&func, &pod).is_err());
    }

    #[test]
    fn test_execute_function_date_add_invalid_interval() {
        let pod = Pod::new_hash();
        let func = Function {
            name: "DATEADD".to_string(),
            args: vec![
                FunctionArg::FieldValue(FieldValue::String("INVALID".to_string())),
                FunctionArg::FieldValue(FieldValue::Number(1.0)),
                FunctionArg::FieldValue(FieldValue::String("2024-12-30".to_string())),
            ],
        };
        assert!(execute_function_date_add(&func, &pod).is_err());
    }

    /***************************************************************************************************
     * TESTS for execute_function_date
     * *************************************************************************************************/
    #[test]
    fn test_execute_function_date() {
        let pod = Pod::new_hash();

        let func = Function {
            name: "DATE".to_string(),
            args: vec![FunctionArg::FieldValue(FieldValue::String(
                "2024-12-30".to_string(),
            ))],
        };

        assert_eq!(
            Ok(FieldValue::String("2024-12-30T00:00:00".to_string())),
            execute_function_date(&func, &pod)
        );
    }

    #[test]
    fn test_execute_function_date_with_pod() {
        let mut pod = Pod::new_hash();
        let _ = pod.insert("date".to_string(), Pod::String("2024-12-30".to_string()));

        let func = Function {
            name: "DATE".to_string(),
            args: vec![FunctionArg::FieldName("date".to_string())],
        };

        assert_eq!(
            Ok(FieldValue::String("2024-12-30T00:00:00".to_string())),
            execute_function_date(&func, &pod)
        );
    }

    #[test]
    fn test_execute_function_date_with_pod_and_format() {
        let mut pod = Pod::new_hash();
        let _ = pod.insert("date".to_string(), Pod::String("2024-12+30".to_string()));
        let _ = pod.insert("format".to_string(), Pod::String("%Y-%m+%d".to_string()));

        let func = Function {
            name: "DATE".to_string(),
            args: vec![
                FunctionArg::FieldName("date".to_string()),
                FunctionArg::FieldName("format".to_string()),
            ],
        };

        assert_eq!(
            Ok(FieldValue::String("2024-12-30T00:00:00".to_string())),
            execute_function_date(&func, &pod)
        );
    }

    /***************************************************************************************************
     * TESTS for parse_naive_datetime
     * *************************************************************************************************/
}
