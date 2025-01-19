use crate::libs::peekable_deque::PeekableDeque;
use core::f64;
use hashbrown::HashSet;
use std::str::FromStr;

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    And,
    Or,
    In,
    Lt,
    Lte,
    Gt,
    Gte,
    Eq,
    Neq,
    // Like,
    Plus,
    Minus,
    Multiply,
    Divide,
    Power,
    FloorDivide,
}

impl Operator {
    const OPERATOR_MAP: phf::Map<&'static str, Operator> = phf::phf_map! {
        "AND" => Operator::And,
        "OR" => Operator::Or,
        "IN" => Operator::In,
        "<" => Operator::Lt,
        "<=" => Operator::Lte,
        ">" => Operator::Gt,
        ">=" => Operator::Gte,
        "==" => Operator::Eq,
        "!=" => Operator::Neq,
        //"LIKE" => Operator::Like,
        "+" => Operator::Plus,
        "-" => Operator::Minus,
        "*" => Operator::Multiply,
        "/" => Operator::Divide,
        "**" => Operator::Power,
        "//" => Operator::FloorDivide,
    };

    pub fn get_operator_first_chars() -> String {
        Self::OPERATOR_MAP
            .keys()
            .map(|s| s.chars().next().unwrap())
            .collect::<String>()
    }

    pub fn strings_hash() -> HashSet<&'static str> {
        Self::OPERATOR_MAP.keys().cloned().collect()
    }
}

impl FromStr for Operator {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        //Option<Self> {
        match Self::OPERATOR_MAP.get(s.to_uppercase().as_str()).cloned() {
            Some(op) => Ok(op),
            None => Err(format!("Unknown operator: {}", s)),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ExpressionElement {
    OpenedBracket,
    ClosedBracket,
    Operator(Operator),
    FieldName(String),
    FieldValue(FieldValue),
    Function(Function),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Function {
    pub name: String,
    pub args: Vec<FunctionArg>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum FunctionArg {
    FieldName(String),
    FieldValue(FieldValue),
}

impl Function {
    pub fn new(name: String, args: Vec<FunctionArg>) -> Self {
        Function { name, args }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum FieldValue {
    String(String),
    Number(f64),
    Bool(bool),
}

#[derive(Debug)]
pub struct OrderByFieldOption {
    pub field_name: String,
    pub order_direction: OrderDirection,
}

#[derive(Debug)]
pub enum OrderDirection {
    ASC,
    DESC,
}

#[derive(Debug)]
pub struct Query {
    pub select_fields: Vec<String>,
    pub from_tables: Vec<ExpressionElement>,
    pub where_expression: Vec<ExpressionElement>,
    pub order_by_fields: Vec<OrderByFieldOption>,
}

impl FromStr for Query {
    type Err = String;

    fn from_str(query: &str) -> Result<Self, Self::Err> {
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        let select_fields = match Query::parse_select(&mut peekable_query) {
            Ok(sf) => sf,
            Err(error) => return Err(format!("Error: {}, Query: {:?}", error, peekable_query)),
        };

        let from_tables = match Query::parse_from(&mut peekable_query) {
            Ok(ft) => ft,
            Err(error) => return Err(format!("Error: {}, Query: {:?}", error, peekable_query)),
        };

        let mut where_expression = Vec::new();
        if let Some(&peeked_char) = peekable_query.peek() {
            if peeked_char == 'w' || peeked_char == 'W' {
                where_expression = match Query::parse_where(&mut peekable_query) {
                    Ok(we) => we,
                    Err(error) => {
                        return Err(format!("Error: {}, Query: {:?}", error, peekable_query))
                    }
                };
            }
        }

        let mut order_by_fields = Vec::new();
        if let Some(&peeked_char) = peekable_query.peek() {
            if peeked_char == 'o' || peeked_char == 'O' {
                order_by_fields = match Query::parse_order_by(&mut peekable_query) {
                    Ok(ob) => ob,
                    Err(error) => {
                        return Err(format!("Error: {}, Query: {:?}", error, peekable_query))
                    }
                };
            }
        }

        //if let Some(&peeked_char) = peekable_query.peek() {
        //    return Err(format!("Unexpected character: {}", peeked_char));
        //}

        Ok(Query::new(
            select_fields,
            from_tables,
            where_expression,
            order_by_fields,
        ))
    }
}

impl Query {
    pub fn new(
        select_fields: Vec<String>,
        from_tables: Vec<ExpressionElement>,
        where_expression: Vec<ExpressionElement>,
        order_by_fields: Vec<OrderByFieldOption>,
    ) -> Self {
        Query {
            select_fields,
            from_tables,
            where_expression,
            order_by_fields,
        }
    }

    fn parse_select(peekable_query: &mut PeekableDeque<char>) -> Result<Vec<String>, String> {
        match Query::parse_keyword(peekable_query, "SELECT", false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        let mut select_fields: Vec<String> = Vec::new();

        loop {
            Query::parse_whitespaces(peekable_query);

            match Query::parse_field_name(peekable_query) {
                Ok(field_name) => select_fields.push(field_name),
                Err(error) => return Err(error),
            }

            Query::parse_whitespaces(peekable_query);

            if let Some(&peeked_char) = peekable_query.peek() {
                if peeked_char != ',' {
                    break;
                }
            }

            peekable_query.next();
        }

        Ok(select_fields)
    }

    fn parse_from(
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<Vec<ExpressionElement>, String> {
        match Query::parse_keyword(peekable_query, "FROM", false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        Query::parse_whitespaces(peekable_query);
        let mut from_expression: Vec<ExpressionElement> = Vec::new();

        match Query::parse_expression(peekable_query, &mut from_expression) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        Ok(from_expression)
    }

    // call only when you expect WHERE should happen
    fn parse_where(
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<Vec<ExpressionElement>, String> {
        match Query::parse_keyword(peekable_query, "WHERE", false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }
        Query::parse_whitespaces(peekable_query);

        let mut where_expression: Vec<ExpressionElement> = Vec::new();

        match Query::parse_expression(peekable_query, &mut where_expression) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        Ok(where_expression)
    }

    // call only when you expect ORDER BY should happen
    fn parse_order_by(
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<Vec<OrderByFieldOption>, String> {
        match Query::parse_keyword(peekable_query, "ORDER BY", false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        Query::parse_whitespaces(peekable_query);

        let mut order_by_fields: Vec<OrderByFieldOption> = Vec::new();
        order_by_fields.push(OrderByFieldOption {
            field_name: "".to_string(),
            order_direction: OrderDirection::ASC,
        });
        // TODO: Implement ORDER BY parsing
        Ok(order_by_fields)
    }

    fn parse_expression(
        peekable_query: &mut PeekableDeque<char>,
        expression_elements: &mut Vec<ExpressionElement>,
    ) -> Result<(), String> {
        if let Some(&peeked_char) = peekable_query.peek() {
            if peeked_char == '(' {
                match Query::parse_bracket_expression(peekable_query, expression_elements) {
                    Ok(()) => {}
                    Err(error) => return Err(error),
                }
            } else {
                match Query::parse_no_bracket_expression(peekable_query, expression_elements) {
                    Ok(()) => {}
                    Err(error) => return Err(error),
                }
            }
        } else {
            return Err("Expected expression, but found nothing".to_string());
        }
        Query::parse_whitespaces(peekable_query);

        Ok(())
    }

    fn parse_bracket_expression(
        peekable_query: &mut PeekableDeque<char>,
        expression_elements: &mut Vec<ExpressionElement>,
    ) -> Result<(), String> {
        if let Some(&peeked_char) = peekable_query.peek() {
            if peeked_char != '(' {
                return Err(format!("Expected a '(', but found: {}", peeked_char));
            }
        }
        expression_elements.push(ExpressionElement::OpenedBracket);
        peekable_query.next();
        Query::parse_whitespaces(peekable_query);

        match Query::parse_expression(peekable_query, expression_elements) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        if let Some(&peeked_char) = peekable_query.peek() {
            if peeked_char != ')' {
                return Err(format!("Expected a ')', but found: {}", peeked_char));
            }
        } else {
            return Err("Expected a ')', but found nothing".to_string());
        }
        expression_elements.push(ExpressionElement::ClosedBracket);
        peekable_query.next();
        Query::parse_whitespaces(peekable_query);

        match Query::parse_operator(peekable_query) {
            Ok(op) => expression_elements.push(ExpressionElement::Operator(op)),
            Err(_) => return Ok(()),
        }
        Query::parse_whitespaces(peekable_query);

        Query::parse_expression(peekable_query, expression_elements)
    }

    fn parse_no_bracket_expression(
        peekable_query: &mut PeekableDeque<char>,
        expression_elements: &mut Vec<ExpressionElement>,
    ) -> Result<(), String> {
        match Query::parse_bool_field_name_or_function(peekable_query) {
            Ok(field_name_or_function) => expression_elements.push(field_name_or_function),
            Err(_) => match Query::parse_field_value(peekable_query) {
                Ok(fv) => expression_elements.push(ExpressionElement::FieldValue(fv)),
                Err(_) => return Err("No FieldValue, Function, nor FieldName found!".to_string()),
            },
        }
        Query::parse_whitespaces(peekable_query);

        loop {
            match Query::parse_operator(peekable_query) {
                Ok(op) => expression_elements.push(ExpressionElement::Operator(op)),
                Err(_) => return Ok(()),
            }
            Query::parse_whitespaces(peekable_query);

            match Query::parse_expression(peekable_query, expression_elements) {
                Ok(()) => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn parse_operator(peekable_query: &mut PeekableDeque<char>) -> Result<Operator, String> {
        let mut operator_candidates = Operator::strings_hash();
        let mut index = 0;

        while let Some(&peeked_char) = peekable_query.peek() {
            operator_candidates.retain(|op| {
                op.len() > index && op.chars().nth(index) == Some(peeked_char.to_ascii_uppercase())
            });

            if operator_candidates.len() == 1 {
                let op = operator_candidates
                    .iter()
                    .next()
                    .ok_or("Expected one operator but set was empty")?;

                if op.len() == index + 1 {
                    peekable_query.next(); // move to the next char
                    return op.parse();
                }
            }

            if operator_candidates.is_empty() {
                break;
            }

            peekable_query.next();
            index += 1;
        }

        Err("No operator".to_string())
    }

    fn parse_field_value(peekable_query: &mut PeekableDeque<char>) -> Result<FieldValue, String> {
        if let Ok(str) = Query::parse_string(peekable_query) {
            return Ok(FieldValue::String(str));
        }
        if let Ok(num) = Query::parse_number(peekable_query) {
            return Ok(FieldValue::Number(num));
        }
        if let Ok(bv) = Query::parse_bool(peekable_query) {
            return Ok(FieldValue::Bool(bv));
        }

        Err("No field value found!".to_string())
    }

    fn parse_string(peekable_query: &mut PeekableDeque<char>) -> Result<String, String> {
        if let Some(&peeked_char) = peekable_query.peek() {
            if peeked_char != '"' && peeked_char != '\'' {
                return Err(format!(
                    "Expected a quote symbol, but found: {}",
                    peeked_char
                ));
            }
        }
        let opened_quote = *peekable_query.peek().unwrap();
        peekable_query.next();

        let mut str = String::new();

        while let Some(&peeked_char) = peekable_query.peek() {
            if peeked_char == opened_quote {
                peekable_query.next();
                return Ok(str);
            }
            str.push(peeked_char);
            peekable_query.next();
        }

        Err(format!("Query ended before string ({}) was closed!", str))
    }

    fn parse_number(peekable_query: &mut PeekableDeque<char>) -> Result<f64, String> {
        let mut number = String::new();

        if let Some(&peeked_char) = peekable_query.peek() {
            // First char can be minus or a number
            if !peeked_char.is_numeric() && peeked_char != '-' {
                return Err(format!("Number can not start with {}!", peeked_char));
            }
            number.push(peeked_char);
            peekable_query.next();
        } else {
            return Err("Number expected. nothing found".to_string());
        }

        // if first char was -, then next one needs to be a number
        if number.chars().nth(0).unwrap() == '-' {
            if let Some(&peeked_char) = peekable_query.peek() {
                if !peeked_char.is_numeric() {
                    return Err(format!("Number can not start with {}!", peeked_char));
                }
                number.push(peeked_char);
                peekable_query.next();
            } else {
                return Err("Number expected. nothing found".to_string());
            }
        }

        let mut has_decimal = false;
        while let Some(&(mut peeked_char)) = peekable_query.peek() {
            if peeked_char == '.' || peeked_char == ',' {
                if has_decimal {
                    return Err("Can not have multiple decimal signs".to_string());
                }
                has_decimal = true;
                peeked_char = '.';
            } else if !peeked_char.is_numeric() {
                break;
            }
            number.push(peeked_char);
            peekable_query.next();
        }

        number.parse::<f64>().map_err(|e| e.to_string())
    }

    fn parse_bool(peekable_query: &mut PeekableDeque<char>) -> Result<bool, String> {
        Err("TODO: implement parse_bool".to_string())
    }

    fn parse_bool_field_name_or_function(
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<ExpressionElement, String> {
        let field_name = match Query::parse_field_name(peekable_query) {
            Ok(field_name) => field_name,
            Err(_) => return Err("No Function, nor FieldName found!".to_string()),
        };

        if let Some(&peeked_char) = peekable_query.peek() {
            if peeked_char == '(' {
                match Query::parse_function(peekable_query, Some(field_name)) {
                    Ok(func) => return Ok(ExpressionElement::Function(func)),
                    Err(error) => return Err(error),
                }
            }
        }

        if let Ok(bool_value) = field_name.parse::<bool>() {
            return Ok(ExpressionElement::FieldValue(FieldValue::Bool(bool_value)));
        }

        Ok(ExpressionElement::FieldName(field_name))
    }

    fn parse_function(
        peekable_query: &mut PeekableDeque<char>,
        _func_name: Option<String>,
    ) -> Result<Function, String> {
        let func_name = match _func_name {
            Some(_fn) => _fn,
            None => {
                // parse it
                match Query::parse_field_name(peekable_query) {
                    Ok(field_name) => field_name,
                    Err(error) => return Err(error),
                }
            }
        };

        let mut args = Vec::new();

        if let Some(&peeked_char) = peekable_query.peek() {
            if peeked_char == '(' {
                peekable_query.next();
            } else {
                return Err(format!("Expected '(', but found {}", peeked_char));
            }
        } else {
            return Err("Expected '(', but found nothing".to_string());
        }

        loop {
            Query::parse_whitespaces(peekable_query);

            if let Some(&peeked_char) = peekable_query.peek() {
                if peeked_char == ')' {
                    peekable_query.next();
                    break;
                }
            }

            // TODO: implement field name parsing as well
            match Query::parse_field_value(peekable_query) {
                Ok(fv) => args.push(FunctionArg::FieldValue(fv)),
                Err(error) => return Err(error),
            }

            // TODO: implement comma parsing for multiple arguments
        }

        Ok(Function::new(func_name, args))
    }

    fn parse_field_name(peekable_query: &mut PeekableDeque<char>) -> Result<String, String> {
        let mut field_name = String::new();

        if let Some(&peeked_char) = peekable_query.peek() {
            // First char can be letter or underscore
            if !peeked_char.is_alphabetic() && peeked_char != '_' {
                return Err(format!("Field name expected. They must start with letter, underscore or a minus, found: {}", peeked_char));
            }
            field_name.push(peeked_char);
            peekable_query.next();
        } else {
            return Err("Field name expected. nothing found".to_string());
        }

        while let Some(&peeked_char) = peekable_query.peek() {
            if !peeked_char.is_alphanumeric() && peeked_char != '_' && peeked_char != '-' {
                break;
            }
            field_name.push(peeked_char);
            peekable_query.next();
        }

        Ok(field_name)
    }

    fn parse_keyword(
        peekable_query: &mut PeekableDeque<char>,
        keyword: &str,
        case_sensitive: bool,
    ) -> Result<(), String> {
        let mut keyword_chars = keyword.chars();
        let mut matched = String::new();

        for expected_char in &mut keyword_chars {
            if let Some(&peeked_char) = peekable_query.peek() {
                matched.push(peeked_char);

                let match_condition = if case_sensitive {
                    peeked_char == expected_char
                } else {
                    peeked_char.to_ascii_lowercase() == expected_char.to_ascii_lowercase()
                };

                if !match_condition {
                    return Err(format!(
                        "Expected {}, but instead found: {}...",
                        keyword, matched
                    ));
                }
                peekable_query.next();
            } else {
                return Err(format!(
                    "Expected {}, but instead found: {}...",
                    keyword, matched
                ));
            }
        }

        if let Some(&c) = peekable_query.peek() {
            if !c.is_whitespace() {
                return Err(format!(
                    "Expected emptyspace after {} keyward, but found: {}...",
                    keyword, c
                ));
            }
        }
        Ok(())
    }

    fn parse_whitespaces(peekable_query: &mut PeekableDeque<char>) {
        loop {
            if let Some(&c) = peekable_query.peek() {
                if !c.is_whitespace() {
                    return;
                }
                peekable_query.next();
            } else {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::panic;

    #[ignore = "TODO: implement this test"]
    #[test]
    fn parse_order_by() {}

    #[ignore = "TODO: implement this test"]
    #[test]
    fn parse_where() {}

    #[ignore = "TODO: implement this test"]
    #[test]
    fn parse_from() {}

    #[ignore = "TODO: implement this test"]
    #[test]
    fn parse_select() {}

    #[ignore = "TODO: implement this test"]
    #[test]
    fn parse_bracket_expression() {}

    #[ignore = "TODO: implement this test"]
    #[test]
    fn parse_expression() {}

    /////////////////////////////////////
    // PARSE FUNCTION
    /////////////////////////////////////
    #[ignore = "TODO: implement bool parsing, and comma parsing"]
    #[test]
    fn test_parse_function_with_name_multiple_args() -> Result<(), String> {
        let func_name = "test".to_string();

        let arg1: f64 = 5.5;
        let arg2_str = "some str".to_string();
        let arg2 = format!("'{}'", arg2_str);
        let arg3 = true;

        let query = format!("{}({}, {}, {}) ", func_name, arg1, arg2, arg3);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_function(&mut peekable_query, None) {
            Ok(func) => assert_eq!(
                Function::new(
                    func_name,
                    vec![
                        FunctionArg::FieldValue(FieldValue::Number(arg1)),
                        FunctionArg::FieldValue(FieldValue::String(arg2_str)),
                        FunctionArg::FieldValue(FieldValue::Bool(arg3))
                    ]
                ),
                func
            ),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[ignore = "TODO: implement field name parsing in function"]
    #[test]
    fn test_parse_function_with_name_one_fn_args() -> Result<(), String> {
        let func_name = "test".to_string();
        let arg1 = "field".to_string();
        let query = format!("{}({}) ", func_name, arg1);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_function(&mut peekable_query, None) {
            Ok(func) => assert_eq!(
                Function::new(func_name, vec![FunctionArg::FieldName(arg1)]),
                func
            ),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_function_with_name_one_fv_args() -> Result<(), String> {
        let func_name = "test".to_string();
        let arg1: f64 = 5.5;
        let query = format!("{}({}) ", func_name, arg1);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_function(&mut peekable_query, None) {
            Ok(func) => assert_eq!(
                Function::new(
                    func_name,
                    vec![FunctionArg::FieldValue(FieldValue::Number(arg1))]
                ),
                func
            ),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_function_without_name_no_args() -> Result<(), String> {
        let func_name = "test".to_string();
        let query = "() ".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_function(&mut peekable_query, Some(func_name.clone())) {
            Ok(func) => assert_eq!(Function::new(func_name, Vec::new()), func),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_function_with_name_no_args() -> Result<(), String> {
        let func_name = "test".to_string();
        let query = format!("{}() ", func_name);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_function(&mut peekable_query, None) {
            Ok(func) => assert_eq!(Function::new(func_name, Vec::new()), func),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    /////////////////////////////////////
    // PARSE FIELD VALUE
    /////////////////////////////////////
    #[ignore = "TODO: implement bool parsing"]
    #[test]
    fn test_parse_field_value_when_bool() -> Result<(), String> {
        let bool_value = false;
        let query = format!("{} ", bool_value);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_field_value(&mut peekable_query) {
            Ok(fv) => assert_eq!(FieldValue::Bool(bool_value), fv),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_field_value_when_string() -> Result<(), String> {
        let str = "test".to_string();
        let query = format!("'{}' ", str);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_field_value(&mut peekable_query) {
            Ok(fv) => assert_eq!(FieldValue::String(str), fv),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_field_value_when_number() -> Result<(), String> {
        let num: f64 = 541.0;
        let query = format!("{} ", num);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_field_value(&mut peekable_query) {
            Ok(fv) => assert_eq!(FieldValue::Number(num), fv),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    /////////////////////////////////////
    // PARSE NO BRACKET EXPRESSION
    /////////////////////////////////////
    #[test]
    fn test_parse_no_bracket_expression_with_operator() -> Result<(), String> {
        let field_name = "kifla".to_string();
        let bool_value = false;
        let query = format!("{} and {}", field_name, bool_value);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        let mut expression_elements: Vec<ExpressionElement> = Vec::new();

        assert_eq!(
            Ok(()),
            Query::parse_no_bracket_expression(&mut peekable_query, &mut expression_elements)
        );
        assert_eq!(
            vec![
                ExpressionElement::FieldName(field_name),
                ExpressionElement::Operator(Operator::And),
                ExpressionElement::FieldValue(FieldValue::Bool(bool_value))
            ],
            expression_elements
        );

        Ok(())
    }

    #[test]
    fn test_parse_no_bracket_expression_when_field_name() -> Result<(), String> {
        let field_name = "truea".to_string();
        let query = format!("{} ", field_name);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        let mut expression_elements: Vec<ExpressionElement> = Vec::new();

        assert_eq!(
            Ok(()),
            Query::parse_no_bracket_expression(&mut peekable_query, &mut expression_elements)
        );
        assert_eq!(
            vec![ExpressionElement::FieldName(field_name)],
            expression_elements
        );

        Ok(())
    }

    #[test]
    fn test_parse_no_bracket_expression_when_func() -> Result<(), String> {
        let func_name = "true".to_string();
        let query = format!("{}() ", func_name);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        let mut expression_elements: Vec<ExpressionElement> = Vec::new();

        assert_eq!(
            Ok(()),
            Query::parse_no_bracket_expression(&mut peekable_query, &mut expression_elements)
        );
        assert_eq!(
            vec![ExpressionElement::Function(Function::new(
                func_name,
                Vec::new()
            ))],
            expression_elements
        );

        Ok(())
    }

    #[test]
    fn test_parse_no_bracket_expression_when_bool() -> Result<(), String> {
        let bool_value = false;
        let query = format!("{} ", bool_value);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        let mut expression_elements: Vec<ExpressionElement> = Vec::new();

        assert_eq!(
            Ok(()),
            Query::parse_no_bracket_expression(&mut peekable_query, &mut expression_elements)
        );
        assert_eq!(
            vec![ExpressionElement::FieldValue(FieldValue::Bool(bool_value))],
            expression_elements
        );

        Ok(())
    }

    #[test]
    fn test_parse_no_bracket_expression_when_string() -> Result<(), String> {
        let str = "test".to_string();
        let query = format!("'{}' ", str);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        let mut expression_elements: Vec<ExpressionElement> = Vec::new();

        assert_eq!(
            Ok(()),
            Query::parse_no_bracket_expression(&mut peekable_query, &mut expression_elements)
        );
        assert_eq!(
            vec![ExpressionElement::FieldValue(FieldValue::String(str))],
            expression_elements
        );

        Ok(())
    }

    #[test]
    fn test_parse_no_bracket_expression_when_number() -> Result<(), String> {
        let num: f64 = 541.0;
        let query = format!("{} ", num);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        let mut expression_elements: Vec<ExpressionElement> = Vec::new();

        assert_eq!(
            Ok(()),
            Query::parse_no_bracket_expression(&mut peekable_query, &mut expression_elements)
        );
        assert_eq!(
            vec![ExpressionElement::FieldValue(FieldValue::Number(num))],
            expression_elements
        );

        Ok(())
    }

    /////////////////////////////////////
    // PARSE BOOL FIELD NAME OR FUNCTION
    /////////////////////////////////////
    #[test]
    fn test_parse_bool_field_name_or_function_when_field_name() -> Result<(), String> {
        let field_name = "truea".to_string();
        let query = format!("{} ", field_name);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_bool_field_name_or_function(&mut peekable_query) {
            Ok(_field_name) => assert_eq!(ExpressionElement::FieldName(field_name), _field_name),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_bool_field_name_or_function_when_function() -> Result<(), String> {
        let func_name = "true".to_string();
        let query = format!("{}() ", func_name);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_bool_field_name_or_function(&mut peekable_query) {
            Ok(_func) => assert_eq!(
                ExpressionElement::Function(Function::new(func_name, Vec::new())),
                _func
            ),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_bool_field_name_or_function_when_false() -> Result<(), String> {
        let bool_value = false;
        let query = format!("{} ", bool_value);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_bool_field_name_or_function(&mut peekable_query) {
            Ok(_bool_value) => assert_eq!(
                ExpressionElement::FieldValue(FieldValue::Bool(bool_value)),
                _bool_value
            ),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_bool_field_name_or_function_when_true() -> Result<(), String> {
        let bool_value = true;
        let query = format!("{} ", bool_value);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_bool_field_name_or_function(&mut peekable_query) {
            Ok(_bool_value) => assert_eq!(
                ExpressionElement::FieldValue(FieldValue::Bool(bool_value)),
                _bool_value
            ),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    /////////////////////////////////////
    // PARSE NUMBER
    /////////////////////////////////////
    #[test]
    fn test_parse_invalid_decimal_number() -> Result<(), String> {
        let query = "5.3.2".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        if Query::parse_number(&mut peekable_query).is_ok() {
            return Err("This should fail, because \"test\" is not a number".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_invalid_negative_number() -> Result<(), String> {
        let query = "-test".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        if Query::parse_number(&mut peekable_query).is_ok() {
            return Err("This should fail, because \"test\" is not a number".to_string());
        }

        assert_eq!('t', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_invalid_number() -> Result<(), String> {
        let query = "test".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        if Query::parse_number(&mut peekable_query).is_ok() {
            return Err("This should fail, because \"test\" is not a number".to_string());
        }

        assert_eq!('t', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_decimal_number_with_comma() -> Result<(), String> {
        let num: f64 = 543.21;
        let query = "543,21a".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_number(&mut peekable_query) {
            Ok(_num) => assert_eq!(num, _num),
            Err(error) => return Err(error),
        }

        assert_eq!('a', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_decimal_number_with_dot() -> Result<(), String> {
        let num: f64 = 543.21;
        let query = format!("{}a", num);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_number(&mut peekable_query) {
            Ok(_num) => assert_eq!(num, _num),
            Err(error) => return Err(error),
        }

        assert_eq!('a', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_negative_number() -> Result<(), String> {
        let num: f64 = -543.0;
        let query = format!("{}a", num);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_number(&mut peekable_query) {
            Ok(_num) => assert_eq!(num, _num),
            Err(error) => return Err(error),
        }

        assert_eq!('a', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_positive_number() -> Result<(), String> {
        let num: f64 = 543.0;
        let query = format!("{}a", num);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_number(&mut peekable_query) {
            Ok(_num) => assert_eq!(num, _num),
            Err(error) => return Err(error),
        }

        assert_eq!('a', *peekable_query.peek().unwrap());

        Ok(())
    }

    /////////////////////////////////////
    // PARSE STRING
    /////////////////////////////////////
    #[test]
    fn test_parse_string_without_opening_quote() -> Result<(), String> {
        let query = "test' and field > 5".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        if Query::parse_string(&mut peekable_query).is_ok() {
            return Err("This should fail, because string is not closed".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_string_without_closed_quote() -> Result<(), String> {
        let query = "'test and field > 5".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        if Query::parse_string(&mut peekable_query).is_ok() {
            return Err("This should fail, because string is not closed".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_string_with_mixed_quotes2() -> Result<(), String> {
        let query = "\"test'".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        if Query::parse_string(&mut peekable_query).is_ok() {
            return Err("This should fail, because string is not closed".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_string_with_mixed_quotes1() -> Result<(), String> {
        let query = "'test\"".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        if Query::parse_string(&mut peekable_query).is_ok() {
            return Err("This should fail, because string is not closed".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_string_with_single_quotes() -> Result<(), String> {
        let query = "'test'".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_string(&mut peekable_query) {
            Ok(str) => assert_eq!("test", str),
            Err(error) => return Err(error),
        }

        Ok(())
    }

    #[test]
    fn test_parse_string_with_double_quotes() -> Result<(), String> {
        let query = "\"test\"".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_string(&mut peekable_query) {
            Ok(str) => assert_eq!("test", str),
            Err(error) => return Err(error),
        }

        Ok(())
    }

    #[test]
    fn test_parse_valid_string_with_different_chars() -> Result<(), String> {
        let str = "o oeuaoe 45646 ?$%^ ";
        let query = format!("'{}'", str);
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_string(&mut peekable_query) {
            Ok(_str) => assert_eq!(str, _str),
            Err(error) => return Err(error),
        }

        Ok(())
    }

    /////////////////////////////////////
    // PARSE OPERATOR
    /////////////////////////////////////
    #[test]
    fn test_parse_existing_operator_with_space() -> Result<(), String> {
        let operator = "AND ".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(operator.chars());

        match Query::parse_operator(&mut peekable_query) {
            Ok(op) => assert_eq!(Operator::And, op),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_existing_operator_lowercase() -> Result<(), String> {
        let operator = "and ".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(operator.chars());

        match Query::parse_operator(&mut peekable_query) {
            Ok(op) => assert_eq!(Operator::And, op),
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_existing_operator_no_text() -> Result<(), String> {
        let operator = "<=".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(operator.chars());

        match Query::parse_operator(&mut peekable_query) {
            Ok(op) => assert_eq!(Operator::Lte, op),
            Err(error) => return Err(error),
        }

        Ok(())
    }

    #[test]
    fn test_parse_existing_operator() -> Result<(), String> {
        let operator = "AND".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(operator.chars());

        match Query::parse_operator(&mut peekable_query) {
            Ok(op) => assert_eq!(Operator::And, op),
            Err(error) => return Err(error),
        }

        Ok(())
    }

    #[test]
    fn test_parse_non_existing_long_operator() -> Result<(), String> {
        let operator = "ANDN".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(operator.chars());

        match Query::parse_operator(&mut peekable_query) {
            Ok(op) => assert_eq!(Operator::And, op),
            Err(error) => return Err(error),
        }

        assert_eq!('N', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_non_existing_short_operator_with_space() -> Result<(), String> {
        let operator = "A ".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(operator.chars());

        if Query::parse_operator(&mut peekable_query).is_ok() {
            return Err("It should fail since there is no operator ANN!".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_non_existing_short_operator() -> Result<(), String> {
        let operator = "A".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(operator.chars());

        if Query::parse_operator(&mut peekable_query).is_ok() {
            return Err("It should fail since there is no operator ANN!".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_non_existing_operator() -> Result<(), String> {
        let operator = "ANN".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(operator.chars());

        if Query::parse_operator(&mut peekable_query).is_ok() {
            return Err("It should fail since there is no operator ANN!".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_non_existing_operator_different_first_char() -> Result<(), String> {
        let operator = "NAN".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(operator.chars());

        if Query::parse_operator(&mut peekable_query).is_ok() {
            return Err("It should fail since there is no operator ANN!".to_string());
        }

        assert_eq!('N', *peekable_query.peek().unwrap());

        Ok(())
    }

    /////////////////////////////////////
    // PARSE FIELD NAME
    /////////////////////////////////////
    #[test]
    fn test_parse_field_name_first_char_num() -> Result<(), String> {
        let field_name = "5test".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(field_name.chars());

        if Query::parse_field_name(&mut peekable_query).is_ok() {
            return Err("It should fail since field name can't start with a number!".to_string());
        }

        Ok(())
    }

    /////////////////////////////////////
    // PARSE KEYWORD
    /////////////////////////////////////
    #[test]
    fn test_parse_keyword_case_sensitive() -> Result<(), String> {
        let query = "SeLeCt ".to_string();
        let keyword = "SELECT".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        if Query::parse_keyword(&mut peekable_query, &keyword, true).is_ok() {
            return Err(
                "It should fail since there is no match if we take into account case sensitivity!"
                    .to_string(),
            );
        }

        assert_eq!('e', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_keyword_exact_case_sensitive() -> Result<(), String> {
        let query = "SeLeCt ".to_string();
        let keyword = "SeLeCt".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_keyword(&mut peekable_query, &keyword, true) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_keyword_exact_case_insensitive() -> Result<(), String> {
        let query = "SELECT ".to_string();
        let keyword = "SeLeCt".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        match Query::parse_keyword(&mut peekable_query, &keyword, false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        assert_eq!(' ', *peekable_query.peek().unwrap());

        Ok(())
    }

    #[test]
    fn test_parse_keyword_start_with_whitespace() -> Result<(), String> {
        let query = "  SELECT".to_string();
        let keyword = "SELECT".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        if let Ok(()) = Query::parse_keyword(&mut peekable_query, &keyword, false) {
            return Err("It should fail since it is supposed to expect the keywoard and it has empty space in the beginning!".to_string());
        }

        Ok(())
    }

    /////////////////////////////////////
    // PARSE WHITESPACE
    /////////////////////////////////////
    #[test]
    fn test_parse_whitespaces_skip_whitspace() {
        let query = "  \t  \t\t\n  \t\n\n  a".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        Query::parse_whitespaces(&mut peekable_query);
        if let Some(&peeked_char) = peekable_query.peek() {
            assert_eq!('a', peeked_char);
        } else {
            panic!("Expected 'a' char, but got nothing!");
        }
    }

    #[test]
    fn test_parse_whitespaces_nothing_to_skip() {
        let query = "a  \t\t\n\n  ".to_string();
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(query.chars());

        Query::parse_whitespaces(&mut peekable_query);
        if let Some(&peeked_char) = peekable_query.peek() {
            assert_eq!('a', peeked_char);
        } else {
            panic!("Expected 'a' char, but got nothing!");
        }
    }
}
