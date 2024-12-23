use std::fmt::format;
use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug)]
pub struct QueryStatement {
    select_fields: Vec<String>,
    from_tables: Vec<FromExpressionElement>,
    where_expression: Vec<WhereExpressionElement>,
    order_by_fields: Vec<OrderByFieldOption>,
}

#[derive(Debug)]
pub enum FromExpressionElement {
    OpenedBracket,
    ClosedBracket,
    Tag(String),
    OperatorAnd,
    OperatorOr,
}

#[derive(Debug)]
pub enum WhereExpressionElement {
    OpenedBracket,
    ClosedBracket,
    FieldName(String),
    FieldValue,
    OperatorAnd,
    Operatorzor,
}

#[derive(Debug)]
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
pub enum OrderDirection { ASC, DESC, }

pub struct QueryParser {
    query: String,
}

impl QueryParser {
    pub fn new(query: String) -> Self {
        QueryParser { query }
    }

    pub fn parse(&self) -> Result<QueryStatement, String> {
        let mut peekable_query = self.query.chars().peekable();
        let mut query_statement = QueryStatement {
            select_fields: Vec::new(),
            from_tables: Vec::new(),
            where_expression: Vec::new(),
            order_by_fields: Vec::new(),
        };

        match self.parse_select(&mut peekable_query) {
            Ok(sf) => { query_statement.select_fields = sf },
            Err(error) => return Err(error)
        };

        match self.parse_from(&mut peekable_query) {
            Ok(ft) => { query_statement.from_tables = ft },
            Err(error) => return Err(error)
        };

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char == 'w' || *peeked_char == 'W' {
                match self.parse_where(&mut peekable_query) {
                    Ok(we) => { query_statement.where_expression = we },
                    Err(error) => return Err(error)
                };
            }
        }

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char == 'o' || *peeked_char == 'O' {
                match self.parse_order_by(&mut peekable_query) {
                    Ok(ob) => { query_statement.order_by_fields = ob },
                    Err(error) => return Err(error)
                };
            }
        }

        //if let Some(peeked_char) = peekable_query.peek() {
        //    return Err(format!("Unexpected character: {}", *peeked_char));
        //}

        
        Ok(query_statement)
    }

    fn parse_select(&self, peekable_query: &mut Peekable<Chars>) -> Result<Vec<String>, String> {
        match self.parse_keyword(peekable_query, "SELECT", false) {
            Ok(()) => {}
            Err(error) => return Err(error)
        }

        let mut select_fields: Vec<String> = Vec::new();

        loop {
            self.parse_whitespaces(peekable_query);

            match self.parse_field_name(peekable_query) {
                Ok(field_name) => { select_fields.push(field_name) }
                Err(error) => return Err(error)
            }

            self.parse_whitespaces(peekable_query);

            if let Some(peeked_char) = peekable_query.peek() {
                if *peeked_char != ',' {
                    break;
                }
            }

            peekable_query.next();
        }

        Ok(select_fields)
    }

    fn parse_from(&self, peekable_query: &mut Peekable<Chars>) -> Result<Vec<FromExpressionElement>, String> {
        match self.parse_keyword(peekable_query, "FROM", false) {
            Ok(()) => {}
            Err(error) => return Err(error)
        }

        self.parse_whitespaces(peekable_query);
        let mut from_expresion: Vec<FromExpressionElement> = Vec::new();

        match self.parse_from_expression(peekable_query, &mut from_expresion) {
            Ok(()) => {}
            Err(error) => return Err(error)
        }

        Ok(from_expresion)
    }

    fn parse_from_expression(&self, peekable_query: &mut Peekable<Chars>, from_expresion: &mut Vec<FromExpressionElement>) -> Result<(), String> {
        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char == '(' {
                match self.parse_from_bracket_expression(peekable_query, from_expresion) {
                    Ok(()) => {}
                    Err(error) => return Err(error)
                }
            } else if *peeked_char == '#' {
                match self.parse_from_tag_expression(peekable_query, from_expresion) {
                    Ok(()) => {}
                    Err(error) => return Err(error)
                }
            } else {
                return Err(format!("Expected '#' or a '(', but found: {}", *peeked_char));
            }
        }
        self.parse_whitespaces(peekable_query);

        Ok(())
    }

    fn parse_from_bracket_expression(&self, peekable_query: &mut Peekable<Chars>, from_expresion: &mut Vec<FromExpressionElement>) -> Result<(), String> {
        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char != '(' {
                return Err(format!("Expected a '(', but found: {}", *peeked_char));
            }
        }
        from_expresion.push(FromExpressionElement::OpenedBracket);
        peekable_query.next();
        self.parse_whitespaces(peekable_query);

        match self.parse_from_expression(peekable_query, from_expresion) {
            Ok(()) => {}
            Err(error) => return Err(error)
        }

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char != ')' {
                return Err(format!("Expected a ')', but found: {}", *peeked_char));
            }
        } else {
            return Err("Expected a ')', but found nothing".to_string());
        }
        from_expresion.push(FromExpressionElement::ClosedBracket);
        peekable_query.next();
        self.parse_whitespaces(peekable_query);

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char == 'a' || *peeked_char == 'A' {
                match self.parse_keyword(peekable_query, "AND", false) {
                    Ok(()) => { from_expresion.push(FromExpressionElement::OperatorAnd) }
                    Err(error) => return Err(error)
                }
            } else if *peeked_char == 'o' || *peeked_char == 'O' {
                match self.parse_keyword(peekable_query, "OR", false) {
                    Ok(()) => { from_expresion.push(FromExpressionElement::OperatorOr) }
                    Err(error) => return Err(error)
                }
            } else {
                return Ok(())
            }
        } else {
            return Ok(());
        }
        self.parse_whitespaces(peekable_query);

        match self.parse_from_expression(peekable_query, from_expresion) {
            Ok(()) => {}
            Err(error) => return Err(error)
        }

        Ok(())
    }

    fn parse_from_tag_expression(&self, peekable_query: &mut Peekable<Chars>, from_expresion: &mut Vec<FromExpressionElement>) -> Result<(), String> {
        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char != '#' {
                return Err(format!("Expected a '#', but found: {}", *peeked_char));
            }
        }
        peekable_query.next();

        match self.parse_string_value(peekable_query) {
            Ok(sv) => { from_expresion.push(FromExpressionElement::Tag(sv)) }
            Err(error) => return Err(error)
        }
        self.parse_whitespaces(peekable_query);

        loop {
            if let Some(peeked_char) = peekable_query.peek() {
                if *peeked_char == 'a' || *peeked_char == 'A' {
                    match self.parse_keyword(peekable_query, "AND", false) {
                        Ok(()) => { from_expresion.push(FromExpressionElement::OperatorAnd) }
                        Err(error) => return Err(error)
                    }
                } else if *peeked_char == 'o' || *peeked_char == 'O' {
                    match self.parse_keyword(peekable_query, "OR", false) {
                        Ok(()) => { from_expresion.push(FromExpressionElement::OperatorOr) }
                        Err(error) => return Err(error)
                    }
                } else {
                    return Ok(())
                }
            } else {
                return Ok(());
            }
            self.parse_whitespaces(peekable_query);

            match self.parse_from_expression(peekable_query, from_expresion) {
                Ok(()) => {}
                Err(error) => return Err(error)
            }
        }
    }

    // call only when you expect WHERE should happen
    fn parse_where(&self, peekable_query: &mut Peekable<Chars>) -> Result<Vec<WhereExpressionElement>, String> {
        match self.parse_keyword(peekable_query, "WHERE", false) {
            Ok(()) => {}
            Err(error) => return Err(error)
        }
        self.parse_whitespaces(peekable_query);

        let mut where_expression: Vec<WhereExpressionElement> = Vec::new();
        where_expression.push(WhereExpressionElement::OpenedBracket);
        // TODO: Implement WHERE parsing
        Ok(where_expression)
    }

    // call only when you expect ORDER BY should happen
    fn parse_order_by(&self, peekable_query: &mut Peekable<Chars>) -> Result<Vec<OrderByFieldOption>, String> {
        match self.parse_keyword(peekable_query, "ORDER BY", false) {
            Ok(()) => {}
            Err(error) => return Err(error)
        }

        self.parse_whitespaces(peekable_query);

        let mut order_by_fields: Vec<OrderByFieldOption> = Vec::new();
        order_by_fields.push(OrderByFieldOption { field_name: "".to_string(), order_direction: OrderDirection::ASC });
        // TODO: Implement WHERE parsing
        Ok(order_by_fields)
    }

    fn parse_field_value(&self, peekable_query: &mut Peekable<Chars>) -> Result<FieldValue, String> {
        Ok(FieldValue::String("test".to_string()))
    }

    fn parse_string_value(&self, peekable_query: &mut Peekable<Chars>) -> Result<String, String> {
        let mut field_name = String::new();

        if let Some(peeked_char) = peekable_query.peek() {
            // First char can't be a number
            if !(*peeked_char).is_alphabetic() && *peeked_char != '_' && *peeked_char != '-' {
                return Err(format!("Field name expected. They must start with letter, underscore or a minus, found: {}", *peeked_char));
            }
            field_name.push(*peeked_char);
            peekable_query.next();
        } else {
            return Err("Field name expected. nothing found".to_string());
        }

        while let Some(peeked_char) = peekable_query.peek() {
            if !(*peeked_char).is_alphanumeric() && *peeked_char != '_' && *peeked_char != '-' && *peeked_char != '/' {
                break;
            }
            field_name.push(*peeked_char);
            peekable_query.next();
        }

        Ok(field_name)
    }

    fn parse_field_name(&self, peekable_query: &mut Peekable<Chars>) -> Result<String, String> {
        let mut field_name = String::new();

        if let Some(peeked_char) = peekable_query.peek() {
            // First char can't be a number
            if !(*peeked_char).is_alphabetic() && *peeked_char != '_' && *peeked_char != '-' {
                return Err(format!("Field name expected. They must start with letter, underscore or a minus, found: {}", *peeked_char));
            }
            field_name.push(*peeked_char);
            peekable_query.next();
        } else {
            return Err("Field name expected. nothing found".to_string());
        }

        while let Some(peeked_char) = peekable_query.peek() {
            if !(*peeked_char).is_alphanumeric() && *peeked_char != '_' && *peeked_char != '-' {
                break;
            }
            field_name.push(*peeked_char);
            peekable_query.next();
        }

        Ok(field_name)
    }

    fn parse_keyword(&self, peekable_query: &mut Peekable<Chars>, keyword: &str, case_sensitive: bool) -> Result<(), String> {
        let mut keyword_chars = keyword.chars();
        let mut matched = String::new();

        for expected_char in &mut keyword_chars {
            if let Some(peeked_char) = peekable_query.peek() {
                matched.push(*peeked_char);

                let match_condition = if case_sensitive {
                    *peeked_char == expected_char
                } else {
                    peeked_char.to_ascii_lowercase() == expected_char.to_ascii_lowercase()
                };

                if !match_condition {
                    return Err(format!("Expected {}, but instead found: {}...", keyword, matched));
                }
                peekable_query.next();
            } else {
                return Err(format!("Expected {}, but instead found: {}...", keyword, matched));
            }
        }

        if let Some(c) = peekable_query.peek() {
            if !(*c).is_whitespace() {
                return Err(format!("Expected emptyspace after {} keyward, but found: {}...", keyword, c));
            }
        }
        Ok(())
    }

    fn parse_whitespaces(&self, peekable_query: &mut Peekable<Chars>) {
        loop {
            if let Some(c) = peekable_query.peek() { 
                if !(*c).is_whitespace() {
                    return;
                }
                peekable_query.next();
            }
        }
    }
}
