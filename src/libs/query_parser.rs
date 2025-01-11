use crate::libs::peekable_deque::PeekableDeque;
use std::collections::HashSet;

#[derive(Debug)]
pub struct QueryStatement {
    select_fields: Vec<String>,
    from_tables: Vec<FromExpressionElement>,
    where_expression: Vec<WhereExpressionElement>,
    order_by_fields: Vec<OrderByFieldOption>,
}

trait Expression {
    fn allows_brackets() -> bool;
    fn opened_bracket() -> Self;
    fn closed_bracket() -> Self;
}

#[derive(Debug)]
pub enum FromExpressionElement {
    OpenedBracket,
    ClosedBracket,
    Tag(String),
    OperatorAnd,
    OperatorOr,
}

impl Expression for FromExpressionElement {
    fn allows_brackets() -> bool {
        true
    }
    fn opened_bracket() -> Self {
        Self::OpenedBracket
    }
    fn closed_bracket() -> Self {
        Self::ClosedBracket
    }
}

#[derive(Debug)]
pub enum WhereExpressionElement {
    OpenedBracket,
    ClosedBracket,
    FieldName(String),
    FieldValue,
    OperatorAnd,
    OperatorOr,
}

impl Expression for WhereExpressionElement {
    fn allows_brackets() -> bool {
        true
    }
    fn opened_bracket() -> Self {
        Self::OpenedBracket
    }
    fn closed_bracket() -> Self {
        Self::ClosedBracket
    }
}

#[derive(Debug)]
enum ExpressionOperator {
    OperatorAnd,
    OperatorOr,
}

#[derive(Debug)]
enum ExpressionElement {
    Tag(String),
    String(String),
    Number(f64),
    Bool(bool),
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
pub enum OrderDirection {
    ASC,
    DESC,
}

pub struct QueryParser {
    query: String,
}

impl QueryParser {
    pub fn new(query: String) -> Self {
        QueryParser { query }
    }

    pub fn parse(&self) -> Result<QueryStatement, String> {
        let mut peekable_query: PeekableDeque<char> = PeekableDeque::from_iter(self.query.chars());
        let mut query_statement = QueryStatement {
            select_fields: Vec::new(),
            from_tables: Vec::new(),
            where_expression: Vec::new(),
            order_by_fields: Vec::new(),
        };

        match self.parse_select(&mut peekable_query) {
            Ok(sf) => query_statement.select_fields = sf,
            Err(error) => return Err(format!("Error: {}, Query: {:?}", error, peekable_query)),
        };

        match self.parse_from(&mut peekable_query) {
            Ok(ft) => query_statement.from_tables = ft,
            Err(error) => return Err(format!("Error: {}, Query: {:?}", error, peekable_query)),
        };

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char == 'w' || *peeked_char == 'W' {
                match self.parse_where(&mut peekable_query) {
                    Ok(we) => query_statement.where_expression = we,
                    Err(error) => {
                        return Err(format!("Error: {}, Query: {:?}", error, peekable_query))
                    }
                };
            }
        }

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char == 'o' || *peeked_char == 'O' {
                match self.parse_order_by(&mut peekable_query) {
                    Ok(ob) => query_statement.order_by_fields = ob,
                    Err(error) => {
                        return Err(format!("Error: {}, Query: {:?}", error, peekable_query))
                    }
                };
            }
        }

        //if let Some(peeked_char) = peekable_query.peek() {
        //    return Err(format!("Unexpected character: {}", *peeked_char));
        //}

        Ok(query_statement)
    }

    fn parse_select(
        &self,
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<Vec<String>, String> {
        match self.parse_keyword(peekable_query, "SELECT", false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        let mut select_fields: Vec<String> = Vec::new();

        loop {
            self.parse_whitespaces(peekable_query);

            match self.parse_field_name(peekable_query) {
                Ok(field_name) => select_fields.push(field_name),
                Err(error) => return Err(error),
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

    fn parse_from(
        &self,
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<Vec<FromExpressionElement>, String> {
        match self.parse_keyword(peekable_query, "FROM", false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        self.parse_whitespaces(peekable_query);
        let mut from_expression: Vec<FromExpressionElement> = Vec::new();

        match self.parse_expression(
            peekable_query,
            &mut from_expression,
            &|peekable_query| self.parse_tag(peekable_query),
            &|peekable_query| self.parse_from_operators(peekable_query),
        ) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        Ok(from_expression)
    }

    // call only when you expect WHERE should happen
    fn parse_where(
        &self,
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<Vec<WhereExpressionElement>, String> {
        match self.parse_keyword(peekable_query, "WHERE", false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }
        self.parse_whitespaces(peekable_query);

        let mut where_expression: Vec<WhereExpressionElement> = Vec::new();
        where_expression.push(WhereExpressionElement::OpenedBracket);
        // TODO: Implement WHERE parsing
        Ok(where_expression)
    }

    // call only when you expect ORDER BY should happen
    fn parse_order_by(
        &self,
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<Vec<OrderByFieldOption>, String> {
        match self.parse_keyword(peekable_query, "ORDER BY", false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        self.parse_whitespaces(peekable_query);

        let mut order_by_fields: Vec<OrderByFieldOption> = Vec::new();
        order_by_fields.push(OrderByFieldOption {
            field_name: "".to_string(),
            order_direction: OrderDirection::ASC,
        });
        // TODO: Implement WHERE parsing
        Ok(order_by_fields)
    }

    fn parse_field_value(
        &self,
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<FieldValue, String> {
        Ok(FieldValue::String("".to_string()))
    }

    fn parse_expression<T: Expression, F1, F2>(
        &self,
        peekable_query: &mut PeekableDeque<char>,
        expression_elements: &mut Vec<T>,
        parse_elements: &F1,
        parse_operators: &F2,
    ) -> Result<(), String>
    where
        F1: Fn(&mut PeekableDeque<char>) -> Result<T, String>,
        F2: Fn(&mut PeekableDeque<char>) -> Result<T, String>,
    {
        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char == '(' {
                if !T::allows_brackets() {
                    return Err(
                        "Found opened bracket, however, expression does not allow brackets!"
                            .to_string(),
                    );
                }
                match self.parse_bracket_expression(
                    peekable_query,
                    expression_elements,
                    parse_elements,
                    parse_operators,
                ) {
                    Ok(()) => {}
                    Err(error) => return Err(error),
                }
            } else {
                match self.parse_no_bracket_expression(
                    peekable_query,
                    expression_elements,
                    parse_elements,
                    parse_operators,
                ) {
                    Ok(()) => {}
                    Err(error) => return Err(error),
                }
            }
        } else {
            return Err("Expected expression, but found nothing".to_string());
        }
        self.parse_whitespaces(peekable_query);

        Ok(())
    }

    fn parse_bracket_expression<T: Expression, F1, F2>(
        &self,
        peekable_query: &mut PeekableDeque<char>,
        expression_elements: &mut Vec<T>,
        parse_elements: &F1,
        parse_operators: &F2,
    ) -> Result<(), String>
    where
        F1: Fn(&mut PeekableDeque<char>) -> Result<T, String>,
        F2: Fn(&mut PeekableDeque<char>) -> Result<T, String>,
    {
        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char != '(' {
                return Err(format!("Expected a '(', but found: {}", *peeked_char));
            }
        }
        expression_elements.push(T::opened_bracket());
        peekable_query.next();
        self.parse_whitespaces(peekable_query);

        match self.parse_expression(
            peekable_query,
            expression_elements,
            parse_elements,
            parse_operators,
        ) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char != ')' {
                return Err(format!("Expected a ')', but found: {}", *peeked_char));
            }
        } else {
            return Err("Expected a ')', but found nothing".to_string());
        }
        expression_elements.push(T::closed_bracket());
        peekable_query.next();
        self.parse_whitespaces(peekable_query);

        match parse_operators(peekable_query) {
            Ok(op) => expression_elements.push(op),
            Err(_) => return Ok(()),
        }
        self.parse_whitespaces(peekable_query);

        self.parse_expression(
            peekable_query,
            expression_elements,
            parse_elements,
            parse_operators,
        )
    }

    fn parse_no_bracket_expression<T: Expression, F1, F2>(
        &self,
        peekable_query: &mut PeekableDeque<char>,
        expression_elements: &mut Vec<T>,
        parse_elements: &F1,
        parse_operators: &F2,
    ) -> Result<(), String>
    where
        F1: Fn(&mut PeekableDeque<char>) -> Result<T, String>,
        F2: Fn(&mut PeekableDeque<char>) -> Result<T, String>,
    {
        match parse_elements(peekable_query) {
            Ok(el) => expression_elements.push(el),
            Err(error) => return Err(error),
        }
        self.parse_whitespaces(peekable_query);

        loop {
            match parse_operators(peekable_query) {
                Ok(op) => expression_elements.push(op),
                Err(_) => return Ok(()),
            }
            self.parse_whitespaces(peekable_query);

            match self.parse_expression(
                peekable_query,
                expression_elements,
                parse_elements,
                parse_operators,
            ) {
                Ok(()) => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn parse_operators<String>(
        &self,
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<String, String> {
        let operators: HashSet<_> = [
            "AND", "OR", "IN", "<", "<=", ">", ">=", "==", "!=", "+", "-", "*", "/", "**", "//",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

        let mut operator_candidates = operators.clone();
        let mut index = 0;

        while let Some(&peeked_char) = peekable_query.peek() {
            operator_candidates.retain(|op| op.chars().nth(index) != Some(peeked_char));

            if operator_candidates.len() == 1 {
                return Ok(operator_candidates
                    .into_iter()
                    .next()
                    .expect("Set is empty?"));
            }

            if operator_candidates.is_empty() {
                break;
            }

            // Move to the next character and increment the index
            peekable_query.next();
            index += 1;
        }

        Err("No operator".to_string())
    }

    fn parse_from_operators(
        &self,
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<FromExpressionElement, String> {
        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char == 'a' || *peeked_char == 'A' {
                match self.parse_keyword(peekable_query, "AND", false) {
                    Ok(()) => Ok(FromExpressionElement::OperatorAnd),
                    Err(error) => Err(error),
                }
            } else if *peeked_char == 'o' || *peeked_char == 'O' {
                match self.parse_keyword(peekable_query, "OR", false) {
                    Ok(()) => Ok(FromExpressionElement::OperatorOr),
                    Err(error) => Err(error),
                }
            } else {
                Err("No operator".to_string())
            }
        } else {
            Err("No operator".to_string())
        }
    }

    fn parse_tag(
        &self,
        peekable_query: &mut PeekableDeque<char>,
    ) -> Result<FromExpressionElement, String> {
        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char != '#' {
                return Err(format!("Expected a '#', but found: {}", *peeked_char));
            }
        }
        peekable_query.next();

        let mut tag = String::new();

        if let Some(peeked_char) = peekable_query.peek() {
            // First char can't be a number
            if !(*peeked_char).is_alphabetic() && *peeked_char != '_' && *peeked_char != '-' {
                return Err(format!("Field name expected. They must start with letter, underscore or a minus, found: {}", *peeked_char));
            }
            tag.push(*peeked_char);
            peekable_query.next();
        } else {
            return Err("Field name expected. nothing found".to_string());
        }

        while let Some(peeked_char) = peekable_query.peek() {
            if !(*peeked_char).is_alphanumeric()
                && *peeked_char != '_'
                && *peeked_char != '-'
                && *peeked_char != '/'
            {
                break;
            }
            tag.push(*peeked_char);
            peekable_query.next();
        }

        Ok(FromExpressionElement::Tag(tag))
    }

    fn parse_field_name(&self, peekable_query: &mut PeekableDeque<char>) -> Result<String, String> {
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

    fn parse_keyword(
        &self,
        peekable_query: &mut PeekableDeque<char>,
        keyword: &str,
        case_sensitive: bool,
    ) -> Result<(), String> {
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

        if let Some(c) = peekable_query.peek() {
            if !(*c).is_whitespace() {
                return Err(format!(
                    "Expected emptyspace after {} keyward, but found: {}...",
                    keyword, c
                ));
            }
        }
        Ok(())
    }

    fn parse_whitespaces(&self, peekable_query: &mut PeekableDeque<char>) {
        loop {
            if let Some(c) = peekable_query.peek() {
                if !(*c).is_whitespace() {
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

    #[test]
    fn parse_from_operators() {}

    #[test]
    fn parse_no_bracket_expression() {}

    #[test]
    fn parse_bracket_expression() {}

    #[test]
    fn parse_expression() {}

    #[test]
    fn parse_field_value() {}

    #[test]
    fn parse_order_by() {}

    #[test]
    fn parse_where() {}

    #[test]
    fn parse_from() {}

    #[test]
    fn parse_select() {}

    #[test]
    fn parse_tag() {}

    #[test]
    fn test_parse_field_name_first_char_num() -> Result<(), String> {
        let field_name = "5test".to_string();
        let parser = QueryParser::new(field_name);
        let mut peekable_query: PeekableDeque<char> =
            PeekableDeque::from_iter(parser.query.chars());

        if parser.parse_field_name(&mut peekable_query).is_ok() {
            return Err("It should fail since field name can't start with a number!".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_keyword_case_sensitive() -> Result<(), String> {
        let query = "SeLeCt ".to_string();
        let keyword = "SELECT".to_string();
        let parser = QueryParser::new(query);
        let mut peekable_query: PeekableDeque<char> =
            PeekableDeque::from_iter(parser.query.chars());

        if let Ok(()) = parser.parse_keyword(&mut peekable_query, &keyword, true) {
            return Err(
                "It should fail since there is no match if we take into account case sensitivity!"
                    .to_string(),
            );
        }

        Ok(())
    }

    #[test]
    fn test_parse_keyword_exact_case_sensitive() -> Result<(), String> {
        let query = "SeLeCt ".to_string();
        let keyword = "SeLeCt".to_string();
        let parser = QueryParser::new(query);
        let mut peekable_query: PeekableDeque<char> =
            PeekableDeque::from_iter(parser.query.chars());

        match parser.parse_keyword(&mut peekable_query, &keyword, true) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        if let Some(peeked_char) = peekable_query.peek() {
            assert_eq!(' ', *peeked_char);
        } else {
            panic!("Expected empty space, but got nothing!");
        }

        Ok(())
    }

    #[test]
    fn test_parse_keyword_exact_case_insensitive() -> Result<(), String> {
        let query = "SELECT ".to_string();
        let keyword = "SeLeCt".to_string();
        let parser = QueryParser::new(query);
        let mut peekable_query: PeekableDeque<char> =
            PeekableDeque::from_iter(parser.query.chars());

        match parser.parse_keyword(&mut peekable_query, &keyword, false) {
            Ok(()) => {}
            Err(error) => return Err(error),
        }

        if let Some(peeked_char) = peekable_query.peek() {
            assert_eq!(' ', *peeked_char);
        } else {
            panic!("Expected empty space, but got nothing!");
        }

        Ok(())
    }

    #[test]
    fn test_parse_keyword_start_with_whitespace() -> Result<(), String> {
        let query = "  SELECT".to_string();
        let keyword = "SELECT".to_string();
        let parser = QueryParser::new(query);
        let mut peekable_query: PeekableDeque<char> =
            PeekableDeque::from_iter(parser.query.chars());

        if let Ok(()) = parser.parse_keyword(&mut peekable_query, &keyword, false) {
            return Err("It should fail since it is supposed to expect the keywoard and it has empty space in the beginning!".to_string());
        }

        Ok(())
    }

    #[test]
    fn test_parse_whitespaces_skip_whitspace() {
        let query = "  \t  \t\t\n  \t\n\n  a".to_string();
        let parser = QueryParser::new(query);
        let mut peekable_query: PeekableDeque<char> =
            PeekableDeque::from_iter(parser.query.chars());

        parser.parse_whitespaces(&mut peekable_query);
        if let Some(peeked_char) = peekable_query.peek() {
            assert_eq!('a', *peeked_char);
        } else {
            panic!("Expected 'a' char, but got nothing!");
        }
    }

    #[test]
    fn test_parse_whitespaces_nothing_to_skip() {
        let query = "a  \t\t\n\n  ".to_string();
        let parser = QueryParser::new(query);
        let mut peekable_query: PeekableDeque<char> =
            PeekableDeque::from_iter(parser.query.chars());

        parser.parse_whitespaces(&mut peekable_query);
        if let Some(peeked_char) = peekable_query.peek() {
            assert_eq!('a', *peeked_char);
        } else {
            panic!("Expected 'a' char, but got nothing!");
        }
    }
}
