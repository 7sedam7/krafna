use krafna::{QueryParser, QueryStatement};
use krafna::libs::query_parser::FromExpressionElement;

#[test]
fn test_complex_query_parsing() {
    let query = "SELECT field1, field2 FROM (#tag1 and  (#tag2 or #tag3)   )";
    let parser = QueryParser::new(query.to_string());

    let result = parser.parse();
    assert!(result.is_ok(), "Parsing should succeed");

    let query_statement = result.unwrap();

    // Verify SELECT fields
    assert_eq!(query_statement.select_fields, vec!["field1", "field2"]);

    // Verify FROM expression
    let expected_from = vec![
        FromExpressionElement::OpenedBracket,
        FromExpressionElement::Tag("tag1".to_string()),
        FromExpressionElement::OperatorAnd,
        FromExpressionElement::OpenedBracket,
        FromExpressionElement::Tag("tag2".to_string()),
        FromExpressionElement::OperatorOr,
        FromExpressionElement::Tag("tag3".to_string()),
        FromExpressionElement::ClosedBracket,
        FromExpressionElement::ClosedBracket,
    ];

    assert_eq!(query_statement.from_tables, expected_from);
}
