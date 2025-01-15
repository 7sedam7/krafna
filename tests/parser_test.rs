use krafna::libs::parser::ExpressionElement;
use krafna::libs::parser::Operator;
use krafna::QueryStatement;

#[test]
fn test_complex_query_parsing() {
    let query = "SELECT field1, field2 FROM (#tag1 and  (#tag2 or #tag3)   )";

    let result: QueryStatement = query.parse().expect("Parsing should succeed");

    // Verify SELECT fields
    assert_eq!(result.select_fields, vec!["field1", "field2"]);

    // Verify FROM expression
    let expected_from = vec![
        ExpressionElement::OpenedBracket,
        ExpressionElement::Tag("tag1".to_string()),
        ExpressionElement::Operator(Operator::And),
        ExpressionElement::OpenedBracket,
        ExpressionElement::Tag("tag2".to_string()),
        ExpressionElement::Operator(Operator::Or),
        ExpressionElement::Tag("tag3".to_string()),
        ExpressionElement::ClosedBracket,
        ExpressionElement::ClosedBracket,
    ];

    assert_eq!(result.from_tables, expected_from);
}
