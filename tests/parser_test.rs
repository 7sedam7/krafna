use krafna::libs::parser::{ExpressionElement, Operator};
use krafna::Query;

#[test]
fn test_complex_query_parsing() {
    let query = "SELECT field1, field2 FROM (tag1 and  (tag2 or tag3)+tag4  ) where (tag1 and  (tag2 or tag3)+tag4  )";

    let result: Query = query.parse().expect("Parsing should succeed");

    // Verify SELECT fields
    assert_eq!(result.select_fields, vec!["field1", "field2"]);

    // Verify FROM expression
    let expected_from = vec![
        ExpressionElement::OpenedBracket,
        ExpressionElement::FieldName("tag1".to_string()),
        ExpressionElement::Operator(Operator::And),
        ExpressionElement::OpenedBracket,
        ExpressionElement::FieldName("tag2".to_string()),
        ExpressionElement::Operator(Operator::Or),
        ExpressionElement::FieldName("tag3".to_string()),
        ExpressionElement::ClosedBracket,
        ExpressionElement::Operator(Operator::Plus),
        ExpressionElement::FieldName("tag4".to_string()),
        ExpressionElement::ClosedBracket,
    ];
    assert_eq!(result.from_tables, expected_from);

    // Verify WHERE expression
    let expected_where = vec![
        ExpressionElement::OpenedBracket,
        ExpressionElement::FieldName("tag1".to_string()),
        ExpressionElement::Operator(Operator::And),
        ExpressionElement::OpenedBracket,
        ExpressionElement::FieldName("tag2".to_string()),
        ExpressionElement::Operator(Operator::Or),
        ExpressionElement::FieldName("tag3".to_string()),
        ExpressionElement::ClosedBracket,
        ExpressionElement::Operator(Operator::Plus),
        ExpressionElement::FieldName("tag4".to_string()),
        ExpressionElement::ClosedBracket,
    ];
    assert_eq!(result.where_expression, expected_where);
}
