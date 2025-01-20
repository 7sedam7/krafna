use krafna::libs::parser::{ExpressionElement, FieldValue, Function, FunctionArg, Operator};
use krafna::Query;

#[test]
fn test_complex_query_parsing_without_whitespace() -> Result<(), String> {
    let query = "SELECT field1, field2 FROM FRONTMATTER_INFO('~/folder')where (tag1 and  (tag2 or tag3)+tag4  )";

    if query.parse::<Query>().is_ok() {
        return Err("It should fail, because there is no whitespace before whgere!".to_string());
    }

    Ok(())
}

#[test]
fn test_complex_query_parsing() {
    let query = "SELECT field1, field2 FROM FRONTMATTER_INFO('~/folder') where (tag1 and  (tag2 or tag3)+tag4  )";

    let result: Query = query.parse().expect("Parsing should succeed");

    // Verify SELECT fields
    assert_eq!(result.select_fields, vec!["field1", "field2"]);

    // Verify FROM expression
    let expected_from = Function::new(
        "FRONTMATTER_INFO".to_string(),
        vec![FunctionArg::FieldValue(FieldValue::String(
            "~/folder".to_string(),
        ))],
    );
    assert_eq!(expected_from, result.from_function);

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
    assert_eq!(expected_where, result.where_expression);
}
