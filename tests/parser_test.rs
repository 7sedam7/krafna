use krafna::libs::parser::{
    ExpressionElement, FieldValue, Function, FunctionArg, Operator, OrderByFieldOption,
    OrderDirection,
};
use krafna::Query;

#[test]
fn test_complex_query_parsing_from_without_whitespace() -> Result<(), String> {
    let query = "SELECT field1, field2 FROMFRONTMATTER_INFO('~/folder') where (tag1 and  (tag2 or tag3)+tag4  )";

    if query.parse::<Query>().is_ok() {
        return Err("It should fail, because there is no whitespace before from!".to_string());
    }

    Ok(())
}

#[test]
fn test_complex_query_parsing_where_without_whitespace_before() -> Result<(), String> {
    let query = "SELECT field1, field2 FROM FRONTMATTER_INFO('~/folder')where (tag1 and  (tag2 or tag3)+tag4  )";

    if query.parse::<Query>().is_ok() {
        return Err("It should fail, because there is no whitespace before where!".to_string());
    }

    Ok(())
}

#[test]
fn test_complex_query_parsing_where_without_whitespace_after() -> Result<(), String> {
    let query = "SELECT field1, field2 FROM FRONTMATTER_INFO('~/folder') where(tag1 and  (tag2 or tag3)+tag4  )";

    if query.parse::<Query>().is_ok() {
        return Err("It should fail, because there is no whitespace before where!".to_string());
    }

    Ok(())
}

#[test]
fn test_complex_query_parsing() {
    let query = "SELECT field1, field2 FROM FRONTMATTER_INFO('~/folder') where (tag1 and  (tag2 or tag3)+tag4  ) order by kifla";

    let result: Query = query.parse().expect("Parsing should succeed");

    // Verify SELECT fields
    assert_eq!(result.select_fields, vec!["field1", "field2"]);

    // Verify FROM expression
    assert_eq!(
        Function::new(
            "FRONTMATTER_INFO".to_string(),
            vec![FunctionArg::FieldValue(FieldValue::String(
                "~/folder".to_string(),
            ))],
        ),
        result.from_function
    );

    // Verify WHERE expression
    assert_eq!(
        vec![
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
        ],
        result.where_expression
    );

    // Verify ORDER BY expression
    assert_eq!(
        vec![OrderByFieldOption::new(
            "kifla".to_string(),
            OrderDirection::ASC
        )],
        result.order_by_fields
    )
}
