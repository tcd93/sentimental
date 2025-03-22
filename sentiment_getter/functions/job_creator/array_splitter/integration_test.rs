use lambda_runtime::{Error, LambdaEvent};
use serde_json::Value;

// Import structures from the main module
use split_result::Event;

// Test with a JSON string input similar to AWS Lambda invocation
#[tokio::test]
async fn test_with_json_input() -> Result<(), Error> {
    // This simulates how AWS Lambda would invoke our function
    let json_input = r#"["id1", "id2", "id3", "id4", "id5"]"#;
    let event_json: Value = serde_json::from_str(json_input)?;
    
    // Convert JSON to our Event type
    let string_array: Vec<String> = serde_json::from_value(event_json)?;
    let event = Event(string_array);
    
    // Create a mock context
    let context = lambda_runtime::Context::default();
    let lambda_event = LambdaEvent::new(event, context);
    
    // Call our function handler
    let response = split_result::function_handler(lambda_event).await?;
    
    // Verify the response
    assert_eq!(response.chunk1.len(), 2);
    assert_eq!(response.chunk2.len(), 3);
    assert_eq!(response.chunk1, vec!["id1", "id2"]);
    assert_eq!(response.chunk2, vec!["id3", "id4", "id5"]);
    
    // Test serialization of the response to JSON (as AWS would do)
    let json_response = serde_json::to_string(&response)?;
    let expected_json = r#"{"chunk1":["id1","id2"],"chunk2":["id3","id4","id5"]}"#;
    assert_eq!(json_response, expected_json);
    
    Ok(())
}

// Test with an empty array
#[tokio::test]
async fn test_empty_json_input() -> Result<(), Error> {
    let json_input = r#"[]"#;
    let event_json: Value = serde_json::from_str(json_input)?;
    
    let string_array: Vec<String> = serde_json::from_value(event_json)?;
    let event = Event(string_array);
    
    let context = lambda_runtime::Context::default();
    let lambda_event = LambdaEvent::new(event, context);
    
    let response = split_result::function_handler(lambda_event).await?;
    
    assert!(response.chunk1.is_empty());
    assert!(response.chunk2.is_empty());
    
    let json_response = serde_json::to_string(&response)?;
    let expected_json = r#"{"chunk1":[],"chunk2":[]}"#;
    assert_eq!(json_response, expected_json);
    
    Ok(())
} 