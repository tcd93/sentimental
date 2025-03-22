use lambda_runtime::LambdaEvent;

// Import the code from split_result.rs
mod split_result {
    use lambda_runtime::{Error, LambdaEvent};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    pub struct Response {
        pub chunk1: Vec<String>,
        pub chunk2: Vec<String>,
    }

    #[derive(Deserialize, PartialEq, Debug)]
    pub struct Event(pub Vec<String>);

    pub async fn function_handler(event: LambdaEvent<Event>) -> Result<Response, Error> {
        let input = event.payload.0;
        let total_items = input.len();
        let midpoint = total_items / 2;
        
        // Split the array into two chunks
        let chunk1: Vec<String> = input.iter().take(midpoint).cloned().collect();
        let chunk2: Vec<String> = input.iter().skip(midpoint).cloned().collect();
        
        // Return the split result
        Ok(Response { chunk1, chunk2 })
    }
}

#[tokio::test]
async fn test_even_array_split() {
    // Create a test event with an even number of items
    let input = vec![
        "item1".to_string(),
        "item2".to_string(),
        "item3".to_string(),
        "item4".to_string(),
    ];
    
    let context = lambda_runtime::Context::default();
    let event = LambdaEvent::new(split_result::Event(input), context);
    
    // Call the handler
    let result = split_result::function_handler(event).await.expect("Handler failed");
    
    // Verify the result
    assert_eq!(result.chunk1, vec!["item1".to_string(), "item2".to_string()]);
    assert_eq!(result.chunk2, vec!["item3".to_string(), "item4".to_string()]);
    assert_eq!(result.chunk1.len(), result.chunk2.len());
}

#[tokio::test]
async fn test_odd_array_split() {
    // Create a test event with an odd number of items
    let input = vec![
        "item1".to_string(),
        "item2".to_string(),
        "item3".to_string(),
        "item4".to_string(),
        "item5".to_string(),
    ];
    
    let context = lambda_runtime::Context::default();
    let event = LambdaEvent::new(split_result::Event(input), context);
    
    // Call the handler
    let result = split_result::function_handler(event).await.expect("Handler failed");
    
    // Verify the result - for odd arrays, the second chunk should have one more item
    assert_eq!(result.chunk1, vec!["item1".to_string(), "item2".to_string()]);
    assert_eq!(result.chunk2, vec!["item3".to_string(), "item4".to_string(), "item5".to_string()]);
    assert_eq!(result.chunk1.len() + 1, result.chunk2.len());
}

#[tokio::test]
async fn test_empty_array() {
    // Create a test event with an empty array
    let input: Vec<String> = vec![];
    
    let context = lambda_runtime::Context::default();
    let event = LambdaEvent::new(split_result::Event(input), context);
    
    // Call the handler
    let result = split_result::function_handler(event).await.expect("Handler failed");
    
    // Verify both chunks are empty
    assert!(result.chunk1.is_empty());
    assert!(result.chunk2.is_empty());
}

#[tokio::test]
async fn test_single_item_array() {
    // Create a test event with a single item
    let input = vec!["item1".to_string()];
    
    let context = lambda_runtime::Context::default();
    let event = LambdaEvent::new(split_result::Event(input), context);
    
    // Call the handler
    let result = split_result::function_handler(event).await.expect("Handler failed");
    
    // Verify first chunk is empty and second has the item
    assert!(result.chunk1.is_empty());
    assert_eq!(result.chunk2, vec!["item1".to_string()]);
}

#[tokio::test]
async fn test_large_array() {
    // Create a test event with a large array (1000 items)
    let input: Vec<String> = (1..=1000).map(|i| format!("item{}", i)).collect();
    
    let context = lambda_runtime::Context::default();
    let event = LambdaEvent::new(split_result::Event(input), context);
    
    // Call the handler
    let result = split_result::function_handler(event).await.expect("Handler failed");
    
    // Verify equal split for large array
    assert_eq!(result.chunk1.len(), 500);
    assert_eq!(result.chunk2.len(), 500);
    
    // Check first and last items in each chunk
    assert_eq!(result.chunk1[0], "item1");
    assert_eq!(result.chunk1[499], "item500");
    assert_eq!(result.chunk2[0], "item501");
    assert_eq!(result.chunk2[499], "item1000");
} 