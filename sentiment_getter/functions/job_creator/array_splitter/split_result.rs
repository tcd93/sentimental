use lambda_runtime::{Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use tracing::info;

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
    
    info!("Splitting array of {} items into two chunks", total_items);
    
    // Split the array into two chunks
    let chunk1: Vec<String> = input.iter().take(midpoint).cloned().collect();
    let chunk2: Vec<String> = input.iter().skip(midpoint).cloned().collect();
    
    info!("Split complete: chunk1={} items, chunk2={} items", 
          chunk1.len(), chunk2.len());
    
    // Return the split result
    Ok(Response { chunk1, chunk2 })
} 