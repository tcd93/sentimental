use lambda_runtime::{run, service_fn};
use tracing::info;

// Use the function from our library
use split_result::function_handler;

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    // Initialize the tracing subscriber
    tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("Split Result Lambda function initialized");
    
    // Start the Lambda runtime with our function handler
    run(service_fn(function_handler)).await
} 