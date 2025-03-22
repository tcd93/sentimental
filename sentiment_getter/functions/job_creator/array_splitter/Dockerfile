FROM rust:1.71-alpine3.17

# Install build dependencies
RUN apk add --no-cache musl-dev zip

# Create a workspace directory
WORKDIR /workspace

# Copy project files
COPY Cargo.toml .
COPY split_result.rs .
COPY main.rs .
COPY split_result_test.rs .
COPY integration_test.rs .

# Run tests first
RUN cargo test

# Build the Lambda function
RUN rustup target add x86_64-unknown-linux-musl && \
    cargo build --release --bin bootstrap --target x86_64-unknown-linux-musl

# Create the deployment package
RUN mkdir -p bin && \
    cp target/x86_64-unknown-linux-musl/release/bootstrap bin/ && \
    cd bin && \
    zip -j ../split_result.zip bootstrap

# The output will be in /workspace/split_result.zip 