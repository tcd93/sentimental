#!/bin/bash
set -e

echo "Building Rust Lambda function using Docker..."

# Build the Docker image
docker build -t split-result-builder .

# Create a container from the image and copy the build artifact
docker create --name extract split-result-builder
docker cp extract:/workspace/split_result.zip .
docker rm extract

echo "Build completed successfully. The Lambda deployment package is split_result.zip" 