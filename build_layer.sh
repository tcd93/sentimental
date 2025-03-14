#!/bin/bash
set -e

# Create a temporary directory for building the layer
mkdir -p temp_layer/python

# Install packages into the temporary directory
pip install -r layers/requirements.txt -t temp_layer/python

# Create the zip file
cd temp_layer
zip -r ../layers/python-api-packages.zip .
cd ..

# Clean up
rm -rf temp_layer

echo "Layer built successfully: layers/python-api-packages.zip" 