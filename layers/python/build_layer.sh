#!/bin/bash

# Create a temporary virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies into the python directory
pip install -r requirements.txt -t python/

# Clean up
deactivate
rm -rf venv

# Remove unnecessary files to reduce layer size
find python -type d -name "__pycache__" -exec rm -rf {} +
find python -type d -name "*.dist-info" -exec rm -rf {} +
find python -type d -name "*.egg-info" -exec rm -rf {} +

# Create ZIP file for manual upload if needed
zip -r layer.zip python/ 