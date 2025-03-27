# Iceberg Table Setup Scripts

This directory contains scripts to set up Iceberg tables in AWS Athena.

## Prerequisites

- Python 3.x
- AWS credentials configured
- AWS CLI installed (optional)
- `tomli` package installed (`pip install tomli`)

## Usage

Run the script directly:
```bash
python parse_samconfig.py
```

The script will automatically:
1. Read the bucket name from your `samconfig.toml`
2. Create an Iceberg table in the "sentimental" database
3. Use the same bucket for both data and Athena results

## What it does

The script creates an Iceberg table in AWS Athena with the following schema:
- id (STRING)
- keyword (STRING)
- source (STRING)
- title (STRING)
- created_at (TIMESTAMP)
- body (STRING)
- comments (ARRAY<STRING>)
- execution_id (STRING)
- post_url (STRING)

This schema matches the `Post` model from `models/post.py`. 