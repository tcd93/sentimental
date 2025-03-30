"""Script to create and configure an Iceberg table in Athena for sentiment analysis data."""

import time
import tomli
import boto3
from botocore.exceptions import ClientError


class TableOperationError(Exception):
    """Custom exception for table operations."""

    pass


def create_iceberg_table(
    database_name: str,
    table_name: str,
    s3_location: str,
    results_location: str,
    region: str,
    profile: str,
):
    """Create an Iceberg table in Athena with specified partitioning and properties.

    Args:
        database_name: Name of the Athena database
        table_name: Name of the table to create
        s3_location: S3 location for table data
        results_location: S3 location for query results
        region: AWS region
        profile: AWS profile name
    """
    session = boto3.Session(profile_name=profile)
    athena = session.client("athena", region_name=region)

    # First drop the table if it exists
    drop_query = f"DROP TABLE IF EXISTS {database_name}.{table_name}"
    print(f"Dropping existing table {database_name}.{table_name} if it exists...")

    try:
        response = athena.start_query_execution(
            QueryString=drop_query,
            QueryExecutionContext={"Database": database_name},
            ResultConfiguration={"OutputLocation": results_location},
        )

        # Wait for drop query completion
        while True:
            query_execution = athena.get_query_execution(
                QueryExecutionId=response["QueryExecutionId"]
            )
            query_status = query_execution["QueryExecution"]["Status"]["State"]

            if query_status == "SUCCEEDED":
                print("Table dropped successfully!")
                break
            elif query_status in ["FAILED", "CANCELLED"]:
                error_message = query_execution["QueryExecution"]["Status"].get(
                    "StateChangeReason", "No error message available"
                )
                print(f"Drop query failed with status: {query_status}")
                print(f"Error message: {error_message}")
                break

            time.sleep(1)
    except ClientError as e:
        print(f"Error dropping table: {str(e)}")

    # Create the new table with partitioning
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
        id STRING,
        execution_id STRING,
        keyword STRING,
        source STRING,
        title STRING,
        created_at TIMESTAMP(0),
        body STRING,
        comments ARRAY<STRING>,
        post_url STRING,
        sentiment STRING,
        sentiment_score_mixed FLOAT,
        sentiment_score_positive FLOAT,
        sentiment_score_neutral FLOAT,
        sentiment_score_negative FLOAT
    )
    PARTITIONED BY (month(created_at), bucket(16, keyword))
    LOCATION '{s3_location}'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_compression'='ZSTD',
        'optimize_rewrite_data_file_threshold'='5',
        'optimize_rewrite_delete_file_threshold'='2',
        'vacuum_min_snapshots_to_keep'='1',
        'vacuum_max_snapshot_age_seconds'='216000',
        'vacuum_max_metadata_files_to_keep'='20'
    )
    """

    print(f"Creating Iceberg table {database_name}.{table_name}...")
    print(f"Using S3 location: {s3_location}")
    print(f"Using results location: {results_location}")

    response = athena.start_query_execution(
        QueryString=create_query,
        QueryExecutionContext={"Database": database_name},
        ResultConfiguration={"OutputLocation": results_location},
    )

    # Wait for query completion
    while True:
        query_execution = athena.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )
        query_status = query_execution["QueryExecution"]["Status"]["State"]

        if query_status == "SUCCEEDED":
            print("Table created successfully!")
            break
        elif query_status in ["FAILED", "CANCELLED"]:
            error_message = query_execution["QueryExecution"]["Status"].get(
                "StateChangeReason", "No error message available"
            )
            print(f"Query failed with status: {query_status}")
            print(f"Error message: {error_message}")
            raise TableOperationError(
                f"Query failed with status: {query_status}\nError message: {error_message}"
            )

        time.sleep(1)


def parse_samconfig():
    """Parse samconfig.toml and create an Iceberg table using the configuration."""
    with open("../../samconfig.toml", "rb") as f:
        config = tomli.load(f)

    # Extract parameter overrides
    params = {}
    for param in config["default"]["deploy"]["parameters"]["parameter_overrides"]:
        key, value = param.split("=", 1)
        params[key] = value.strip('"')

    # Get bucket name, region, and profile from parameters
    bucket_name = params.get("BucketName", "")
    region = config["default"]["deploy"]["parameters"]["region"]
    profile = config["default"]["deploy"]["parameters"]["profile"]

    # Set variables
    database_name = "sentimental"
    table_name = "post"
    s3_location = f"s3://{bucket_name}/iceberg/"
    results_location = f"s3://{bucket_name}/athena-results/create-table/"

    # Create the Iceberg table
    create_iceberg_table(
        database_name, table_name, s3_location, results_location, region, profile
    )


if __name__ == "__main__":
    parse_samconfig()
