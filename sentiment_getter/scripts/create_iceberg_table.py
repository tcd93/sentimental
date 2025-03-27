import boto3
import time
import argparse


def create_iceberg_table(
    database_name: str, table_name: str, s3_location: str, results_location: str
):
    athena = boto3.client("athena")

    query = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
        id STRING,
        execution_id STRING,
        keyword STRING,
        source STRING,
        title STRING,
        created_at TIMESTAMP,
        body STRING,
        comments ARRAY<STRING>,
        post_url STRING
    )
    PARTITIONED BY (keyword, created_at)
    LOCATION '{s3_location}'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_compression'='snappy',
        'optimize_rewrite_data_file_threshold'='5',
        'optimize_rewrite_delete_file_threshold'='2',
        'vacuum_min_snapshots_to_keep'='1',
        'vacuum_max_snapshot_age_seconds'='432000',
        'vacuum_max_metadata_files_to_keep'='100'
    )
    """

    print(f"Creating Iceberg table {database_name}.{table_name}...")

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database_name},
        ResultConfiguration={"OutputLocation": results_location},
    )

    # Wait for query completion
    while True:
        query_status = athena.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )["QueryExecution"]["Status"]["State"]

        if query_status == "SUCCEEDED":
            print("Table created successfully!")
            break
        elif query_status in ["FAILED", "CANCELLED"]:
            raise Exception(f"Query failed with status: {query_status}")

        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create an Iceberg table in Athena")
    parser.add_argument("--database", required=True, help="Glue database name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--location", required=True, help="S3 location for table data")
    parser.add_argument(
        "--results", required=True, help="S3 location for Athena query results"
    )

    args = parser.parse_args()

    create_iceberg_table(args.database, args.table, args.location, args.results)
