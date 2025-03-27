import tomli
import boto3
import time
import argparse


def create_iceberg_table(
    database_name: str, table_name: str, s3_location: str, results_location: str, region: str, profile: str
):
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
                error_message = query_execution["QueryExecution"]["Status"].get("StateChangeReason", "No error message available")
                print(f"Drop query failed with status: {query_status}")
                print(f"Error message: {error_message}")
                break
                
            time.sleep(1)
    except Exception as e:
        print(f"Error dropping table: {str(e)}")

    # Create the new table with partitioning
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
        id STRING,
        keyword STRING,
        source STRING,
        title STRING,
        created_at TIMESTAMP,
        body STRING,
        comments ARRAY<STRING>,
        execution_id STRING,
        post_url STRING
    )
    LOCATION '{s3_location}'
    TBLPROPERTIES ('table_type'='ICEBERG')
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
            error_message = query_execution["QueryExecution"]["Status"].get("StateChangeReason", "No error message available")
            print(f"Query failed with status: {query_status}")
            print(f"Error message: {error_message}")
            raise Exception(f"Query failed with status: {query_status}\nError message: {error_message}")

        time.sleep(1)


def parse_samconfig():
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
    create_iceberg_table(database_name, table_name, s3_location, results_location, region, profile)


if __name__ == "__main__":
    parse_samconfig()
