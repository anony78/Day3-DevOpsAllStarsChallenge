import boto3
import json
import time
import requests
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# AWS and API configurations
REGION = "us-west-2"
BUCKET_NAME = "sanalytics-data-lake"
GLUE_DATABASE = "glue_nba_data_lake"
ATHENA_OUTPUT = f"s3://{BUCKET_NAME}/athena-results/"

# Sportsdata.io API configurations
API_KEY = os.getenv("SPORTSDATA_API_KEY")
NBA_ENDPOINT = os.getenv("NBA_ENDPOINT")

# Initialize AWS service clients
s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
athena = boto3.client("athena", region_name=REGION)

def initialize_s3_bucket():
    """Create an S3 bucket to store sports analytics data."""
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
        print(f"S3 bucket '{BUCKET_NAME}' created successfully.")
    except Exception as error:
        print(f"Failed to create S3 bucket: {error}")

def setup_glue_database():
    """Set up a Glue database for the data lake."""
    try:
        glue.create_database(
            DatabaseInput={
                "Name": GLUE_DATABASE,
                "Description": "Database for NBA sports analytics data."
            }
        )
        print(f"Glue database '{GLUE_DATABASE}' created successfully.")
    except Exception as error:
        print(f"Error creating Glue database: {error}")

def retrieve_nba_data():
    """Fetch NBA data from the Sportsdata.io API."""
    try:
        response = requests.get(NBA_ENDPOINT, headers={"Ocp-Apim-Subscription-Key": API_KEY})
        response.raise_for_status()
        print("Successfully retrieved NBA data.")
        return response.json()
    except Exception as error:
        print(f"Error fetching NBA data: {error}")
        return []

def format_as_jsonl(data):
    """Convert a list of records to line-delimited JSON format."""
    print("Formatting data to line-delimited JSON.")
    return "\n".join(json.dumps(record) for record in data)

def upload_to_s3(content, key):
    """Upload content to an S3 bucket with the specified key."""
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=content)
        print(f"Uploaded data to S3 under key: {key}")
    except Exception as error:
        print(f"Error uploading data to S3: {error}")

def define_glue_table():
    """Create a Glue table to map NBA data."""
    try:
        glue.create_table(
            DatabaseName=GLUE_DATABASE,
            TableInput={
                "Name": "nba_players",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "PlayerID", "Type": "int"},
                        {"Name": "FirstName", "Type": "string"},
                        {"Name": "LastName", "Type": "string"},
                        {"Name": "Team", "Type": "string"},
                        {"Name": "Position", "Type": "string"},
                        {"Name": "Points", "Type": "int"}
                    ],
                    "Location": f"s3://{BUCKET_NAME}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print("Glue table 'nba_players' created successfully.")
    except Exception as error:
        print(f"Error creating Glue table: {error}")

def configure_athena_output():
    """Set Athena's output location for query results."""
    try:
        athena.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS nba_analytics",
            QueryExecutionContext={"Database": GLUE_DATABASE},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        print("Athena output location configured successfully.")
    except Exception as error:
        print(f"Error configuring Athena output: {error}")

def main():
    """Main workflow to initialize the NBA data lake."""
    print("Starting NBA data lake setup...")
    initialize_s3_bucket()
    time.sleep(5)  # Allow time for S3 bucket creation
    setup_glue_database()
    nba_data = retrieve_nba_data()
    if nba_data:
        jsonl_data = format_as_jsonl(nba_data)
        upload_to_s3(jsonl_data, "raw-data/nba_player_data.jsonl")
    define_glue_table()
    configure_athena_output()
    print("NBA data lake setup completed.")

if __name__ == "__main__":
    main()
