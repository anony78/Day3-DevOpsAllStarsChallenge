import boto3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
from dotenv import load_dotenv
import os

# Load environment variables (AWS credentials, etc.)
load_dotenv()

# AWS Configurations (update as needed)
region = "us-west-2"  # Change to your AWS region
database_name = "glue_nba_data_lake"  # Glue database name
query_output_location = "s3://players-nba/raw-data/"  # S3 path for Athena query results

# Create Athena client
athena_client = boto3.client("athena", region_name=region)

def query_athena(database, query, output_location):
    """Start an Athena query."""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    return response["QueryExecutionId"]

def fetch_query_results(query_execution_id, output_location):
    """Fetch results of an Athena query."""
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED"]:
            break
        time.sleep(2)
    
    if state == "SUCCEEDED":
        print("Query succeeded!")
        # Athena saves results to S3
        result_s3_path = f"{output_location}{query_execution_id}.csv"
        print(f"Query results saved to: {result_s3_path}")
        return result_s3_path
    else:
        raise Exception("Query failed!")

def visualize_data(file_path):
    """Load data and visualize using Python libraries."""
    # Load query results from the CSV file
    data = pd.read_csv(file_path)

    # Example visualization: Average Points by Team
    plt.figure(figsize=(12, 6))
    sns.barplot(x="Team", y="Points", data=data, estimator=sum)
    plt.title("Total Points by Team", fontsize=16)
    plt.xlabel("Team", fontsize=12)
    plt.ylabel("Points", fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main():
    # SQL Query to fetch data (modify as necessary)
    sql_query = """
    SELECT Team, Points
    FROM nba_players
    """
    
    # Run Athena query
    print("Running Athena query...")
    query_execution_id = query_athena(database_name, sql_query, query_output_location)
    
    # Fetch query results (CSV file stored in S3)
    print("Fetching query results...")
    result_s3_path = fetch_query_results(query_execution_id, query_output_location)
    
    # Visualize data (the chart will be displayed locally)
    print("Visualizing data...")
    visualize_data(result_s3_path)

if __name__ == "__main__":
    main()
