import boto3
import httpx
import json
import pandas as pd
from pathlib import Path
from prefect import flow, task
from pydantic import BaseModel, ValidationError
from typing import Optional, Dict, Any
from botocore.exceptions import ClientError

# Configuration variables
AWS_S3_BUCKET_NAME = '<bucket_name>'
AWS_REGION = '<your_aws_region>'
AWS_ACCESS_KEY = '<iam_user_access_key>'
AWS_SECRET_KEY = '<iam_user_secret_key>'

class Repo(BaseModel):
    repo: str

class GithubRepoSchema(BaseModel):
    """
    Pydantic model defining the expected structure and types of our GitHub repo data.
    This serves as our data contract/schema.
    """
    name: str
    full_name: str
    stargazers_count: int
    watchers_count: int
    forks_count: int

class ETLConfig(BaseModel):
    """Configuration for the ETL pipeline"""
    repository: str  # GitHub repository to analyze (e.g., "PrefectHQ/prefect")
    min_stars: int = 10  # Minimum number of stars required for validation
    output_bucket: str  # S3 bucket for storing results
    aws_region: str = "us-east-1"  # AWS region for S3 bucket




@task(log_prints=True)
def request_data_and_upload_to_s3(repo: str, bucket: str):
    """Fetch the statistics for a GitHub repo and upload to S3"""
    print("Requesting data from the API...")
    repo_url = f"https://api.github.com/repos/{repo}"
    response = httpx.get(repo_url)
    
    file_path = Path("raw-data-example.txt")
    file_path.write_text(response.text)
    
    # Upload raw data to s3
    s3_url = upload_to_s3(str(file_path), "raw-data-example.txt", bucket)
    print(f"Raw data uploaded to: {s3_url}")
    
    return str(file_path)

@task(log_prints=True)
def upload_to_s3(file_path: str, destination_path: str, bucket_name: str) -> str:
    """Uploads the file data to an S3 bucket using boto3."""
    try:
        s3_client = boto3.client(
            service_name='s3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
            
        print(f'Uploading file from {file_path} to {destination_path}')
        with open(file_path, 'rb') as file:
            s3_client.upload_fileobj(file, bucket_name, destination_path)
            
        s3_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{destination_path}"
        return s3_url
        
    except ClientError as e:
        print(f"Error uploading to S3: {str(e)}")
        raise e
@task(log_prints=True)
def read_and_flatten_data(file_path: str):
    """Read JSON data and flatten nested structures using pandas"""
    print("Reading and flattening the data...")
    
    # Read the original data and flatten using pandas
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    df = pd.json_normalize(data)
    flattened_data = df.to_dict(orient='records')[0]
    
    # Save flattened data to a new file
    flattened_file_path = Path("flattened-data.json")
    flattened_file_path.write_text(json.dumps(flattened_data, indent=2))
    
    print(f"Data flattened and saved to {flattened_file_path}")
    return str(flattened_file_path)

@task(log_prints=True)
def validate_data(file_path: str, min_stars: int) -> tuple[bool, str]:
    """Validate the data against our schema and business rules"""
    print("Validating the data...")
    
    try:
        # Read the flattened data
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Validate against our schema
        repo_data = GithubRepoSchema(**data)
        
        # Additional business rules validation
        if repo_data.stargazers_count < min_stars:
            return False, f"Repository has fewer than {min_stars} stars"
        
        print("Data validation successful!")
        return True, "Validation successful"
        
    except ValidationError as e:
        error_message = str(e)
        print(f"Validation error: {error_message}")
        return False, error_message
        
    except Exception as e:
        error_message = str(e)
        print(f"Error during validation: {error_message}")
        return False, error_message

@task(log_prints=True)
def clean_data(file_path: str) -> str:
    """Clean and transform the data"""
    print("Cleaning the data...")
    
    try:
        # Read the flattened data
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Perform cleaning operations
        cleaned_data = {
            "name": data["name"],
            "full_name": data["full_name"],
            "stargazers_count": data["stargazers_count"],
            "watchers_count": data["watchers_count"],
            "forks_count": data["forks_count"]
        }
        
        # Save cleaned data to a new file
        cleaned_file_path = Path("cleaned-data.json")
        cleaned_file_path.write_text(json.dumps(cleaned_data, indent=2))
        
        print(f"Data cleaned and saved to {cleaned_file_path}")
        return str(cleaned_file_path)
        
    except Exception as e:
        print(f"Error during data cleaning: {str(e)}")
        raise e

@task(log_prints=True)
def save_cleaned_data(file_path: str, bucket: str) -> str:
    """Save the cleaned data to S3"""
    try:
        # Upload cleaned data to s3
        s3_url = upload_to_s3(
            file_path=file_path,
            destination_path="cleaned-data.json",
            bucket_name=bucket
        )
        print(f"Cleaned data saved to S3: {s3_url}")
        return s3_url
        
    except Exception as e:
        print(f"Error saving cleaned data: {str(e)}")
        raise e

@task(log_prints=True)
def perform_aggregation(file_path: str) -> str:
    """Perform aggregation operations on the cleaned data"""
    print("Starting data aggregation process...")
    
    try:
        # Read the cleaned data
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Perform aggregations
        aggregated_data = {
            "repository_name": data["name"],
            "total_engagement": data["stargazers_count"] + data["watchers_count"] + data["forks_count"],
            "metrics": {
                "total_stars": data["stargazers_count"],
                "total_watchers": data["watchers_count"],
                "total_forks": data["forks_count"],
            },
            "engagement_ratio": round(
                data["stargazers_count"] / (data["watchers_count"] + 1), 2
            ),
            "timestamp": pd.Timestamp.now().isoformat()
        }
        
        # Save aggregated data to a new file
        aggregated_file_path = Path("aggregated-data.json")
        aggregated_file_path.write_text(json.dumps(aggregated_data, indent=2))
        
        print(f"Data aggregation completed and saved to {aggregated_file_path}")
        return str(aggregated_file_path)
        
    except Exception as e:
        print(f"Error during data aggregation: {str(e)}")
        raise e

@task(log_prints=True)
def save_aggregated_data(file_path: str, bucket: str) -> str:
    """Save the aggregated data to S3"""
    try:
        # Upload aggregated data to s3
        s3_url = upload_to_s3(
            file_path=file_path,
            destination_path="aggregated-data.json",
            bucket_name=bucket
        )
        
        print(f"Aggregated data saved to S3: {s3_url}")
        return s3_url
        
    except Exception as e:
        print(f"Error saving aggregated data: {str(e)}")
        raise e

@flow(name="etl_s3_pipeline")
def main(repository: str, min_stars: int, output_bucket: str):
    """Main ETL pipeline flow"""
    # Step 1: Get data and upload to S3
    file_path = request_data_and_upload_to_s3(repository, output_bucket)
    
    # Step 2: Flatten the data
    flattened_file_path = read_and_flatten_data(file_path)
    
    # Step 3: Validate data
    is_valid, validation_message = validate_data(
        flattened_file_path, 
        min_stars=min_stars
    )
    if not is_valid:
        print(f"Validation failed: {validation_message}")
        return  # Stop processing if validation fails   
    
    # Step 4: Clean the data
    cleaned_file_path = clean_data(flattened_file_path)
    
    # Step 5: Save the cleaned data
    s3_path = save_cleaned_data(cleaned_file_path, bucket=output_bucket)
    
    # Step 6: Perform aggregation
    aggregated_file_path = perform_aggregation(cleaned_file_path)
    
    # Step 7: Save aggregated data
    final_s3_path = save_aggregated_data(
        aggregated_file_path, 
        bucket=output_bucket
    )
    
    print(f"ETL pipeline completed successfully. Final aggregated data saved to: {final_s3_path}")
    return final_s3_path

if __name__ == "__main__":
    # Example usage
    main(
        repository="PrefectHQ/prefect",
        min_stars=100,
        output_bucket="my-etl-results"
    ) 