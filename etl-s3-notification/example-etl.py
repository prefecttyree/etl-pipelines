import boto3
import httpx
import json
import pandas as pd
from pathlib import Path
from prefect import flow, task, pause_flow_run, runtime
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket
from pydantic import BaseModel, ValidationError
from typing import Optional, Dict, Any
from prefect_email import EmailServerCredentials, email_send_message

# Configuration variables
AWS_CREDENTIALS_BLOCK = "your-aws-credentials-block-name"  # e.g., "my-aws-creds"
S3_BUCKET_NAME = "your-s3-bucket-name"  # e.g., "my-etl-bucket"
AWS_REGION = "your-aws-region"  # e.g., "us-east-1"
EMAIL_CREDENTIALS_BLOCK = "your-email-credentials-block-name"  # e.g., "my-email-notifications"
NOTIFICATION_EMAIL = "your-email@example.com"
MIN_STARS_THRESHOLD = 10  # Minimum number of stars required for validation

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

@task(log_prints=True)
def request_data_and_upload_to_s3(repo: str):
    """Fetch the statistics for a GitHub repo and upload to S3"""
    print("Requesting data from the API...")
    repo_url = f"https://api.github.com/repos/{repo}"
    response = httpx.get(repo_url)
    
    file_path = Path("raw-data-example.txt")
    file_path.write_text(response.text)
    
    # Upload raw data to s3
    upload_to_s3(str(file_path), "raw-data-example.txt", S3_BUCKET_NAME)
    print(f"Raw data upload: {file_path}")
    
    return str(file_path)

@task(name="upload_to_s3", log_prints=True)
def upload_to_s3(file_path: str, destination_path: str, bucket_name: str):
    """Uploads the file data to an S3 bucket."""
    aws_credentials = AwsCredentials.load(AWS_CREDENTIALS_BLOCK)
    s3_bucket = S3Bucket(
        bucket_name=bucket_name,
        credentials=aws_credentials
    )
    
    print(f'Uploading file from {file_path} to {destination_path}')
    s3_bucket_path = s3_bucket.upload_from_path(file_path)
    downloaded_file_path = s3_bucket.download_object_to_path(
        s3_bucket_path, destination_path
    )
    s3_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{destination_path}"
    return s3_url

def send_error_email(error_message: str):
    """Helper function to send validation error emails"""
    email_server_credentials = EmailServerCredentials.load(EMAIL_CREDENTIALS_BLOCK)

    try:
        email_send_message.with_options(name="etl-pipeline-validation-failed").submit(
            email_server_credentials=email_server_credentials,
            subject="ETL Pipeline Validation Failed",
            msg=f"""
            ❌ Data Validation Failed
            
            Error Details:
            {error_message}
            link to the flow: {runtime.flow_run.ui_url}
            """,
            email_to=NOTIFICATION_EMAIL
        )
        print(f"Validation error email sent to {NOTIFICATION_EMAIL}")
    except Exception as e:
        print(f"Failed to send notification: {str(e)}")

def send_completion_notification(s3_url: str):
    """Helper function to send completion notification"""
    email_server_credentials = EmailServerCredentials.load(EMAIL_CREDENTIALS_BLOCK)

    try:
        email_send_message.with_options(name="etl-pipeline-completed").submit(
            email_server_credentials=email_server_credentials,
            subject="ETL Pipeline Completed Successfully",
            msg=f"""
            ✅ ETL Pipeline Completed
            
            The pipeline has successfully processed and aggregated the data.
            
            Flow run link: {runtime.flow_run.ui_url}
            """,
            email_to=NOTIFICATION_EMAIL
        )
        print(f"Completion notification sent successfully. Data available at: {s3_url}")
    except Exception as e:
        print(f"Failed to send notification: {str(e)}")

@task(log_prints=True)
def read_and_flatten_data(file_path: str):
    """Read JSON data and flatten nested structures using pandas"""
    print("Reading and flattening the data...")
    
    # Read the original data and flatten using pandas
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    df = pd.json_normalize(data)
    flattened_data = df.to_dict(orient='records')[0]
    print(f'flattened data: {flattened_data}')
    
    # Save flattened data
    flat_file_path = Path("flattened-data.txt")
    flat_file_path.write_text(json.dumps(flattened_data, indent=4))
    
    return str(flat_file_path)

@task(log_prints=True, retries=2)
def validate_data(file_path: str) -> tuple[bool, str]:
    """
    Validates the flattened JSON data against our defined schema.
    """
    try:
        # Read the JSON data
        print(f"Reading data from {file_path} for validation...")
        with open(file_path, 'r') as f:
            flattened_data = json.load(f)
        
        # Restructure the data to match our schema
        data = {
            "name": flattened_data["name"],
            "full_name": flattened_data["full_name"],
            "stargazers_count": flattened_data["stargazers_count"],
            "watchers_count": flattened_data["watchers_count"],
            "forks_count": flattened_data["forks_count"],
        }
        
        # Attempt to validate against our schema
        print("Validating data against schema...")
        validated_data = GithubRepoSchema(**data)
        
        # Additional business rules validation
        validation_errors = []
        
        # Example business rule: Ensure stargazers count is reasonable
        if validated_data.stargazers_count < MIN_STARS_THRESHOLD:
            validation_errors.append(
                f"Stargazers count ({validated_data.stargazers_count}) cannot be less than {MIN_STARS_THRESHOLD}"
            )
            
        # If there are validation errors, send email and return False
        if validation_errors:
            error_message = "Business rule validation failed: " + "; ".join(validation_errors)
            print(f"Validation failed: {error_message}")
            raise Exception(send_error_email(error_message))
        
        print("Data validation successful!")
        return True, "Data validation successful"
        
    except ValidationError as e:
        error_message = "Schema validation failed: " + str(e)
        print(f"Validation error: {error_message}")
        return False, error_message
    
    except Exception as e:
        error_message = f"Validation error: {str(e)}"
        print(f"Unexpected error: {error_message}")
        return False, error_message

@task(log_prints=True)
def clean_data(file_path: str) -> str:
    """Clean and structure the validated data according to GithubRepoSchema"""
    print("Starting data cleaning process...")
    
    try:
        # Read the validated data
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Create cleaned data structure following GithubRepoSchema
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
def save_cleaned_data(file_path: str) -> str:
    """Save the cleaned data to S3"""
    try:
        # Upload cleaned data to s3
        s3_path = upload_to_s3(
            file_path=file_path,
            destination_path="cleaned-data.json",
            bucket_name=S3_BUCKET_NAME
        )
        print(f"Cleaned data saved to S3: {s3_path}")
        return s3_path
        
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
def save_aggregated_data(file_path: str) -> str:
    """Save the aggregated data to S3 and send completion notification"""
    try:
        # Upload aggregated data to s3
        s3_path = upload_to_s3(
            file_path=file_path,
            destination_path="aggregated-data.json",
            bucket_name=S3_BUCKET_NAME
        )
        
        # Send completion notification
        send_completion_notification(s3_path)
        
        print(f"Aggregated data saved to S3: {s3_path}")
        return s3_path
        
    except Exception as e:
        print(f"Error saving aggregated data: {str(e)}")
        raise e

@flow(name="etl_s3_pipeline")
def main():
    """Main ETL pipeline flow"""
    # Step 0: Get the URL
    repo = pause_flow_run(wait_for_input=str)
    
    # Step 1: Get data and upload to S3
    file_path = request_data_and_upload_to_s3(repo)
    
    # Step 2: Flatten the data
    flattened_file_path = read_and_flatten_data(file_path)
    
    # Step 3: Validate and notify
    is_valid, validation_message = validate_data(flattened_file_path)
    if not is_valid:
        return  # Stop processing if validation fails   
    
    # Step 4: Clean the data
    cleaned_file_path = clean_data(flattened_file_path)
    
    # Step 5: Save the cleaned data
    s3_path = save_cleaned_data(cleaned_file_path)
    
    # Step 6: Perform aggregation
    aggregated_file_path = perform_aggregation(cleaned_file_path)
    
    # Step 7: Save aggregated data and send completion notification
    final_s3_path = save_aggregated_data(aggregated_file_path)
    
    print(f"ETL pipeline completed successfully. Final aggregated data saved to: {final_s3_path}")

if __name__ == "__main__":
    main() 