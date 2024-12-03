import boto3
import os
import json

# Initialize the AWS Glue client
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    print("Event received: ", json.dumps(event))
    try:
        # Extract bucket name and object key from the S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        # Glue job name from environment variables
        glue_job_name = os.environ['GLUE_JOB_NAME']
        print("GLUE_JOB_NAME: ", glue_job_name)
        # Additional parameters
        sns_topic_arn = os.environ.get('SNS_TOPIC_ARN', 'default-sns-topic-arn')  # Replace with a default or fetch dynamically
        alert_threshold = os.environ.get('ALERT_THRESHOLD', '10')  # Default to 10% if not set
        expected_schema = os.environ.get('EXPECTED_SCHEMA', '{"Product ID": "string", "Quantity": "integer"}')  # Provide a default schema

        # Log the received event details
        print(f"File uploaded: s3://{bucket_name}/{object_key}")
        
        # Trigger the Glue job with all required parameters
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--BUCKET_NAME': bucket_name,
                '--OBJECT_KEY': object_key,
                '--SNS_TOPIC_ARN': sns_topic_arn,
                '--ALERT_THRESHOLD': alert_threshold,
                '--EXPECTED_SCHEMA': expected_schema
            }
        )
        
        # Log the Glue job run details
        print(f"Started Glue job '{glue_job_name}' with JobRunId: {response['JobRunId']}")
        return {
            'statusCode': 200,
            'body': f"Successfully triggered Glue job {glue_job_name}"
        }
    
    except Exception as e:
        print(f"Error triggering Glue job: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }
