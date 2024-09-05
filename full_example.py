import boto3
import time
import json
# Set environment
USE_LOCALSTACK = True  # Change to False if using real AWS account
# Specify a region
region = 'us-east-1'  # You can choose any valid AWS region

# Initialize the session
session = boto3.Session(
    aws_access_key_id='dummy',  # Dummy values for LocalStack
    aws_secret_access_key='dummy',
    region_name=region  # Specify the region here
)
if USE_LOCALSTACK:
    # Configuration for LocalStack
    sqs = session.client('sqs', endpoint_url='http://localhost:4566')
    s3 = session.client('s3', endpoint_url='http://localhost:4566')
else:
    # Configuration for real AWS
    sqs = session.client('sqs')
    s3 = session.client('s3')

def create_queue(queue_name, retention_period=None, dlq_arn=None, visibility_timeout=30):
    attributes = {}
    
    if retention_period:
        attributes['MessageRetentionPeriod'] = str(retention_period)  # Set retention period
    if visibility_timeout:
        attributes['VisibilityTimeout'] = str(visibility_timeout)  # Set visibility timeout
    
    if dlq_arn:
        # Convert RedrivePolicy dictionary to JSON string
        redrive_policy = {
            'deadLetterTargetArn': dlq_arn,
            'maxReceiveCount': '5'  # Set the max receive count before moving to DLQ
        }
        attributes['RedrivePolicy'] = json.dumps(redrive_policy)

    response = sqs.create_queue(
        QueueName=queue_name,
        Attributes=attributes
    )
    print(f'Queue {queue_name} created with URL: {response["QueueUrl"]}')
    return response['QueueUrl']
# Create S3 Bucket
def create_s3_bucket(bucket_name):
    response = s3.create_bucket(Bucket=bucket_name)
    print(f'Bucket {bucket_name} created.')
    return response

# Send message to SQS Queue
def send_message(queue_url, message_body):
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body
    )
    print(f'Message sent to {queue_url} with ID: {response["MessageId"]}')
    return response

# Receive message from SQS Queue
def receive_message(queue_url):
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1
    )
    messages = response.get('Messages', [])
    if messages:
        print(f'Received message: {messages[0]["Body"]}')
        return messages[0]
    else:
        print('No messages found.')
        return None

# Delete message from SQS Queue
def delete_message(queue_url, receipt_handle):
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    print('Message deleted.')

# Main flow
def main():
    queue_name = 'mainqueue1'
    dlq_name = 'dlq-queue1'
    # bucket_name = 'de-test-bucket'

    # Create Dead-Letter Queue (DLQ)
    dlq_url = create_queue(dlq_name,130)
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']

    # Create Main Queue with DLQ and retention period
    queue_url = create_queue(queue_name, retention_period=120, dlq_arn=dlq_arn)

    # # Create S3 Bucket (for large messages, if needed)
    # create_s3_bucket(bucket_name)

    # Send a message to the SQS queue
    send_message(queue_url, 'This message will be tested for DLQ')

    # Simulate receiving the message multiple times to trigger DLQ movement
    for attempt in range(6):
        message = receive_message(queue_url)
        if message:
            print(f"Attempt {attempt + 1}: Received message but not deleting.")
        time.sleep(2)  # Short sleep to simulate reprocessing failure

    # Check DLQ for the moved message
    dlq_messages = receive_message(dlq_url)
    if dlq_messages:
        print(f"Message has been moved to DLQ: {dlq_messages}")

if __name__ == "__main__":
    main()
