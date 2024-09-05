import boto3

def list_queues(sqs):
    # List all SQS queues
    response = sqs.list_queues()

    # Check if any queues were returned
    if 'QueueUrls' in response:
        print("List of SQS queues:")
        for queue_url in response['QueueUrls']:
            print(queue_url)
    else:
        print("No queues found.")

# Send message to SQS Queue
def send_message(queue_url,sqs, message_body):
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body
    )
    print(f'Message sent to {queue_url} with ID: {response["MessageId"]}')
    return response

# Receive message from SQS Queue
def receive_message(queue_url,sqs):
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
    
def get_queue_visibility_timeout(sqs, queue_url):
    # Get the queue attributes
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['VisibilityTimeout']
    )
    
    # Extract and print the VisibilityTimeout attribute
    visibility_timeout = response['Attributes'].get('VisibilityTimeout')
    print(f"Visibility Timeout for the queue is: {visibility_timeout} seconds")


def main():
    region = 'us-east-1'  # You can choose any valid AWS region

    # Initialize the session
    session = boto3.Session(
        aws_access_key_id='dummy',  # Dummy values for LocalStack
        aws_secret_access_key='dummy',
        region_name=region  # Specify the region here
    )

    sqs = session.client('sqs', endpoint_url='http://localhost:4566')
    
    # Call the function to list queues
    list_queues(sqs)

if __name__ == '__main__':
    main()
