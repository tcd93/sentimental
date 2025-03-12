"""
Lambda function to handle and report errors from the sentiment analysis pipeline.
"""

import json
import os
from datetime import datetime
import boto3

sns = boto3.client('sns')
ERROR_SNS_TOPIC = os.environ['ERROR_SNS_TOPIC']

def lambda_handler(event, _):
    """
    Handle errors from Step Functions execution.
    
    Args:
        event: Dict containing error details
        _: Lambda context
    
    Returns:
        Dict containing error handling status
    """
    error_message = {
        'error': event.get('error', 'Unknown error'),
        'cause': event.get('cause', 'Unknown cause'),
        'keyword': event.get('keyword', 'Unknown keyword'),
        'timestamp': datetime.utcnow().isoformat(),
        'execution_id': event.get('execution_id', 'Unknown'),
        'state': event.get('state', 'Unknown')
    }
    
    sns.publish(
        TopicArn=ERROR_SNS_TOPIC,
        Subject='Sentiment Analysis Pipeline Error',
        Message=json.dumps(error_message, indent=2)
    )
    
    return {
        'statusCode': 200,
        'error_handled': True,
        'message': 'Error recorded and notification sent'
    } 