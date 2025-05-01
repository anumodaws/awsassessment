import boto3
import json
import os
from collections import deque

dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

table = dynamodb.Table(os.environ['DYNAMODB_TABLE_NAME'])
queue_url = os.environ['ALERT_QUEUE_URL']
speed_threshold = int(os.environ.get('SPEED_THRESHOLD', '100'))

# Track speed over last 5 readings per vehicle
speed_window = {}

def lambda_handler(event, context):
    for record in event['Records']:
        payload = json.loads(record['kinesis']['data'], strict=False)
        vehicle_id = payload.get("vehicle_id")
        speed = payload.get("vehicle_speed", 0)

        if not vehicle_id:
            continue

        # Keep last 5 speeds
        history = speed_window.setdefault(vehicle_id, deque(maxlen=5))
        history.append(speed)

        # Check if all last 5 values exceed threshold
        if len(history) == 5 and all(s > speed_threshold for s in history):
            alert = {
                "vehicle_id": vehicle_id,
                "message": f"Speed limit exceeded consistently: {list(history)}"
            }
            # Send alert to SQS
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(alert))

        # Write to DynamoDB
        table.put_item(Item={
            "vehicle_id": vehicle_id,
            "timestamp": payload.get("timestamp"),
            "speed": speed,
            "location": payload.get("location")
        })

    return {"statusCode": 200}
