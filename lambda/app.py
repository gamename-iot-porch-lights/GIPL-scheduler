import os
from datetime import datetime

import boto3
import requests


def get_account_id():
    sts_client = boto3.client('sts')
    account_id = sts_client.get_caller_identity()["Account"]
    return account_id


def get_twilight_times(latitude, longitude):
    sunrise_utc = None
    sunset_utc = None
    date = datetime.now()  # Today's date
    url = (f"https://aa.usno.navy.mil/api/rstt/oneday?date={date.strftime('%Y-%m-%d')}&coords={latitude},{longitude}")
    print(url)
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
    data = response.json()
    print(data)
    sundata = data['properties']['data']['sundata']
    for entry in sundata:
        if entry['phen'] == 'Rise':
            sunrise_utc = entry['time']
        elif entry['phen'] == 'Set':
            sunset_utc = entry['time']

    if not sunrise_utc or not sunset_utc:
        raise ValueError("Sunrise or sunset time not found in the response")

    return sunrise_utc, sunset_utc


def schedule_illumination(trigger_time, message):
    eventbridge = boto3.client('scheduler')

    date = datetime.now()  # Today's date

    trigger_time_formatted = datetime.strptime(trigger_time, '%H:%M').replace(
        year=date.year, month=date.month, day=date.day
    )

    # Generate a unique name for the scheduled rule
    rule_name = f"turn_porch_lights_{message}_today_at_{trigger_time_formatted.strftime('%H%M')}_UTC"
    print(f"rule_name:\r{rule_name}")

    account_id = get_account_id()
    print(f"account_id:\r{account_id}")

    # Create the scheduled rule
    response = eventbridge.create_schedule(
        Name=rule_name,
        ScheduleExpression=f"at({trigger_time_formatted.strftime('%Y-%m-%dT%H:%M:%S')})",
        Target={
            'Arn': f'arn:aws:lambda:us-east-1:{account_id}:function:GIPL-illuminator',
            'RoleArn': f'arn:aws:iam::{account_id}:role/lambda-invoke-role',
            'Input': f'{{"light_switch": "{message}", "schedule_name": "{rule_name}"}}'
        },
        FlexibleTimeWindow={
            'Mode': 'OFF'
        }
    )
    print(f"response:\r{response}")


def lambda_handler(event, context):
    """
    Main handler function for the AWS Lambda. Fetches sun times and stores them in a DynamoDB table.

    Parameters:
    - event (dict): The event data passed to the Lambda function.
    - context (object): The context in which the Lambda function is running.

    Returns:
    - dict: A dictionary containing the status code and body message.
    """

    latitude = os.environ['LATITUDE']
    longitude = os.environ['LONGITUDE']

    try:
        sunrise, sunset = get_twilight_times(latitude, longitude)
    except (requests.RequestException, ValueError, KeyError) as e:
        print(f"Error fetching data from USNO API: {e}")
        return {"statusCode": 500, "body": str(e)}

    schedule_illumination(sunrise, "OFF")
    schedule_illumination(sunset, "ON")
