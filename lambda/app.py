from datetime import datetime
from datetime import timedelta

import boto3
import pytz


def get_current_timezone_name():
    central = pytz.timezone('America/Chicago')
    now = datetime.now(central)
    return "CDT" if now.dst() != timedelta(0) else "CST"


def get_account_id():
    sts_client = boto3.client('sts')
    return sts_client.get_caller_identity()["Account"]


def get_twilight_times_from_ddb(date):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('twilight-times')
    date_str = date.strftime('%Y-%m-%d')
    try:
        response = table.get_item(Key={'Date': date_str})
        if 'Item' not in response:
            raise ValueError(f"No twilight times found for {date_str}")
        item = response['Item']
        return item['SunriseLocal'], item['SunsetLocal']
    except Exception as e:
        print(f"Error fetching from twilight-times for {date_str}: {e}")
        raise


def schedule_illumination(trigger_time_local, local_timezone_name, message):
    eventbridge = boto3.client('scheduler')

    local_tz = pytz.timezone('America/Chicago')
    date = datetime.now(local_tz).date()

    trigger_time_local_formatted = datetime.strptime(trigger_time_local, '%H:%M').replace(
        year=date.year, month=date.month, day=date.day
    )

    trigger_time_local_dt = local_tz.localize(trigger_time_local_formatted)
    trigger_time_utc_formatted = trigger_time_local_dt.astimezone(pytz.utc)

    date_str = date.strftime('%Y%m%d')
    hr_min = trigger_time_local_formatted.strftime('%H%M')
    rule_name = f"turn_porch_lights_{message}_{date_str}_at_{hr_min}_{local_timezone_name}"
    print(f"rule_name:\r{rule_name}")

    account_id = get_account_id()
    print(f"account_id:\r{account_id}")

    response = eventbridge.create_schedule(
        Name=rule_name,
        ScheduleExpression=f"at({trigger_time_utc_formatted.strftime('%Y-%m-%dT%H:%M:%S')})",
        Target={
            'Arn': f'arn:aws:lambda:us-east-1:{account_id}:function:GIPL-illuminator',
            'RoleArn': f'arn:aws:iam::{account_id}:role/lambda-invoke-role',
            'Input': f'{{"light_switch": "{message}", "schedule_name": "{rule_name}"}}',
            'RetryPolicy': {
                'MaximumRetryAttempts': 20,
                'MaximumEventAgeInSeconds': 3600
            }
        },
        FlexibleTimeWindow={'Mode': 'OFF'}
    )
    print(f"response:\r{response}")


def lambda_handler(event, context):
    date = datetime.now(pytz.timezone('America/Chicago')).date()
    timezone_name = get_current_timezone_name()

    try:
        sunrise_local, sunset_local = get_twilight_times_from_ddb(date)
    except Exception as e:
        print(f"Error fetching twilight times: {e}")
        return {"statusCode": 500, "body": str(e)}

    schedule_illumination(sunrise_local, timezone_name, "OFF")
    schedule_illumination(sunset_local, timezone_name, "ON")

    return {"statusCode": 200, "body": "Schedules created successfully"}