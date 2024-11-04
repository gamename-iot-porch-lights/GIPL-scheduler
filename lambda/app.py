import os
from datetime import datetime
from datetime import timedelta

import boto3
import pytz
import requests
from timezonefinder import TimezoneFinder


def get_current_timezone_name():
    # Define the Central timezone
    central = pytz.timezone('America/Chicago')

    # Get the current time in the Central timezone
    now = datetime.now(central)

    # Check if DST is in effect
    if now.dst() != timedelta(0):
        return "CDT"  # Central Daylight Time
    else:
        return "CST"  # Central Standard Time


def utc_to_local_time(utc_time_str, timezone_name):
    try:
        # Get the current date
        current_date = datetime.now().date()

        # Combine the current date with the provided UTC time
        combined_utc_str = f"{current_date} {utc_time_str}"

        # Define the UTC timezone
        utc_timezone = pytz.utc

        # Convert the combined string to a datetime object and localize to UTC
        utc_datetime = utc_timezone.localize(datetime.strptime(combined_utc_str, "%Y-%m-%d %H:%M"))

        # Get the local timezone object
        local_timezone = pytz.timezone(timezone_name)

        # Convert the UTC time to local time in the specified timezone
        local_time = utc_datetime.astimezone(local_timezone)

        # Format the local time to 'HH:MM'
        local_time_str = local_time.strftime('%H:%M')

        return local_time_str
    except pytz.UnknownTimeZoneError:
        return f"Unknown timezone: {timezone_name}"
    except ValueError:
        return "Incorrect time format. Please use 'HH:MM'."


def get_local_time(timezone_name):
    try:
        # Get the timezone object
        timezone = pytz.timezone(timezone_name)

        # Get the current time in the specified timezone
        local_time = datetime.now(timezone).strftime('%H:%M')

        return local_time
    except pytz.UnknownTimeZoneError:
        return f"Unknown timezone: {timezone_name}"


def get_utc_offset(timezone_name):
    try:
        # Create a timezone object
        tz = pytz.timezone(timezone_name)

        # Get the current time in the given timezone
        current_time = datetime.now(tz)

        # Get the offset for standard time
        standard_offset = tz.utcoffset(current_time.replace(tzinfo=None)).total_seconds() / 3600

        # Get the offset during daylight saving time
        dst_offset = tz.dst(current_time.replace(tzinfo=None)).total_seconds() / 3600 if tz.dst(
            current_time.replace(tzinfo=None)) else 0

        # Calculate the total offset during DST
        total_offset_during_dst = standard_offset + dst_offset

        return standard_offset, total_offset_during_dst

    except Exception as e:
        return str(e)


def is_dst(date, timezone):
    tz = pytz.timezone(timezone)
    aware_date = tz.localize(date, is_dst=None)
    return aware_date.dst() != timedelta(0)


def get_local_timezone_offset(date, timezone):
    standard_offset, dst_offset = get_utc_offset(timezone)

    if is_dst(date, timezone):
        return dst_offset  # CDT (Central Daylight Time)
    else:
        return standard_offset  # CST (Central Standard Time)


def get_timezone(latitude, longitude):
    # Create an instance of TimezoneFinder
    tf = TimezoneFinder()
    # Get the timezone name
    return tf.timezone_at(lat=float(latitude), lng=float(longitude))


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


def schedule_illumination(trigger_time_utc, trigger_time_local, local_timezone_name, message):
    eventbridge = boto3.client('scheduler')

    date = datetime.now()  # Today's date

    trigger_time_utc_formatted = datetime.strptime(trigger_time_utc, '%H:%M').replace(
        year=date.year, month=date.month, day=date.day
    )

    trigger_time_local_formatted = datetime.strptime(trigger_time_local, '%H:%M').replace(
        year=date.year, month=date.month, day=date.day
    )

    hr_min = trigger_time_local_formatted.strftime('%H%M')

    # Generate a unique name for the scheduled rule
    rule_name = f"turn_porch_lights_{message}_today_at_{hr_min}_{local_timezone_name}"
    print(f"rule_name:\r{rule_name}")

    account_id = get_account_id()
    print(f"account_id:\r{account_id}")

    # Create the scheduled rule
    response = eventbridge.create_schedule(
        Name=rule_name,
        ScheduleExpression=f"at({trigger_time_utc_formatted.strftime('%Y-%m-%dT%H:%M:%S')})",
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
        sunrise_utc, sunset_utc = get_twilight_times(latitude, longitude)
    except (requests.RequestException, ValueError, KeyError) as e:
        print(f"Error fetching data from USNO API: {e}")
        return {"statusCode": 500, "body": str(e)}

    timezone = get_timezone(latitude, longitude)
    sunrise_local = utc_to_local_time(sunrise_utc, timezone)
    sunset_local = utc_to_local_time(sunset_utc, timezone)

    timezone_name = get_current_timezone_name()

    schedule_illumination(sunrise_utc, sunrise_local, timezone_name, "OFF")
    schedule_illumination(sunset_utc, sunset_local, timezone_name, "ON")
