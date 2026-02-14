import azure.functions as func
import logging
import json
import os
import smtplib
from email.message import EmailMessage
import typing

app = func.FunctionApp()

@app.event_hub_message_trigger(
    arg_name="myhub",
    event_hub_name="traffic-detection-events",
    connection="EventHubConnection",
    consumer_group="alert-function"
)
def traffic_alert_consumer(myhub):##: typing.List[func.EventHubEvent]):
    logging.info('Python EventHub trigger processed an event.')
    logging.error('Hello from inside')
    ##for event in myhub:
    event = myhub
    try:
        body = event.get_body().decode('utf-8')
        data = json.loads(body)
        check_for_alert(data)
    except Exception as e:
        logging.error(f"Error processing event: {e}")

def check_for_alert(data):
    speed = data.get('avg_speed_kmh', 0)
    vehicle_id = data.get('event_id', 'unknown')

    if speed > 130:
        logging.warning(f"ALERT TRIGGERED: Vehicle {vehicle_id} doing {speed} km/h")
        send_email_alert(data)
    else:
        logging.info(f"Vehicle {vehicle_id} within limits ({speed} km/h)")

def send_email_alert(data):
    gmail_user = os.environ["GMAIL_USER"]
    gmail_password = os.environ["GMAIL_APP_PASSWORD"]
    recipient = os.environ["ALERT_RECIPIENT"]

    msg = EmailMessage()
    msg["From"] = gmail_user
    msg["To"] = recipient
    msg["Subject"] = f"URGENT: Speeding Alert - {data['avg_speed_kmh']} km/h"

    msg.set_content("Your email client does not support HTML.")
    msg.add_alternative(
        f"""
        <strong>High Speed Detected</strong><br>
        Vehicle ID: {data['event_id']}<br>
        Speed: {data['max_speed_kmh']} km/h<br>
        Time: {data['timestamp_utc']}<br>
        Camera: {data['source_video_id']}
        """,
        subtype="html",
    )

    try:
        logging.error('Trying to send email')
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.starttls()
            server.login(gmail_user, gmail_password)
            server.send_message(msg)

        logging.info("Email alert sent successfully")

    except Exception as e:
        logging.error(f"Failed to send email: {e}")
