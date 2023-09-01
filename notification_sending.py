from flask import Flask
import json
import firebase_admin
from firebase_admin import credentials, messaging
import supabase
import time
import subprocess
import requests
import httpx
app = Flask(__name__)

# Initialize Firebase
cred = credentials.Certificate('D:\supabase_mqtt\ies-67563-firebase-adminsdk-6epw7-4ebad2bfc3.json')
firebase_admin.initialize_app(cred)

# Initialize Supabase
supabase_url = 'http://13.51.198.7:8000'
supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJhbm9uIiwKICAgICJpc3MiOiAic3VwYWJhc2UtZGVtbyIsCiAgICAiaWF0IjogMTY0MTc2OTIwMCwKICAgICJleHAiOiAxNzk5NTM1NjAwCn0.dc_X5iR_VP_qT0zsiyj_I_OZ2T9FtRU2BBNWN8Bu4GE'
supabase_client = supabase.Client(supabase_url, supabase_key)

def send_firebase_notification(device_id, payload, topic):
    message = messaging.Message(
        notification=messaging.Notification(
            title='Threshold Notification',
            body=f'Threshold crossed: Topic: {topic}, Payload value {payload}'
        ),
        token=device_id
    )
    response = messaging.send(message)
    return response

def fetch_live_data():
    try:
        headers = {
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json',
            'apikey': supabase_key
        }

        response = httpx.get(f'{supabase_url}/rest/v1/rpc/get_all_mqtt_data', headers=headers)

        if response.status_code == 200:
            live_data = response.json()
            return live_data
        else:
            print('Error fetching live data:', response.text)
            return []

    except Exception as e:
        print('Error:', e)
        return []

@app.route('/')
def home():
    return "Flask MQTT Data Processing"

@app.route('/test_process', methods=['POST'])
def test_process():
    device_id ='e2_W-9muQqeAQ1ck6aaqwt:APA91bEHt3yGPLgIrgHiRzQ8WU3MQzeTCai6khJ9KrYcfWT_8YDIP8YggRi3GLmiUbtCZmPWXXGQhnNlJ7TQnADGxml5-8FUQAdxIHyTjMmBq0XbD1J17pGGy9LrNWeNXrv0XC1i2rQm'
    if not device_id:
        return "Please provide a device ID", 400

    while True:
        live_data = fetch_live_data()

        if live_data is None:
            print("No live data available")
            time.sleep(10)
            continue

        for data in live_data:
            topic = data.get('topic')
            payload = float(data.get('payload', 0))  # Convert payload to float

            if topic and payload > 3.0:
                notification = f"Threshold Crossed - Topic: {topic}, Payload: {payload}, Device ID: {device_id}"
                # Send Firebase notification
                send_firebase_notification(device_id, payload, topic)

        time.sleep(5)

    return json.dumps({"status": "Notifications continuously monitored"})
