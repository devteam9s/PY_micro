from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import json
from supabase_py import create_client
from supabase_py import Client as SupabaseClient
import uuid

app = Flask(__name__)
import httpx
# Supabase configuration

supabase_url = 'http://13.51.198.7:8000'
supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJhbm9uIiwKICAgICJpc3MiOiAic3VwYWJhc2UtZGVtbyIsCiAgICAiaWF0IjogMTY0MTc2OTIwMCwKICAgICJleHAiOiAxNzk5NTM1NjAwCn0.dc_X5iR_VP_qT0zsiyj_I_OZ2T9FtRU2BBNWN8Bu4GE'
table_name = 'mqtt_data'
supabase = create_client(supabase_url, supabase_key)

# MQTT configuration

mqtt_broker = '3.25.63.6'
mqtt_port = 3000

mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with code:", rc)
    client.subscribe("site1/pit1/voltage")
    client.subscribe("site1/pit1/current")
    client.subscribe("site1/pit1/resistance")
    client.subscribe("site1/pit1/time")

def on_message(client, userdata, message):
    try:
        payload = message.payload.decode("utf-8")
        topic = message.topic

        json_data = json.loads(payload) #convert valuto py dict

        print("Received message on topic:", topic)
        print("Message data:", json_data)

        data = {
            "id": str(uuid.uuid4()),
            "topic": topic,
            "payload": json_data
        }

        print("Inserting data:", data)

        response = supabase.table(table_name).insert([data]).execute()

        if isinstance(response, dict) and 'status_code' in response and response['status_code'] == 201:
            print("Data inserted successfully!")
        else:
            print("Error inserting data:", response)

    except Exception as e:
        print("Error processing MQTT message:", str(e))

# Set MQTT client

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect MQTT client
mqtt_client.connect(mqtt_broker, mqtt_port)
mqtt_client.loop_start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)