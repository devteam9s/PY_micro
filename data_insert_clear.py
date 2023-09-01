from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import json
from supabase_py import create_client
import requests
from supabase_py import Client as SupabaseClient
import uuid
import time
app = Flask(__name__)
import httpx

# Supabase configuration
supabase_url = 'http://13.51.198.7:8000'
supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJhbm9uIiwKICAgICJpc3MiOiAic3VwYWJhc2UtZGVtbyIsCiAgICAiaWF0IjogMTY0MTc2OTIwMCwKICAgICJleHAiOiAxNzk5NTM1NjAwCn0.dc_X5iR_VP_qT0zsiyj_I_OZ2T9FtRU2BBNWN8Bu4GE'
table_name = 'mqtt_data1'
supabase = create_client(supabase_url, supabase_key)

# MQTT configuration

mqtt_broker = '3.25.63.6'
mqtt_port = 3000

mqtt_client = mqtt.Client()


inserted_records_count = 0
max_records_before_clear = 50

def insert_data(topic, payload):
    global inserted_records_count
    data = {
        "id": str(uuid.uuid4()),
        "topic": topic,
        "payload": payload,
        "sequence": inserted_records_count + 1
    }
    response = requests.post(
        f"{supabase_url}/rest/v1/{table_name}",
        headers={"apikey": supabase_key},
        json=[data]
    )

    if response.status_code == 201:
        inserted_records_count += 1
        print("Data inserted successfully! Total inserted:", inserted_records_count)

        if inserted_records_count >= max_records_before_clear:
            clear_table()

def clear_table():
    global inserted_records_count
    print("Clearing the table...")

    # Delete records

    response = requests.delete(
        f"{supabase_url}/rest/v1/{table_name}?sequence=lt.{max_records_before_clear + 1}",
        headers={"apikey": supabase_key}
    )

    if response.status_code == 204:
        inserted_records_count = 0
        print("Table cleared.")

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
        insert_data(topic, payload)

    except Exception as e:
        print("Error processing MQTT message:", str(e))

# Set MQTT client call
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect MQTT client
mqtt_client.connect(mqtt_broker, mqtt_port)
mqtt_client.loop_start()

if __name__ == '__main__':
    try:
        while True:
            pass  # Keep the script running
    except KeyboardInterrupt:
        mqtt_client.disconnect()