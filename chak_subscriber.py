import json
import requests
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883

API_BASE_URL = "http://<YOUR_SERVER_IP>:8000"

MIXING_TANK_TOPIC = "mixing_tank"
DISTRIBUTION_TANK_TOPIC = "distribution_tank/+"


def post_mixing_tank(payload):
    data = {
        "device_id": "mixing_tank_01",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": payload["temperature"],
        "ph": payload["ph"],
        "level": payload["level"],
        "dissolved_oxygen": payload["dissolved_oxygen"],
    }
    try:
        r = requests.post(f"{API_BASE_URL}/iot/mixing-tank", json=data)
        print(f"  [API] {r.status_code} {r.json()}")
    except requests.RequestException as e:
        print(f"  [API] Error: {e}")


def post_distribution_tank(bed, payload):
    data = {
        "device_id": f"dist_tank_{bed}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": payload["temperature"],
        "ph": payload["ph"],
        "level": payload["level"],
        "conductivity": payload["conductivity"],
    }
    try:
        r = requests.post(f"{API_BASE_URL}/iot/distribution-tank/{bed}", json=data)
        print(f"  [API] {r.status_code} {r.json()}")
    except requests.RequestException as e:
        print(f"  [API] Error: {e}")


def print_mixing_tank(payload):
    print("--- Mixing Tank ---")
    print(f"  Temperature      : {payload['temperature']} °C")
    print(f"  PH               : {payload['ph']}")
    print(f"  Level            : {payload['level']} mm")
    print(f"  Dissolved Oxygen : {payload['dissolved_oxygen']} mg/L")


def print_distribution_tank(bed, payload):
    print(f"--- Distribution Tank | {bed} ---")
    print(f"  Temperature  : {payload['temperature']} °C")
    print(f"  PH           : {payload['ph']}")
    print(f"  Level        : {payload['level']} mm")
    print(f"  Conductivity : {payload['conductivity']} µS/cm")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        client.subscribe(MIXING_TANK_TOPIC)
        client.subscribe(DISTRIBUTION_TANK_TOPIC)
        print(f"Subscribed to: {MIXING_TANK_TOPIC}")
        print(f"Subscribed to: {DISTRIBUTION_TANK_TOPIC}")
        print()
    else:
        print(f"Connection failed with code {rc}")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"[{msg.topic}] Failed to parse payload: {e}")
        return

    if msg.topic == MIXING_TANK_TOPIC:
        print_mixing_tank(payload)
        post_mixing_tank(payload)
    elif msg.topic.startswith("distribution_tank/"):
        bed = msg.topic.split("/")[1]
        print_distribution_tank(bed, payload)
        post_distribution_tank(bed, payload)

    print()


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_forever()
