import json
import os
import requests
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

load_dotenv()

BROKER = os.getenv("BROKER", "localhost")
PORT = int(os.getenv("PORT", 1883))
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

MIXING_TANK_TOPIC = "mixing_tank"
DISTRIBUTION_TANK_TOPIC = "distribution_tank/+"


def _safe_response_body(r):
    try:
        return r.json()
    except ValueError:
        return r.text


def post_medicion(payload, bed=None):
    data = {
        "modulo": payload.get("module"),
        "tanque": payload.get("tank"),
        "balsa": bed if bed is not None else payload.get("bed"),
        "ph": payload["ph"],
        "temperatura": payload["temperature"],
        "nivel": payload["level"],
        "od": payload.get("OD", payload.get("dissolved_oxygen", 0)),
        "ce": payload.get("EC", payload.get("conductivity", 0)),
        "status": payload.get("status", "ok"),
    }
    try:
        r = requests.post(f"{API_BASE_URL}/medicion", json=data, timeout=5)
        print(f"  [API] {r.status_code} {_safe_response_body(r)}")
    except (requests.RequestException, ValueError) as e:
        print(f"  [API] Error: {e}")


def print_mixing_tank(payload):
    print("--- Mixing Tank ---")
    print(f"  Temperature      : {payload['temperature']} °C")
    print(f"  PH               : {payload['ph']}")
    print(f"  Level            : {payload['level']} mm")
    print(f"  Dissolved Oxygen : {payload.get('OD', payload.get('dissolved_oxygen', 0))} mg/L")


def print_distribution_tank(bed, payload):
    print(f"--- Distribution Tank | {bed} ---")
    print(f"  Temperature  : {payload['temperature']} °C")
    print(f"  PH           : {payload['ph']}")
    print(f"  Level        : {payload['level']} mm")
    print(f"  Conductivity : {payload.get('EC', payload.get('conductivity', 0))} µS/cm")


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
        post_medicion(payload)
    elif msg.topic.startswith("distribution_tank/"):
        bed = msg.topic.split("/")[1]
        print_distribution_tank(bed, payload)
        post_medicion(payload, bed=bed)

    print()


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_forever()
