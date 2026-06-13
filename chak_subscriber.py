import json
import os
import requests
from dotenv import load_dotenv
from flask import Flask, request as flask_request, jsonify
import paho.mqtt.client as mqtt

load_dotenv()

BROKER = os.getenv("BROKER", "localhost")
PORT = int(os.getenv("PORT", 1883))
HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("HTTP_PORT", 8001))
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_KEY = os.getenv("API_KEY", "")

MIXING_TANK_TOPIC = "mixing_tank"
DISTRIBUTION_TANK_TOPIC = "distribution_tank/+"
REQUEST_TOPIC = "request"


def _safe_response_body(r):
    """Return parsed JSON if possible, else the raw text."""
    try:
        return r.json()
    except ValueError:
        return r.text


def post_medicion(payload):
    data = {
        "modulo": str(payload.get("module", "")),
        "tanque": int(payload.get("tank", 0)),
        "balsa": int(payload.get("bed", 0)),
        "ph": payload["ph"],
        "temperatura": payload["temperature"],
        "nivel": payload["level"],
        "od": payload.get("OD", 0),
        "ce": payload.get("EC", 0),
        "status": payload.get("status", "ok"),
    }
    try:
        r = requests.post(
            f"{API_BASE_URL}/medicion",
            json=data,
            headers={"X-API-Key": API_KEY},
            timeout=5,
        )
        print(f"  [API] {r.status_code} {_safe_response_body(r)}")
    except (requests.RequestException, ValueError) as e:
        print(f"  [API] Error: {e}")


def print_mixing_tank(payload):
    print("--- Mixing Tank ---")
    print(f"  Module           : {payload.get('module')}")
    print(f"  Tank             : {payload.get('tank')}")
    print(f"  Bed              : {payload.get('bed')}")
    print(f"  Temperature      : {payload['temperature']} °C")
    print(f"  PH               : {payload['ph']}")
    print(f"  Level            : {payload['level']} %")
    print(f"  Dissolved Oxygen : {payload['OD']} mg/L")
    print(f"  Conductivity     : {payload.get('EC', 0)} µS/cm")
    print(f"  Status           : {payload.get('status', 'ok')}")


def print_distribution_tank(bed, payload):
    print(f"--- Distribution Tank | {bed} ---")
    print(f"  Module       : {payload.get('module')}")
    print(f"  Tank         : {payload.get('tank')}")
    print(f"  Bed          : {payload.get('bed', bed)}")
    print(f"  Temperature  : {payload['temperature']} °C")
    print(f"  PH           : {payload['ph']}")
    print(f"  Level        : {payload['level']} %")
    print(f"  Conductivity : {payload['EC']} µS/cm")
    print(f"  Dissolved O2 : {payload.get('OD', 0)} mg/L")
    print(f"  Status       : {payload.get('status', 'ok')}")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        client.subscribe(MIXING_TANK_TOPIC)
        client.subscribe(DISTRIBUTION_TANK_TOPIC)
        print(f"Subscribed to: {MIXING_TANK_TOPIC}")
        print(f"Subscribed to: {DISTRIBUTION_TANK_TOPIC}")
        print(f"Will publish measurement requests to: {REQUEST_TOPIC}")
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
        post_medicion(payload)

    print()


# paho-mqtt ≥ 2.0 deprecated the implicit callback API; pin to V1 if available
# so the existing on_connect/on_message signatures keep working without warnings.
try:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
except AttributeError:
    # paho-mqtt 1.x doesn't expose CallbackAPIVersion
    client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()


# ─── HTTP API: backend → subscriber → ESP32 ───────────────────────
app = Flask(__name__)


@app.route("/request", methods=["POST"])
def handle_request():
    """Receive a measurement request from the backend and forward it
    to the ESP32 nodes via the MQTT request topic.

    Expected JSON: {"module": <uuid>, "tank": <int>, "bed": <int>}
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("module", "tank", "bed") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    payload = {
        "module": data["module"],
        "tank": data["tank"],
        "bed": data["bed"],
    }
    # QoS 1 = at-least-once, so a transient broker hiccup doesn't drop the request.
    result = client.publish(REQUEST_TOPIC, json.dumps(payload), qos=1)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        return jsonify({"error": f"MQTT publish failed (rc={result.rc})"}), 502

    print(f"[REQUEST] -> {REQUEST_TOPIC}: {payload}")
    return jsonify({"status": "queued", "topic": REQUEST_TOPIC, "payload": payload}), 202


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "broker": BROKER, "port": PORT}), 200


if __name__ == "__main__":
    try:
        app.run(host=HTTP_HOST, port=HTTP_PORT, debug=False)
    finally:
        client.loop_stop()
        client.disconnect()
