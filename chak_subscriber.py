import json
import os
import threading
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

# Solicitudes pendientes: tanque (int) -> {modulo, tanque, balsa}
_pending: dict = {}
_pending_lock = threading.Lock()


def _safe_response_body(r):
    """Return parsed JSON if possible, else the raw text."""
    try:
        return r.json()
    except ValueError:
        return r.text


def post_medicion(payload, context=None):
    ctx = context or {}
    data = {
        "modulo": ctx.get("modulo", str(payload.get("module", ""))),
        "tanque": ctx.get("tanque", int(payload.get("tank", 0))),
        "balsa":  ctx.get("balsa",  int(payload.get("bed", 0))),
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
    print(f"  Level            : {payload['level']} L")
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
    print(f"  Level        : {payload['level']} L")
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

    print(f"[MQTT] <- {msg.topic}")

    if msg.topic == MIXING_TANK_TOPIC:
        tank_num = int(payload.get("tank", 0))
        try:
            print_mixing_tank(payload)
        except Exception as e:
            print(f"  [WARN] print error: {e}")
    elif msg.topic.startswith("distribution_tank/"):
        bed = msg.topic.split("/")[1]
        tank_num = int(payload.get("tank", 0))
        try:
            print_distribution_tank(bed, payload)
        except Exception as e:
            print(f"  [WARN] print error: {e}")
    else:
        return

    with _pending_lock:
        context = _pending.pop(tank_num, None)

    print(f"  [-> API] Enviando medicion tanque={tank_num}")
    post_medicion(payload, context)

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


@app.route("/medir", methods=["POST"])
def handle_medir():
    """Receive a measurement request from the backend and forward it
    to the ESP32 nodes via the MQTT request topic.

    Expected JSON: {"modulo": <uuid>, "tanque": <int>, "balsa": <int>}
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "tanque", "balsa") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    tanque = int(data["tanque"])
    balsa  = int(data["balsa"])
    context = {"modulo": data["modulo"], "tanque": tanque, "balsa": balsa}

    with _pending_lock:
        _pending[tanque] = context

    payload = {"modulo": data["modulo"], "tanque": tanque, "balsa": balsa}
    # QoS 1 = at-least-once, so a transient broker hiccup doesn't drop the request.
    result = client.publish(REQUEST_TOPIC, json.dumps(payload), qos=1)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        return jsonify({"error": f"MQTT publish failed (rc={result.rc})"}), 502

    print(f"[MEDIR] -> {REQUEST_TOPIC}: {payload}")
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
