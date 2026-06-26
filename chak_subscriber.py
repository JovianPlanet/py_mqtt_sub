import asyncio
import json
import os
import threading
import requests
from dotenv import load_dotenv
from flask import Flask, request as flask_request, jsonify
import paho.mqtt.client as mqtt

from a64_client import A64Client

load_dotenv()

BROKER = os.getenv("BROKER", "localhost")
PORT = int(os.getenv("PORT", 1883))
HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("HTTP_PORT", 8001))
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_KEY = os.getenv("API_KEY", "")

# ── Kincony KC868-A64 ─────────────────────────────────────────────────────────
A64_HOST          = os.getenv("A64_HOST", "192.168.0.103")
A64_PORT          = int(os.getenv("A64_PORT", "6053"))
A64_NOISE_KEY     = os.getenv("A64_NOISE_KEY", "Dw3Z3r2KbL05KstmqaTSWpxvY/6A4WoRcOUKgq6W99Y=")
A64_EXPECTED_NAME = os.getenv("A64_EXPECTED_NAME", "produccion")
SUCTION_DURATION_S = int(os.getenv("SUCTION_DURATION_S", str(10 * 60)))
RETURN_DURATION_S  = int(os.getenv("RETURN_DURATION_S",  str(10 * 60)))

MIXING_TANK_TOPIC    = "mixing_tank"
DISTRIBUTION_TANK_TOPIC = "distribution_tank/+"
REQUEST_TOPIC = "request"
STATUS_TOPIC  = "status/+"

# Solicitudes pendientes: tanque (int) -> {modulo, tanque, balsa}
_pending: dict = {}
_pending_lock = threading.Lock()

# Estado de conexión de nodos: "status/tank_N" -> "online" | "offline"
_node_status: dict = {}

# Retornos pendientes: tanque (int) -> {"balsa": int}
_return_pending: dict = {}
_return_lock = threading.Lock()


def _safe_response_body(r):
    try:
        return r.json()
    except ValueError:
        return r.text


def post_medicion(payload, context=None):
    ctx = context or {}
    data = {
        "modulo":      ctx.get("modulo", str(payload.get("module", ""))),
        "tanque":      ctx.get("tanque", int(payload.get("tank", 0))),
        "balsa":       ctx.get("balsa",  int(payload.get("bed", 0))),
        "ph":          float(payload["ph"]),
        "temperatura": float(payload["temperature"]),
        "nivel":       float(payload["level"]),
        "od":          float(payload.get("OD", 0)),
        "ce":          float(payload.get("EC", 0)),
        "status":      payload.get("status", "ok"),
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


# ── Kincony A64 sequences ─────────────────────────────────────────────────────

def _a64_client() -> A64Client:
    return A64Client(
        host=A64_HOST,
        port=A64_PORT,
        noise_psk=A64_NOISE_KEY,
        expected_name=A64_EXPECTED_NAME,
    )


async def _suction_sequence(balsa: int) -> None:
    ev_out = f"EV_OUT_BAND_{balsa:02d}"
    print(f"[A64] Iniciando succion: {ev_out} + A_MB_Succion ({SUCTION_DURATION_S}s)")
    async with _a64_client() as a64:
        await a64.turn_on(ev_out)
        await a64.turn_on("A_MB_Succion ")
        await asyncio.sleep(SUCTION_DURATION_S)
        await a64.turn_off("A_MB_Succion ")
        await a64.turn_off(ev_out)
    print(f"[A64] Succion finalizada: {ev_out}")


async def _luminaria_sequence(modulo: str, tanque: int, balsa: int, estado: int) -> None:
    entity = f"LUZ_BAND_{balsa:02d}"
    accion = "encendiendo" if estado else "apagando"
    print(f"[A64] {accion.capitalize()} luminaria: {entity}")
    async with _a64_client() as a64:
        if estado:
            await a64.turn_on(entity)
        else:
            await a64.turn_off(entity)
    print(f"[A64] Luminaria {entity} {'encendida' if estado else 'apagada'}")

    try:
        r = requests.post(
            f"{API_BASE_URL}/medicion/luminarias/confirmacion",
            json={"modulo": modulo, "tanque": tanque, "balsa": balsa, "luminaria": estado},
            headers={"X-API-Key": API_KEY},
            timeout=5,
        )
        print(f"  [API] Confirmacion luminaria: {r.status_code} {_safe_response_body(r)}")
    except requests.RequestException as e:
        print(f"  [API] Error confirmacion luminaria: {e}")


async def _return_sequence(balsa: int) -> None:
    ev_in = f"EV_IN_BAND_{balsa:02d}"
    print(f"[A64] Iniciando retorno: A_MB200 + {ev_in} ({RETURN_DURATION_S}s)")
    async with _a64_client() as a64:
        await a64.turn_on("A_MB200")
        await a64.turn_on(ev_in)
        await asyncio.sleep(RETURN_DURATION_S)
        await a64.turn_off(ev_in)
        await a64.turn_off("A_MB200")
    print(f"[A64] Retorno finalizado: {ev_in}")


def _start_medir_sequence(tanque: int, balsa: int, modulo: str) -> None:
    """
    Corre en un thread daemon.
    1. Activa succion en la A64 durante SUCTION_DURATION_S.
    2. Publica el request MQTT para que el ESP32 tome la medicion.
    """
    try:
        asyncio.run(_suction_sequence(balsa))
    except Exception as e:
        print(f"[A64] Error en succion (balsa={balsa}): {e}")
        print("[A64] Abortando secuencia: no se enviara request de medicion.")
        return

    context = {"modulo": modulo, "tanque": tanque, "balsa": balsa}
    with _pending_lock:
        _pending[tanque] = context
    with _return_lock:
        _return_pending[tanque] = {"balsa": balsa}

    payload = {"modulo": modulo, "tanque": tanque, "balsa": balsa}
    result = client.publish(REQUEST_TOPIC, json.dumps(payload), qos=1)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"[MEDIR] MQTT publish fallido (rc={result.rc})")
        with _return_lock:
            _return_pending.pop(tanque, None)
    else:
        print(f"[MEDIR] -> {REQUEST_TOPIC}: {payload}")


def _start_return_sequence(balsa: int) -> None:
    """Corre en un thread daemon. Activa retorno en la A64."""
    try:
        asyncio.run(_return_sequence(balsa))
    except Exception as e:
        print(f"[A64] Error en retorno (balsa={balsa}): {e}")


def _start_luminaria_sequence(modulo: str, tanque: int, balsa: int, estado: int) -> None:
    """Corre en un thread daemon. Cambia estado de luminaria en la A64 y confirma al backend."""
    try:
        asyncio.run(_luminaria_sequence(modulo, tanque, balsa, estado))
    except Exception as e:
        print(f"[A64] Error en luminaria (balsa={balsa}): {e}")


# ── MQTT callbacks ────────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        client.subscribe(MIXING_TANK_TOPIC)
        client.subscribe(DISTRIBUTION_TANK_TOPIC)
        client.subscribe(STATUS_TOPIC)
        print(f"Subscribed to: {MIXING_TANK_TOPIC}")
        print(f"Subscribed to: {DISTRIBUTION_TANK_TOPIC}")
        print(f"Subscribed to: {STATUS_TOPIC}")
        print(f"Will publish measurement requests to: {REQUEST_TOPIC}")
        print()
    else:
        print(f"Connection failed with code {rc}")


def on_message(client, userdata, msg):
    if msg.topic.startswith("status/"):
        estado = msg.payload.decode(errors="replace").strip()
        _node_status[msg.topic] = estado
        print(f"[STATUS] {msg.topic} → {estado}")
        return

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

    # Activar retorno si hay una secuencia pendiente para este tanque
    with _return_lock:
        return_ctx = _return_pending.pop(tank_num, None)

    if return_ctx:
        balsa = return_ctx["balsa"]
        t = threading.Thread(
            target=_start_return_sequence,
            args=(balsa,),
            daemon=True,
            name=f"retorno-b{balsa:02d}",
        )
        t.start()

    print()


# ── MQTT client ───────────────────────────────────────────────────────────────

try:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
except AttributeError:
    client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()


# ── HTTP API: backend → subscriber → ESP32 ────────────────────────────────────
app = Flask(__name__)


@app.route("/medir", methods=["POST"])
def handle_medir():
    """
    Recibe una solicitud de medicion del backend.

    Inicia en background:
      1. Activa succion en A64 (EV_OUT_BAND_XX + A_MB_Succion) durante
         SUCTION_DURATION_S segundos.
      2. Publica request MQTT al nodo ESP32 correspondiente.
      3. Al recibir la medicion, activa retorno (A_MB200 + EV_IN_BAND_XX)
         durante RETURN_DURATION_S segundos.

    JSON esperado: {"modulo": <uuid>, "tanque": <int>, "balsa": <int>}
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "tanque", "balsa") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    tanque = int(data["tanque"])
    balsa  = int(data["balsa"])
    modulo = data["modulo"]

    t = threading.Thread(
        target=_start_medir_sequence,
        args=(tanque, balsa, modulo),
        daemon=True,
        name=f"medir-t{tanque}-b{balsa:02d}",
    )
    t.start()

    print(f"[MEDIR] Secuencia iniciada: tanque={tanque} balsa={balsa}")
    return jsonify({
        "status": "queued",
        "tanque": tanque,
        "balsa": balsa,
        "suction_duration_s": SUCTION_DURATION_S,
    }), 202


@app.route("/luminarias", methods=["POST"])
def handle_luminarias():
    """
    Recibe comando de cambio de estado de luminaria del backend.

    Responde 200 de inmediato y en background:
      1. Activa/desactiva el relé LUM_BAND_XX en la A64.
      2. Confirma el estado real al backend via POST /medicion/luminarias/confirmacion.

    JSON esperado: {"modulo": <uuid>, "tanque": <int>, "balsa": <int>, "estado": 0|1}
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "tanque", "balsa", "estado") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    modulo = data["modulo"]
    tanque = int(data["tanque"])
    balsa  = int(data["balsa"])
    estado = int(data["estado"])

    if estado not in (0, 1):
        return jsonify({"error": "estado must be 0 or 1"}), 400

    t = threading.Thread(
        target=_start_luminaria_sequence,
        args=(modulo, tanque, balsa, estado),
        daemon=True,
        name=f"luminaria-b{balsa:02d}",
    )
    t.start()

    print(f"[LUMINARIA] Comando recibido: balsa={balsa} estado={estado}")
    return jsonify({"status": "ok"}), 200


@app.route("/status", methods=["GET"])
def node_status():
    return jsonify(_node_status), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "broker": BROKER, "port": PORT}), 200


if __name__ == "__main__":
    try:
        app.run(host=HTTP_HOST, port=HTTP_PORT, debug=False)
    finally:
        client.loop_stop()
        client.disconnect()
