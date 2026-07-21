import asyncio
import json
import os
import threading
import requests
from dotenv import load_dotenv
from flask import Flask, request as flask_request, jsonify
import paho.mqtt.client as mqtt

from a64_client import A64Client
from tank_mix_client import TankMixClient

load_dotenv()

BROKER       = os.getenv("BROKER", "localhost")
PORT         = int(os.getenv("PORT", 1883))
HTTP_HOST    = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT    = int(os.getenv("HTTP_PORT", 8001))
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_KEY      = os.getenv("API_KEY", "")

# ── Kincony a64_prod ──────────────────────────────────────────────────────────
A64_HOST          = os.getenv("A64_HOST", "192.168.0.103")
A64_PORT          = int(os.getenv("A64_PORT", "6053"))
A64_NOISE_KEY     = os.getenv("A64_NOISE_KEY", "Dw3Z3r2KbL05KstmqaTSWpxvY/6A4WoRcOUKgq6W99Y=")
A64_EXPECTED_NAME = os.getenv("A64_EXPECTED_NAME", "produccion")

# ── Kincony tank-mix ──────────────────────────────────────────────────────────
TANKMIX_HOST          = os.getenv("TANKMIX_HOST", "192.168.0.102")
TANKMIX_PORT          = int(os.getenv("TANKMIX_PORT", "6053"))
TANKMIX_NOISE_KEY     = os.getenv("TANKMIX_NOISE_KEY", "ojkGTpvfiDJwONH69xSDFDImSyLZZfT6IuCcNwL28gU=")
TANKMIX_EXPECTED_NAME = os.getenv("TANKMIX_EXPECTED_NAME", "tank-mix")

# ── Tiempos configurables (segundos) ─────────────────────────────────────────
T_SUCCION = int(os.getenv("T_SUCCION", str(10 * 60)))
T_MB200   = int(os.getenv("T_MB200",   str(10 * 60)))
T_MBMIX   = int(os.getenv("T_MBMIX",   str(10 * 60)))

MIXING_TANK_TOPIC       = "mixing_tank"
DISTRIBUTION_TANK_TOPIC = "distribution_tank/+"
REQUEST_TOPIC = "request"
STATUS_TOPIC  = "status/+"

# Solicitudes pendientes de respuesta ESP32: tanque (int) -> {modulo, tanque, balsa}
_pending: dict = {}
_pending_lock = threading.Lock()

# Mediciones activas: tanque (int) — previene doble disparo de /medir
_active_sequences: set = set()
_active_lock = threading.Lock()

# Estado de conexión de nodos: "status/tank_N" -> "online" | "offline"
_node_status: dict = {}


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


# ── Kincony client factories ──────────────────────────────────────────────────

def _a64_client() -> A64Client:
    return A64Client(
        host=A64_HOST,
        port=A64_PORT,
        noise_psk=A64_NOISE_KEY,
        expected_name=A64_EXPECTED_NAME,
    )


def _tankmix_client() -> TankMixClient:
    return TankMixClient(
        host=TANKMIX_HOST,
        port=TANKMIX_PORT,
        noise_psk=TANKMIX_NOISE_KEY,
        expected_name=TANKMIX_EXPECTED_NAME,
    )


# ── Secuencias async ──────────────────────────────────────────────────────────

async def _suction_sequence(balsa: int) -> None:
    ev_out = f"EV_OUT_BAND_{balsa:02d}"
    print(f"[A64] Iniciando succion: {ev_out} + A_MB_Succion ({T_SUCCION}s)")
    async with _a64_client() as a64:
        await a64.turn_on(ev_out)
        await a64.turn_on("A_MB_Succion ")
        await asyncio.sleep(T_SUCCION)
        await a64.turn_off("A_MB_Succion ")
        await a64.turn_off(ev_out)
    print(f"[A64] Succion finalizada: {ev_out}")


async def _transferir_dist_mix_sequence() -> None:
    print(f"[A64] Iniciando transferencia dist→mix: A_MB200 + A_EV_OUT_NUTRICION ({T_MB200}s)")
    async with _a64_client() as a64:
        await a64.turn_on("A_MB200")
        await a64.turn_on("A_EV_OUT_NUTRICION")
        await asyncio.sleep(T_MB200)
        await a64.turn_off("A_EV_OUT_NUTRICION")
        await a64.turn_off("A_MB200")
    print("[A64] Transferencia dist→mix finalizada")


async def _transferir_mix_dist_sequence() -> None:
    print(f"[TANKMIX] Iniciando transferencia mix→dist: A_EV_IN_NUTRICION + MOTOBOMBA ({T_MBMIX}s)")
    async with _a64_client() as a64, _tankmix_client() as tmix:
        await asyncio.gather(
            a64.turn_on("A_EV_IN_NUTRICION"),
            tmix.turn_on("Motobomba"),
        )
        await asyncio.sleep(T_MBMIX)
        await asyncio.gather(
            a64.turn_off("A_EV_IN_NUTRICION"),
            tmix.turn_off("Motobomba"),
        )
    print("[TANKMIX] Transferencia mix→dist finalizada")


async def _llenar_sequence(balsa: int) -> None:
    ev_in = f"EV_IN_BAND_{balsa:02d}"
    print(f"[A64] Iniciando llenado: {ev_in} + A_MB200 ({T_MB200}s)")
    async with _a64_client() as a64:
        await a64.turn_on(ev_in)
        await a64.turn_on("A_MB200")
        await asyncio.sleep(T_MB200)
        await a64.turn_off("A_MB200")
        await a64.turn_off(ev_in)
    print(f"[A64] Llenado finalizado: {ev_in}")


async def _luminaria_sequence(modulo: str, tanque: int, balsa: int, estado: int) -> None:
    entity = f"LUZ_BAND_{balsa:02d}"
    print(f"[A64] {'Encendiendo' if estado else 'Apagando'} luminaria: {entity}")
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


async def _bombas_sequence(estado: bool) -> None:
    print(f"[TANKMIX] {'Encendiendo' if estado else 'Apagando'} Agitador")
    async with _tankmix_client() as tmix:
        if estado:
            await tmix.turn_on("Agitador")
        else:
            await tmix.turn_off("Agitador")
    print(f"[TANKMIX] Agitador {'encendido' if estado else 'apagado'}")


# ── Thread starters ───────────────────────────────────────────────────────────

def _start_medir_sequence(tanque: int, balsa: int, modulo: str) -> None:
    """
    Corre en un thread daemon.
    tanque=1: activa succion (EV_OUT_BAND_XX + A_MB_Succion) durante T_SUCCION,
              luego publica request MQTT para que el ESP32 tome la medicion.
    tanque=2: publica request MQTT directamente, sin actuacion previa.
    Al recibir la medicion del ESP32, on_message llama a post_medicion y libera
    _active_sequences.
    """
    try:
        if tanque == 1:
            try:
                asyncio.run(_suction_sequence(balsa))
            except Exception as e:
                print(f"[A64] Error en succion (balsa={balsa}): {e}")
                print("[A64] Abortando secuencia: no se enviara request de medicion.")
                with _active_lock:
                    _active_sequences.discard(tanque)
                return

        context = {"modulo": modulo, "tanque": tanque, "balsa": balsa}
        with _pending_lock:
            _pending[tanque] = context

        payload = {"modulo": modulo, "tanque": tanque, "balsa": balsa}
        result = client.publish(REQUEST_TOPIC, json.dumps(payload), qos=1)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[MEDIR] MQTT publish fallido (rc={result.rc})")
            with _pending_lock:
                _pending.pop(tanque, None)
            with _active_lock:
                _active_sequences.discard(tanque)
        else:
            print(f"[MEDIR] -> {REQUEST_TOPIC}: {payload}")
    except Exception as e:
        print(f"[MEDIR] Error inesperado en secuencia (tanque={tanque}): {e}")
        with _active_lock:
            _active_sequences.discard(tanque)


def _start_transferir_sequence(tanque: int) -> None:
    try:
        if tanque == 1:
            asyncio.run(_transferir_dist_mix_sequence())
        else:
            asyncio.run(_transferir_mix_dist_sequence())
    except Exception as e:
        print(f"[TRANSFERIR] Error en transferencia (tanque={tanque}): {e}")


def _start_llenar_sequence(balsa: int) -> None:
    try:
        asyncio.run(_llenar_sequence(balsa))
    except Exception as e:
        print(f"[LLENAR] Error en llenado (balsa={balsa}): {e}")


def _start_luminaria_sequence(modulo: str, tanque: int, balsa: int, estado: int) -> None:
    try:
        asyncio.run(_luminaria_sequence(modulo, tanque, balsa, estado))
    except Exception as e:
        print(f"[A64] Error en luminaria (balsa={balsa}): {e}")


def _start_bombas_sequence(estado: bool) -> None:
    try:
        asyncio.run(_bombas_sequence(estado))
    except Exception as e:
        print(f"[TANKMIX] Error en agitador: {e}")


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

    with _active_lock:
        _active_sequences.discard(tank_num)

    print(f"  [-> API] Enviando medicion tanque={tank_num}")
    post_medicion(payload, context)
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


# ── HTTP API ──────────────────────────────────────────────────────────────────
app = Flask(__name__)


@app.route("/medir", methods=["POST"])
def handle_medir():
    """
    Inicia una medicion de sensores en el tanque indicado.

    JSON esperado: {"modulo": <uuid>, "tanque": 1|2, "balsa": <int>}

    tanque=1: activa succion (EV_OUT_BAND_XX + A_MB_Succion) durante T_SUCCION s,
              luego publica request MQTT al nodo ESP32.
    tanque=2: publica request MQTT directamente, sin actuacion previa.

    Responde 202 inmediato. Si el tanque ya tiene una medicion activa, responde 409.
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

    if tanque not in (1, 2):
        return jsonify({"error": "tanque must be 1 or 2"}), 422

    with _active_lock:
        if tanque in _active_sequences:
            return jsonify({"error": f"Secuencia ya activa para tanque={tanque}"}), 409
        _active_sequences.add(tanque)

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
        "suction_duration_s": T_SUCCION if tanque == 1 else 0,
    }), 202


@app.route("/transferir", methods=["POST"])
def handle_transferir():
    """
    Transfiere agua entre tanques.

    JSON esperado: {"modulo": <uuid>, "tanque": 1|2}

    tanque=1 (dist→mix): activa A_MB200 + A_EV_OUT_NUTRICION durante T_MB200 s.
    tanque=2 (mix→dist): activa A_EV_IN_NUTRICION + MOTOBOMBA durante T_MBMIX s.

    Responde 202 inmediato (fire-and-forget).
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "tanque") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    tanque = int(data["tanque"])

    if tanque not in (1, 2):
        return jsonify({"error": "tanque must be 1 or 2"}), 422

    t = threading.Thread(
        target=_start_transferir_sequence,
        args=(tanque,),
        daemon=True,
        name=f"transferir-t{tanque}",
    )
    t.start()

    duracion   = T_MB200 if tanque == 1 else T_MBMIX
    direccion  = "dist→mix" if tanque == 1 else "mix→dist"
    print(f"[TRANSFERIR] Iniciando: {direccion} ({duracion}s)")
    return jsonify({
        "status": "queued",
        "tanque": tanque,
        "duracion_s": duracion,
    }), 202


@app.route("/llenar", methods=["POST"])
def handle_llenar():
    """
    Llena la balsa indicada desde el tanque de distribución.

    JSON esperado: {"modulo": <uuid>, "balsa": <int>}

    Activa EV_IN_BAND_XX + A_MB200 durante T_MB200 s.
    Responde 202 inmediato (fire-and-forget).
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "balsa") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    balsa = int(data["balsa"])

    if balsa < 1:
        return jsonify({"error": "balsa must be >= 1"}), 422

    t = threading.Thread(
        target=_start_llenar_sequence,
        args=(balsa,),
        daemon=True,
        name=f"llenar-b{balsa:02d}",
    )
    t.start()

    print(f"[LLENAR] Iniciando llenado: balsa={balsa} ({T_MB200}s)")
    return jsonify({
        "status": "queued",
        "balsa": balsa,
        "duracion_s": T_MB200,
    }), 202


@app.route("/luminarias", methods=["POST"])
def handle_luminarias():
    """
    Cambia el estado de la luminaria de una balsa.

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


@app.route("/bombas", methods=["POST"])
def handle_bombas():
    """
    Enciende o apaga el agitador del tanque mix (placa tank-mix).

    JSON esperado: {"modulo": <uuid>, "estado": true|false}
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "estado") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    estado = bool(data["estado"])

    t = threading.Thread(
        target=_start_bombas_sequence,
        args=(estado,),
        daemon=True,
        name=f"bombas-{'on' if estado else 'off'}",
    )
    t.start()

    print(f"[BOMBAS] Comando recibido: estado={estado}")
    return jsonify({"status": "ok", "estado_aplicado": estado}), 200


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
