import asyncio
import json
import os
import time
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
T_SUCCION      = int(os.getenv("T_SUCCION",      str(10 * 60)))
T_MB200        = int(os.getenv("T_MB200",        str(10 * 60)))
T_MBMIX        = int(os.getenv("T_MBMIX",        str(10 * 60)))
T_FILL_POLL_S  = int(os.getenv("T_FILL_POLL_S",  30))   # intervalo entre sondeos de nivel (s)
T_FILL_MAX_S   = int(os.getenv("T_FILL_MAX_S",   3600)) # timeout de seguridad máximo (s)
T_OZ           = int(os.getenv("T_OZ",           300))
T_DOSIFICACION = int(os.getenv("T_DOSIFICACION",  51))

MIXING_TANK_TOPIC       = "mixing_tank"
DISTRIBUTION_TANK_TOPIC = "distribution_tank/+"
REQUEST_TOPIC = "request"
STATUS_TOPIC  = "status/+"

# Solicitudes pendientes de respuesta ESP32: tanque (int) -> {modulo, tanque, balsa}
_pending: dict = {}
_pending_lock = threading.Lock()

# Mediciones activas: tanque (int) — previene doble disparo de /medir
_active_sequences: set = set()
_active_sequence_times: dict = {}          # tanque -> time.time() al añadir
_active_lock = threading.Lock()
ACTIVE_SEQUENCE_TIMEOUT_S = 15 * 60       # 15 min: margen sobre T_SUCCION (10 min)

# Reintentos de publicación MQTT en _start_medir_sequence
_PUBLISH_RETRY_MAX     = 3
_PUBLISH_RETRY_DELAY_S = 5

# Watchdog: re-publica requests sin respuesta del ESP32
WATCHDOG_INTERVAL_S    = 60   # frecuencia de escaneo (s)
WATCHDOG_RETRY_AFTER_S = 90   # tiempo sin respuesta antes de re-publicar (s)
WATCHDOG_MAX_RETRIES   = 3    # máximo de re-publicaciones por request

# Estado de conexión de nodos: "status/tank_N" -> "online" | "offline"
_node_status: dict = {}

# Último nivel conocido del tanque mix — actualizado en on_message
_last_mix_level: float = 0.0
_last_mix_level_lock = threading.Lock()
# Señal que se activa cada vez que llega un mensaje de mixing_tank
_mix_level_event = threading.Event()
# Controla el loop activo de llenado; limpiar para abortar
_fill_active = threading.Event()

# Señal de conexión MQTT — se activa en on_connect, se limpia en on_disconnect
_mqtt_connected = threading.Event()


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


async def _agitador_sequence(tanque: int, estado: bool) -> None:
    if tanque == 1:
        print(f"[A64] {'Encendiendo' if estado else 'Apagando'} agitador dist: A_AG200")
        async with _a64_client() as a64:
            if estado:
                await a64.turn_on("A_AG200")
            else:
                await a64.turn_off("A_AG200")
        print(f"[AGITADOR] A_AG200 (a64_prod) {'encendido' if estado else 'apagado'}")
    else:
        print(f"[TANKMIX] {'Encendiendo' if estado else 'Apagando'} agitador mix: Agitador")
        async with _tankmix_client() as tmix:
            if estado:
                await tmix.turn_on("Agitador")
            else:
                await tmix.turn_off("Agitador")
        print(f"[AGITADOR] Agitador (tank-mix) {'encendido' if estado else 'apagado'}")


async def _open_epm() -> None:
    async with _tankmix_client() as tmix:
        await tmix.turn_on("Entrada EPM")
    print("[TANKMIX] Entrada EPM abierta")


async def _close_epm() -> None:
    async with _tankmix_client() as tmix:
        await tmix.turn_off("Entrada EPM")
    print("[TANKMIX] Entrada EPM cerrada")


async def _ozono_sequence(estado: bool) -> None:
    print(f"[TANKMIX] {'Activando' if estado else 'Desactivando'} Ozono")
    async with _tankmix_client() as tmix:
        if estado:
            await tmix.turn_on("Ozono")
            await asyncio.sleep(T_OZ)
            await tmix.turn_off("Ozono")
        else:
            await tmix.turn_off("Ozono")
    print(f"[TANKMIX] Ozono {'completado' if estado else 'apagado'}")


async def _peris_sequence(peris: int, estado: bool) -> None:
    name = f"Peris_{peris}"
    print(f"[TANKMIX] {'Activando' if estado else 'Desactivando'} {name}")
    async with _tankmix_client() as tmix:
        if estado:
            await tmix.turn_on(name)
            await asyncio.sleep(T_DOSIFICACION)
            await tmix.turn_off(name)
        else:
            await tmix.turn_off(name)
    print(f"[TANKMIX] {name} {'completado' if estado else 'apagado'}")


# ── Thread starters ───────────────────────────────────────────────────────────

def _start_medir_sequence(tanque: int, balsa: int, modulo: str) -> None:
    """
    Corre en un thread daemon.
    tanque=1: activa succion (EV_OUT_BAND_XX + A_MB_Succion) durante T_SUCCION,
              luego publica request MQTT para que el ESP32 tome la medicion.
    tanque=2: publica request MQTT directamente, sin actuacion previa.
    Al recibir la medicion del ESP32, on_message llama a post_medicion y libera
    _active_sequences.

    Si el PUBACK falla (broker inalcanzable), reintenta _PUBLISH_RETRY_MAX veces
    con _PUBLISH_RETRY_DELAY_S entre intentos.  El watchdog _pending_watchdog
    cubre el caso donde el PUBACK llega pero el ESP32 no recibe el mensaje
    (sesion limpia + ESP32 desconectado en el momento del publish).
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
                    _active_sequence_times.pop(tanque, None)
                return

        payload  = {"modulo": modulo, "tanque": tanque, "balsa": balsa}
        published = False

        for attempt in range(1, _PUBLISH_RETRY_MAX + 1):
            if not _mqtt_connected.wait(timeout=10):
                print(f"[MEDIR] Sin MQTT (intento {attempt}/{_PUBLISH_RETRY_MAX}, tanque={tanque})")
                if attempt < _PUBLISH_RETRY_MAX:
                    time.sleep(_PUBLISH_RETRY_DELAY_S)
                    continue
                with _active_lock:
                    _active_sequences.discard(tanque)
                    _active_sequence_times.pop(tanque, None)
                return

            result = client.publish(REQUEST_TOPIC, json.dumps(payload), qos=1)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"[MEDIR] Publish fallido rc={result.rc} (intento {attempt}/{_PUBLISH_RETRY_MAX})")
                if attempt < _PUBLISH_RETRY_MAX:
                    time.sleep(_PUBLISH_RETRY_DELAY_S)
                    continue
                with _active_lock:
                    _active_sequences.discard(tanque)
                    _active_sequence_times.pop(tanque, None)
                return

            try:
                result.wait_for_publish(timeout=10)
            except Exception:
                pass

            if result.is_published():
                context = {
                    "modulo": modulo, "tanque": tanque, "balsa": balsa,
                    "_published_at": time.time(), "_retry_count": 0,
                }
                with _pending_lock:
                    _pending[tanque] = context
                print(f"[MEDIR] -> {REQUEST_TOPIC}: {payload} (intento {attempt})")
                published = True
                break
            else:
                print(f"[MEDIR] Sin PUBACK en 10s (intento {attempt}/{_PUBLISH_RETRY_MAX}, tanque={tanque})")
                if attempt < _PUBLISH_RETRY_MAX:
                    time.sleep(_PUBLISH_RETRY_DELAY_S)

        if not published:
            print(f"[MEDIR] Todos los intentos fallidos. Abortando tanque={tanque}.")
            with _active_lock:
                _active_sequences.discard(tanque)
                _active_sequence_times.pop(tanque, None)

    except Exception as e:
        print(f"[MEDIR] Error inesperado (tanque={tanque}): {e}")
        with _pending_lock:
            _pending.pop(tanque, None)
        with _active_lock:
            _active_sequences.discard(tanque)
            _active_sequence_times.pop(tanque, None)


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


def _start_agitador_sequence(tanque: int, estado: bool) -> None:
    try:
        asyncio.run(_agitador_sequence(tanque, estado))
    except Exception as e:
        print(f"[AGITADOR] Error en agitador (tanque={tanque}): {e}")


def _start_entrada_agua_sequence(modulo: str, volumen: int, estado: bool) -> None:
    """
    Corre en un thread daemon.
    estado=False: cierra Entrada EPM inmediatamente y aborta cualquier llenado activo.
    estado=True:  abre Entrada EPM y monitorea el nivel del tanque mix via MQTT.
                  Cierra la válvula cuando nivel >= volumen o al cumplirse T_FILL_MAX_S.
    """
    if not estado:
        _fill_active.clear()
        try:
            asyncio.run(_close_epm())
        except Exception as e:
            print(f"[TANKMIX] Error cerrando Entrada EPM: {e}")
        return

    # _fill_active ya fue reservado sincrónicamente por el handler (cierra el
    # hueco con /medir t2). Si falla la apertura de la válvula, lo liberamos.
    try:
        asyncio.run(_open_epm())
    except Exception as e:
        print(f"[TANKMIX] Error abriendo Entrada EPM: {e}")
        _fill_active.clear()
        return

    print(f"[ENTRADA_AGUA] Monitoreo iniciado. Objetivo: {volumen} L")

    t_inicio = time.time()
    alcanzado = False

    while _fill_active.is_set() and (time.time() - t_inicio) < T_FILL_MAX_S:
        # Esperar conexión MQTT antes de sondear
        if not _mqtt_connected.wait(timeout=30):
            print("[ENTRADA_AGUA] Sin conexión MQTT. Reintentando en 30 s...")
            time.sleep(T_FILL_POLL_S)
            continue

        # Solicitar medición al ESP32 del tanque mix
        _mix_level_event.clear()
        req = {"modulo": modulo, "tanque": 2, "balsa": 0}
        result = client.publish(REQUEST_TOPIC, json.dumps(req), qos=1)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[ENTRADA_AGUA] MQTT publish fallido (rc={result.rc}). Reintentando en {T_FILL_POLL_S}s")
            time.sleep(T_FILL_POLL_S)
            continue
        try:
            result.wait_for_publish(timeout=10)
        except Exception:
            pass
        if not result.is_published():
            print(f"[ENTRADA_AGUA] Broker no confirmó entrega. Reintentando en {T_FILL_POLL_S}s")
            time.sleep(T_FILL_POLL_S)
            continue

        # Esperar respuesta del ESP32 (máx 60 s)
        got = _mix_level_event.wait(timeout=60)
        if not got:
            print("[ENTRADA_AGUA] Sin respuesta del ESP32 en 60 s. Reintentando...")
            continue

        with _last_mix_level_lock:
            nivel_actual = _last_mix_level

        elapsed = int(time.time() - t_inicio)
        print(f"[ENTRADA_AGUA] Nivel: {nivel_actual:.2f} L / objetivo: {volumen} L (t={elapsed}s)")

        if nivel_actual >= volumen:
            alcanzado = True
            _fill_active.clear()
            break

        # Esperar el intervalo configurado antes del próximo sondeo
        time.sleep(T_FILL_POLL_S)

    motivo = "Volumen alcanzado" if alcanzado else \
             "Detenido externamente" if not _fill_active.is_set() else \
             f"Timeout de seguridad ({T_FILL_MAX_S}s)"
    print(f"[ENTRADA_AGUA] {motivo}. Cerrando Entrada EPM.")
    _fill_active.clear()

    try:
        asyncio.run(_close_epm())
    except Exception as e:
        print(f"[TANKMIX] Error cerrando Entrada EPM: {e}")


def _start_ozono_sequence(estado: bool) -> None:
    try:
        asyncio.run(_ozono_sequence(estado))
    except Exception as e:
        print(f"[TANKMIX] Error en ozono: {e}")


def _start_peris_sequence(peris: int, estado: bool) -> None:
    try:
        asyncio.run(_peris_sequence(peris, estado))
    except Exception as e:
        print(f"[TANKMIX] Error en peristáltica Peris_{peris}: {e}")


# ── MQTT helpers ─────────────────────────────────────────────────────────────

def _republish_pending(t_num: int) -> None:
    """
    Re-publica el request MQTT para el tanque dado si hay una medicion pendiente.
    Llamado desde on_connect (reconexion del subscriber) y on_message cuando
    el ESP32 vuelve a conectarse (status = "online").

    El PUBACK confirma entrega Pi→broker, pero NO broker→ESP32.  Si el ESP32
    tenia sesion limpia y estaba offline cuando se publico el request original,
    el broker descarto el mensaje.  Esta funcion recupera ese caso.
    """
    with _pending_lock:
        ctx = _pending.get(t_num)
    if ctx is None:
        return
    if not _mqtt_connected.is_set():
        return
    req = {"modulo": ctx["modulo"], "tanque": t_num, "balsa": ctx["balsa"]}
    r = client.publish(REQUEST_TOPIC, json.dumps(req), qos=1)
    if r.rc == mqtt.MQTT_ERR_SUCCESS:
        with _pending_lock:
            if t_num in _pending:
                _pending[t_num]["_published_at"] = time.time()
                _pending[t_num]["_retry_count"] = _pending[t_num].get("_retry_count", 0) + 1
        print(f"[MQTT] Re-publicado request tanque={t_num}")
    else:
        print(f"[MQTT] Re-publicacion fallida tanque={t_num} rc={r.rc}")


def _pending_watchdog() -> None:
    """
    Hilo daemon. Cada WATCHDOG_INTERVAL_S escanea _pending en busca de requests
    sin respuesta del ESP32.  Si un entry lleva > WATCHDOG_RETRY_AFTER_S sin
    respuesta, re-publica el request via _republish_pending.
    Tras WATCHDOG_MAX_RETRIES re-publicaciones sin exito, limpia el entry y
    libera _active_sequences para desbloquear la siguiente llamada a /medir.
    """
    while True:
        time.sleep(WATCHDOG_INTERVAL_S)
        now = time.time()
        # Snapshot: (published_at, retry_count) por tanque.  published_at es la
        # marca única que identifica ESTA petición; se usa después para verificar
        # que la entrada no fue reemplazada por un /medir nuevo antes de actuar.
        with _pending_lock:
            stale = {
                t: (ctx.get("_published_at", now), ctx.get("_retry_count", 0))
                for t, ctx in _pending.items()
                if (now - ctx.get("_published_at", now)) > WATCHDOG_RETRY_AFTER_S
            }

        for t_num, (pub_at, retry_count) in stale.items():
            if retry_count >= WATCHDOG_MAX_RETRIES:
                # Limpieza atómica: solo si la entrada sigue siendo la misma.
                # Evita borrar una petición nueva que entró mientras decidíamos.
                removed = False
                with _pending_lock:
                    cur = _pending.get(t_num)
                    if cur is not None and cur.get("_published_at") == pub_at:
                        _pending.pop(t_num, None)
                        removed = True
                # Solo liberar _active_sequences si nosotros removimos la entrada.
                # Un /medir nuevo se bloquea con 409 mientras _active siga puesto,
                # así que no puede colarse otra petición en este hueco.
                if removed:
                    with _active_lock:
                        _active_sequences.discard(t_num)
                        _active_sequence_times.pop(t_num, None)
                    print(
                        f"[WATCHDOG] Tanque={t_num} sin respuesta tras "
                        f"{WATCHDOG_MAX_RETRIES} reintentos. Limpiado."
                    )
                continue

            if not _mqtt_connected.is_set():
                print(f"[WATCHDOG] Sin MQTT. Postergando re-publicacion tanque={t_num}.")
                continue

            # Re-publicar solo si la entrada no cambió desde el snapshot.
            with _pending_lock:
                cur = _pending.get(t_num)
                still_same = cur is not None and cur.get("_published_at") == pub_at
            if not still_same:
                continue

            print(
                f"[WATCHDOG] Tanque={t_num} sin respuesta tras "
                f"{now - pub_at:.0f}s. "
                f"Re-publicando (reintento {retry_count + 1}/{WATCHDOG_MAX_RETRIES})."
            )
            _republish_pending(t_num)


# ── MQTT callbacks ────────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        _mqtt_connected.set()
        print("Connected to broker")
        client.subscribe(MIXING_TANK_TOPIC)
        client.subscribe(DISTRIBUTION_TANK_TOPIC)
        client.subscribe(STATUS_TOPIC)
        print(f"Subscribed to: {MIXING_TANK_TOPIC}")
        print(f"Subscribed to: {DISTRIBUTION_TANK_TOPIC}")
        print(f"Subscribed to: {STATUS_TOPIC}")
        print(f"Will publish measurement requests to: {REQUEST_TOPIC}")
        # Re-publicar requests pendientes: si el subscriber se reconecto al broker,
        # los mensajes anteriores pueden haberse perdido (broker reiniciado, etc.)
        with _pending_lock:
            pending_snapshot = list(_pending.keys())
        if pending_snapshot:
            print(f"[MQTT] Reconexion: re-publicando {len(pending_snapshot)} request(s) pendiente(s)")
            for t_num in pending_snapshot:
                _republish_pending(t_num)
        print()
    else:
        _mqtt_connected.clear()
        print(f"Connection failed with code {rc}")


def on_disconnect(client, userdata, rc):
    _mqtt_connected.clear()
    if rc == 0:
        print("[MQTT] Desconectado del broker correctamente.")
    else:
        print(f"[MQTT] Desconexión inesperada del broker (rc={rc}). Reconectando...")


def on_message(client, userdata, msg):
    if msg.topic.startswith("status/"):
        estado = msg.payload.decode(errors="replace").strip()
        _node_status[msg.topic] = estado
        print(f"[STATUS] {msg.topic} → {estado}")
        if estado == "online":
            # El ESP32 acaba de reconectarse.  Si habia un request pendiente para
            # este tanque, el broker ya lo descarto (sesion limpia).  Re-publicar.
            parts = msg.topic.split("/")
            if len(parts) >= 2:
                tank_str = parts[1]
                try:
                    t_num = int(tank_str[5:]) if tank_str.startswith("tank_") else int(tank_str)
                    _republish_pending(t_num)
                except (ValueError, IndexError):
                    pass
        return

    try:
        payload = json.loads(msg.payload.decode())
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"[{msg.topic}] Failed to parse payload: {e}")
        return

    print(f"[MQTT] <- {msg.topic}")

    if msg.topic == MIXING_TANK_TOPIC:
        tank_num = int(payload.get("tank", 0))
        with _last_mix_level_lock:
            _last_mix_level = float(payload.get("level", 0.0))
        _mix_level_event.set()
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
        # Solo liberar si había una medición activa (no para lecturas del loop de llenado)
        if context is not None:
            _active_sequences.discard(tank_num)
            _active_sequence_times.pop(tank_num, None)

    if context is not None:
        print(f"  [-> API] Enviando medicion tanque={tank_num}")
        post_medicion(payload, context)
        print()
    else:
        print(f"  [MQTT] Nivel registrado (sondeo de llenado, sin medicion pendiente)")


# ── MQTT client ───────────────────────────────────────────────────────────────

try:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
except AttributeError:
    client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.reconnect_delay_set(min_delay=2, max_delay=30)

client.connect(BROKER, PORT)
client.loop_start()

_watchdog_thread = threading.Thread(
    target=_pending_watchdog, daemon=True, name="pending-watchdog"
)
_watchdog_thread.start()


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
        # _fill_active se comprueba dentro del lock para excluir mutuamente con
        # /entrada_agua: ambos publican requests t2 y solaparlos corrompería la
        # atribución de la lectura. La reserva de /entrada_agua también ocurre
        # bajo _active_lock, así que ambos handlers se serializan aquí.
        if tanque == 2 and _fill_active.is_set():
            return jsonify({"error": "Llenado activo en tanque mix. Detén /entrada_agua primero."}), 409
        if tanque in _active_sequences:
            added_at = _active_sequence_times.get(tanque, 0)
            if (time.time() - added_at) < ACTIVE_SEQUENCE_TIMEOUT_S:
                return jsonify({"error": f"Secuencia ya activa para tanque={tanque}"}), 409
            print(f"[MEDIR] Auto-limpiando secuencia bloqueada para tanque={tanque} (sin respuesta tras {ACTIVE_SEQUENCE_TIMEOUT_S}s)")
            _active_sequences.discard(tanque)
            _active_sequence_times.pop(tanque, None)
        _active_sequences.add(tanque)
        _active_sequence_times[tanque] = time.time()

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


@app.route("/agitador", methods=["POST"])
def handle_agitador():
    """
    Enciende o apaga el agitador del tanque indicado.

    JSON esperado: {"modulo": <uuid>, "tanque": 1|2, "estado": true|false}

    tanque=1: A_AG200 en placa a64_prod (tanque de distribución).
    tanque=2: Agitador en placa tank-mix.
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "tanque", "estado") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    tanque = int(data["tanque"])
    estado = bool(data["estado"])

    if tanque not in (1, 2):
        return jsonify({"error": "tanque must be 1 or 2"}), 422

    t = threading.Thread(
        target=_start_agitador_sequence,
        args=(tanque, estado),
        daemon=True,
        name=f"agitador-t{tanque}-{'on' if estado else 'off'}",
    )
    t.start()

    print(f"[AGITADOR] Comando recibido: tanque={tanque} estado={estado}")
    return jsonify({"status": "ok", "tanque": tanque, "estado_aplicado": estado}), 200


@app.route("/bombas", methods=["POST"])
def handle_bombas():
    """
    Alias de /agitador para tanque=2 (backward-compat con el broker). Prefiere /agitador.

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
        target=_start_agitador_sequence,
        args=(2, estado),
        daemon=True,
        name=f"bombas-{'on' if estado else 'off'}",
    )
    t.start()

    print(f"[BOMBAS] Comando recibido: estado={estado}")
    return jsonify({"status": "ok", "estado_aplicado": estado}), 200


@app.route("/entrada_agua", methods=["POST"])
def handle_entrada_agua():
    """
    Controla la entrada de agua de red al tanque mix (Entrada EPM).

    JSON esperado: {"modulo": <uuid>, "volumen": <int>, "estado": true|false}

    estado=true:  abre Entrada EPM y monitorea el nivel del tanque mix vía MQTT.
                  Cierra la válvula automáticamente cuando nivel >= volumen (L).
                  Timeout de seguridad: T_FILL_MAX_S segundos.
    estado=false: cierra Entrada EPM inmediatamente y aborta el llenado activo.
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "volumen", "estado") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    modulo = str(data["modulo"])
    volumen = int(data["volumen"])
    estado  = bool(data["estado"])

    if estado and volumen <= 0:
        return jsonify({"error": "volumen must be > 0"}), 422

    if estado:
        # Reserva atómica del recurso "tanque mix" bajo _active_lock: rechaza si
        # ya hay un llenado activo o una medición /medir en curso. _fill_active se
        # marca aquí (sincrónicamente), no en el thread, para que /medir t2 lo vea
        # de inmediato y no se cuele en el hueco de arranque.
        with _active_lock:
            if _fill_active.is_set():
                return jsonify({"error": "Ya hay un llenado activo. Envía estado=false para detenerlo primero."}), 409
            if 2 in _active_sequences:
                added_at = _active_sequence_times.get(2, 0)
                if (time.time() - added_at) < ACTIVE_SEQUENCE_TIMEOUT_S:
                    return jsonify({"error": "Medición activa en tanque mix. Espera a que termine o usa /abortar."}), 409
                print("[ENTRADA_AGUA] Auto-limpiando /medir bloqueado para tanque=2")
                _active_sequences.discard(2)
                _active_sequence_times.pop(2, None)
            _fill_active.set()

    t = threading.Thread(
        target=_start_entrada_agua_sequence,
        args=(modulo, volumen, estado),
        daemon=True,
        name=f"entrada-agua-{'on' if estado else 'off'}",
    )
    t.start()

    print(f"[ENTRADA_AGUA] Comando recibido: estado={estado} volumen={volumen} L")
    if estado:
        return jsonify({
            "status": "queued",
            "volumen_objetivo_L": volumen,
            "poll_interval_s": T_FILL_POLL_S,
            "timeout_max_s": T_FILL_MAX_S,
        }), 202
    return jsonify({"status": "ok", "accion": "Entrada EPM cerrada"}), 200


@app.route("/ozono", methods=["POST"])
def handle_ozono():
    """
    Activa el generador de ozono del tanque mix durante T_OZ s.

    JSON esperado: {"modulo": <uuid>, "estado": true|false}

    estado=true:  activa Ozono durante T_OZ s y luego lo desactiva.
    estado=false: desactiva Ozono inmediatamente.
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "estado") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    estado = bool(data["estado"])

    t = threading.Thread(
        target=_start_ozono_sequence,
        args=(estado,),
        daemon=True,
        name=f"ozono-{'on' if estado else 'off'}",
    )
    t.start()

    print(f"[OZONO] Comando recibido: estado={estado}")
    return jsonify({
        "status": "queued",
        "estado": estado,
        "duracion_s": T_OZ if estado else 0,
    }), 202


@app.route("/peris", methods=["POST"])
def handle_peris():
    """
    Activa una bomba peristáltica del tanque mix durante T_DOSIFICACION s.

    JSON esperado: {"modulo": <uuid>, "peris": 1|2|3|4, "estado": true|false}

    estado=true:  activa Peris_N durante T_DOSIFICACION s y luego la desactiva.
    estado=false: desactiva Peris_N inmediatamente.
    """
    data = flask_request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400

    missing = [k for k in ("modulo", "peris", "estado") if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    peris  = int(data["peris"])
    estado = bool(data["estado"])

    if peris not in (1, 2, 3, 4):
        return jsonify({"error": "peris must be 1, 2, 3 or 4"}), 422

    t = threading.Thread(
        target=_start_peris_sequence,
        args=(peris, estado),
        daemon=True,
        name=f"peris-{peris}-{'on' if estado else 'off'}",
    )
    t.start()

    print(f"[PERIS] Comando recibido: peris={peris} estado={estado}")
    return jsonify({
        "status": "queued",
        "peris": peris,
        "estado": estado,
        "duracion_s": T_DOSIFICACION if estado else 0,
    }), 202


@app.route("/abortar", methods=["POST"])
def handle_abortar():
    """
    Libera manualmente secuencias bloqueadas para evitar el error 409.

    JSON esperado: {"tanque": 1|2}  (opcional — sin campo limpia todos los tanques)

    Limpia _active_sequences y _pending para el tanque indicado.
    Si tanque=2 o no se especifica, también aborta el loop de llenado activo.
    """
    data = flask_request.get_json(silent=True) or {}

    tanque = data.get("tanque")
    fill_abortado = False
    tanques_liberados = []

    if tanque is not None:
        tanque = int(tanque)
        if tanque not in (1, 2):
            return jsonify({"error": "tanque must be 1 or 2"}), 422
        with _active_lock:
            if tanque in _active_sequences:
                _active_sequences.discard(tanque)
                _active_sequence_times.pop(tanque, None)
                tanques_liberados.append(tanque)
        with _pending_lock:
            _pending.pop(tanque, None)
        if tanque == 2 and _fill_active.is_set():
            _fill_active.clear()
            fill_abortado = True
    else:
        with _active_lock:
            tanques_liberados = list(_active_sequences)
            _active_sequences.clear()
            _active_sequence_times.clear()
        with _pending_lock:
            _pending.clear()
        if _fill_active.is_set():
            _fill_active.clear()
            fill_abortado = True

    print(f"[ABORTAR] Tanques liberados: {tanques_liberados} | fill_abortado: {fill_abortado}")
    return jsonify({
        "status": "ok",
        "tanques_liberados": tanques_liberados,
        "fill_abortado": fill_abortado,
    }), 200


@app.route("/status", methods=["GET"])
def node_status():
    return jsonify(_node_status), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "broker": BROKER, "port": PORT}), 200


@app.route("/test_mqtt", methods=["GET"])
def test_mqtt():
    """Diagnóstico: publica un mensaje de prueba y verifica si el broker lo confirma."""
    result = client.publish("test/subscriber_ping", "ping", qos=1)
    try:
        result.wait_for_publish(timeout=5)
    except Exception:
        pass
    return jsonify({
        "mqtt_connected_flag": _mqtt_connected.is_set(),
        "rc": result.rc,
        "puback_confirmado": result.is_published(),
        "broker": BROKER,
        "port": PORT,
    }), 200


if __name__ == "__main__":
    try:
        app.run(host=HTTP_HOST, port=HTTP_PORT, debug=False)
    finally:
        client.loop_stop()
        client.disconnect()
