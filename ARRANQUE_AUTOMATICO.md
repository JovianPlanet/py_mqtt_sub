# Plan — Arranque automático del Subscriber (`py_mqtt_sub`) en la Raspberry Pi

**Objetivo:** que `chak_subscriber.py` se ejecute solo cada vez que la Pi arranca, usando el **entorno virtual propio del repositorio**, con reinicio automático ante fallos.

**Mecanismo:** un servicio `systemd` de sistema (arranca sin necesidad de iniciar sesión).

> En este repo se incluye el archivo de unidad de ejemplo [`chak-subscriber.service`](chak-subscriber.service). Ajusta en él el usuario y las rutas antes de copiarlo a `/etc/systemd/system/`.

---

## Paso 0 — Requisitos previos

- El repositorio `py_mqtt_sub` ya está en la Pi (por `git clone` o copiado).
- **Mosquitto** (broker MQTT) está instalado y habilitado en el arranque:
  ```bash
  sudo systemctl enable --now mosquitto
  systemctl is-enabled mosquitto   # debe decir: enabled
  ```
- El archivo `.env` del repo está completo (IPs, `API_KEY`, claves Noise de las placas).

---

## Paso 1 — Localizar el repositorio y el usuario

En la Pi, entra a la carpeta del repo y anota los datos que usaremos como variables:

```bash
cd ~/py_mqtt_sub        # ajusta si está en otra ruta
pwd                     # -> RUTA del repo, p.ej. /home/pi/py_mqtt_sub
whoami                  # -> USUARIO, p.ej. pi
```

> Anota ambos valores; los necesitas en el Paso 3. En los ejemplos siguientes uso `/home/pi/py_mqtt_sub` y usuario `pi`.

---

## Paso 2 — Recrear el entorno virtual **en la Pi** (importante)

El `venv` incluido en el repositorio fue creado en **Windows** y **no funciona en Linux** (un entorno virtual no es portable entre sistemas operativos). Hay que regenerarlo en la Pi (queda dentro del propio repo, como espera tu flujo de trabajo):

```bash
cd ~/py_mqtt_sub

# Borra el venv de Windows si vino incluido en la copia
rm -rf venv

# Crea el venv de Linux dentro del repo
python3 -m venv venv

# Instala dependencias dentro del venv del repo
./venv/bin/python -m pip install --upgrade pip
./venv/bin/pip install -r requirements.txt
```

Verifica que el intérprete del repo existe y tiene las dependencias:

```bash
./venv/bin/python -c "import flask, paho.mqtt.client, aioesphomeapi, dotenv; print('deps OK')"
```

> A partir de aquí, la ruta del Python del repo es: `/home/pi/py_mqtt_sub/venv/bin/python`

---

## Paso 3 — Prueba manual antes de automatizar

Confirma que el script arranca a mano usando el venv del repo:

```bash
cd ~/py_mqtt_sub
./venv/bin/python chak_subscriber.py
```

Debe mostrar `Connected to broker`, las suscripciones y `Running on http://0.0.0.0:8001`. En otra terminal:

```bash
curl http://localhost:8001/health     # -> {"status":"ok",...}
```

Detén con `Ctrl-C`. Si esto funciona, el servicio también lo hará.

---

## Paso 4 — Instalar el servicio systemd

Este repo incluye `chak-subscriber.service`. Ábrelo y **ajusta `User`, `WorkingDirectory`, `EnvironmentFile` y `ExecStart`** a tu usuario y ruta reales; luego cópialo a systemd:

```bash
cd ~/py_mqtt_sub
nano chak-subscriber.service        # ajustar rutas/usuario si es necesario
sudo cp chak-subscriber.service /etc/systemd/system/chak-subscriber.service
```

Contenido de referencia (para usuario `pi` y ruta `/home/pi/py_mqtt_sub`):

```ini
[Unit]
Description=CHAK Subscriber (gateway MQTT/HTTP hidroponia)
After=network-online.target mosquitto.service
Wants=network-online.target
Requires=mosquitto.service

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/py_mqtt_sub
EnvironmentFile=/home/pi/py_mqtt_sub/.env
ExecStart=/home/pi/py_mqtt_sub/venv/bin/python /home/pi/py_mqtt_sub/chak_subscriber.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Puntos clave de esta configuración:
- **`ExecStart`** apunta al **Python del venv del repo** (`venv/bin/python`), así no dependes de activar el entorno ni de paquetes globales.
- **`Requires`/`After` mosquitto** garantiza que el broker MQTT esté arriba antes (el script se conecta a MQTT al iniciar).
- **`EnvironmentFile`** carga tu `.env` (además el script lo lee con `python-dotenv`).
- **`Restart=on-failure`** relanza el proceso si se cae.

---

## Paso 5 — Habilitar y arrancar el servicio

```bash
sudo systemctl daemon-reload
sudo systemctl enable chak-subscriber.service    # arranca en cada boot
sudo systemctl start chak-subscriber.service     # arranca ahora
sudo systemctl status chak-subscriber.service    # debe decir: active (running)
```

---

## Paso 6 — Verificar el arranque automático real

```bash
sudo reboot
```

Al reconectar (espera ~1–2 min):

```bash
systemctl is-enabled chak-subscriber    # -> enabled
systemctl is-active  chak-subscriber    # -> active
curl http://localhost:8001/health       # -> {"status":"ok",...}
```

---

## Comandos de operación y diagnóstico

```bash
# Ver logs en vivo
sudo journalctl -u chak-subscriber -f

# Últimas 200 líneas
sudo journalctl -u chak-subscriber -n 200 --no-pager

# Reiniciar (tras editar .env o hacer git pull)
sudo systemctl restart chak-subscriber

# Detener / arrancar
sudo systemctl stop chak-subscriber
sudo systemctl start chak-subscriber

# Deshabilitar el arranque automático
sudo systemctl disable chak-subscriber
```

**Al actualizar el código:**
```bash
cd ~/py_mqtt_sub
git pull
./venv/bin/pip install -r requirements.txt   # por si cambian dependencias
sudo systemctl restart chak-subscriber
```

---

## Solución de problemas

| Síntoma | Causa probable | Acción |
|---|---|---|
| Servicio en bucle (`activating`/`failed`) justo al bootear | Arrancó antes que Mosquitto | Ya cubierto por `Requires/After mosquitto`; revisa `journalctl` |
| `ModuleNotFoundError` en los logs | venv sin dependencias o venv de Windows | Repite el Paso 2 (recrear venv en la Pi) |
| `No such file or directory` en `ExecStart` | Ruta del repo o del venv incorrecta | Corrige rutas en el `.service` y `daemon-reload` |
| `Connection refused` a MQTT | Mosquitto caído | `sudo systemctl status mosquitto` |
| Arranca a mano pero no como servicio | Usuario/permisos del `.service` | Revisa que `User=` sea el dueño del repo y del `.env` |

---

## Resumen (checklist)

- [ ] Mosquitto `enabled`.
- [ ] `.env` completo.
- [ ] **venv recreado en la Pi** (Linux) e instaladas dependencias.
- [ ] Prueba manual con `./venv/bin/python chak_subscriber.py` OK.
- [ ] `chak-subscriber.service` ajustado y copiado a `/etc/systemd/system/`.
- [ ] `enable` + `start` y `status` en *active (running)*.
- [ ] Verificado tras un `reboot` real.
