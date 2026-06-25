"""
Cliente para la placa Kincony KC868-A64 corriendo el firmware 
ESPHome `a64_prod`.

La placa está configurada con la API nativa de ESPHome (NO con MQTT), por lo que
usamos `aioesphomeapi`, que es la misma librería que usa Home Assistant para
hablar con dispositivos ESPHome.

Parámetros tomados del YAML cargado en la placa:
  - host:        192.168.0.103 (IP estática)
  - api port:    6053 (default)
  - noise key:   "Dw3Z3r2KbL05KstmqaTSWpxvY/6A4WoRcOUKgq6W99Y="
  - device name: a64_prod

Capacidades expuestas:
  - 50 switches (relés) en los hubs PCF8575 (hub_out_1..4)
  - 3 botones de secuencia (`test_luces`, `test_distribucion`, 
  `retorno_t_mix`)
  - 22 sensores y binary_sensors (lectura)

Uso CLI:
    python a64_client.py list                                # listar entidades
    python a64_client.py on  EV_IN_BAND_01                   # encender salida
    python a64_client.py off LUZ_BAND_07                     # apagar salida
    python a64_client.py toggle A_MB200                      # conmutar
    python a64_client.py press test_luces                    # disparar botón
    python a64_client.py watch                               # ver cambios en vivo
    python a64_client.py status                              # snapshot actual

Uso como librería:
    import asyncio
    from a64_client import A64Client

    async def main():
        async with A64Client() as a64:
            await a64.set_switch("LUZ_BAND_01", True)
            await a64.press_button("retorno_t_mix")
            print(await a64.snapshot())
    asyncio.run(main())

Requisitos: pip install "aioesphomeapi>=24.0"
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Dict, Optional

from aioesphomeapi import (
    APIClient,
    APIConnectionError,
    ButtonInfo,
    EntityInfo,
    EntityState,
    InvalidEncryptionKeyAPIError,
    ResolveAPIError,
    SwitchInfo,
    SwitchState,
)

# --------------------------- Configuración --------------------------------
DEFAULT_HOST = os.environ.get("A64_HOST", "192.168.0.103")
DEFAULT_PORT = int(os.environ.get("A64_PORT", "6053"))
DEFAULT_NOISE_KEY = os.environ.get(
    "A64_NOISE_KEY",
    "Dw3Z3r2KbL05KstmqaTSWpxvY/6A4WoRcOUKgq6W99Y=",
)
DEFAULT_EXPECTED_NAME = os.environ.get("A64_EXPECTED_NAME", "produccion")# "a64_prod")

log = logging.getLogger("a64")


# --------------------------- Cliente -------------------------------------
class A64Client:
    """
    Cliente de alto nivel sobre `aioesphomeapi` para la placa a64_prod.

    Resuelve entidades por nombre amigable (el `name:` del YAML) y mantiene
    un cache de estados en memoria.
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        noise_psk: Optional[str] = DEFAULT_NOISE_KEY,
        expected_name: Optional[str] = DEFAULT_EXPECTED_NAME,
        password: str = "",            # la placa no usa password, solo noise
        keepalive: float = 15.0,
    ) -> None:
        self._host = host
        self._port = port
        self._client = APIClient(
            address=host,
            port=port,
            password=password,
            noise_psk=noise_psk,
            expected_name=expected_name,
            keepalive=keepalive,
            client_info="a64_client.py",
        )

        # name (lo que aparece en YAML como `name:`) -> EntityInfo
        self._switches_by_name: Dict[str, SwitchInfo] = {}
        self._buttons_by_name: Dict[str, ButtonInfo] = {}
        # key (entero estable por entidad) -> info
        self._info_by_key: Dict[int, EntityInfo] = {}
        # key -> último estado conocido (solo switches por ahora)
        self._state_by_key: Dict[int, bool] = {}

        self._connected = False

    # ---- context manager ------------------------------------------------
    async def __aenter__(self) -> "A64Client":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.disconnect()

    # ---- ciclo de vida ---------------------------------------------------
    async def connect(self) -> None:
        log.info("Conectando a la placa en %s:%s ...", self._host, self._port)
        try:
            await self._client.connect(login=True)
        except InvalidEncryptionKeyAPIError as e:
            raise RuntimeError(
                "Encryption key inválida: comprobá A64_NOISE_KEY o el YAML."
            ) from e
        except ResolveAPIError as e:
            raise RuntimeError(
                f"No se pudo resolver el host {self._client.address}. "
                "Verificá conectividad de red."
            ) from e
        except APIConnectionError as e:
            raise RuntimeError(f"Error de conexión: {e}") from e

        self._connected = True
        info = await self._client.device_info()
        log.info("Conectado a '%s' (ESPHome %s, modelo %s)",
                 info.name, info.esphome_version, info.model)

        entities, _services = await self._client.list_entities_services()
        for ent in entities:
            self._info_by_key[ent.key] = ent
            if isinstance(ent, SwitchInfo):
                self._switches_by_name[ent.name] = ent
            elif isinstance(ent, ButtonInfo):
                self._buttons_by_name[ent.name] = ent
        log.info("Entidades descubiertas: %d switches, %d botones",
                 len(self._switches_by_name), len(self._buttons_by_name))

        # Suscribirse a estados (se actualiza el cache en background)
        self._client.subscribe_states(self._on_state)

    async def disconnect(self) -> None:
        if self._connected:
            await self._client.disconnect()
            self._connected = False

    # ---- callbacks internos --------------------------------------------
    def _on_state(self, state: EntityState) -> None:
        if isinstance(state, SwitchState):
            self._state_by_key[state.key] = bool(state.state)
            info = self._info_by_key.get(state.key)
            name = info.name if info else f"key={state.key}"
            log.debug("Estado %-22s -> %s", name, "ON" if state.state else "OFF")

    # ---- helpers privados ----------------------------------------------
    def _switch_key(self, name: str) -> int:
        info = self._switches_by_name.get(name)
        if info is None:
            raise KeyError(
                f"No existe switch con name='{name}'. "
                f"Usá `list_switches()` para ver los disponibles."
            )
        return info.key

    def _button_key(self, name: str) -> int:
        info = self._buttons_by_name.get(name)
        if info is None:
            raise KeyError(
                f"No existe button con name='{name}'. "
                f"Usá `list_buttons()` para ver los disponibles."
            )
        return info.key

    # ---- API pública ----------------------------------------------------
    async def set_switch(self, name: str, on: bool) -> None:
        """Enciende/apaga un switch por su `name:` del YAML."""
        key = self._switch_key(name)
        log.info("-> %s := %s", name, "ON" if on else "OFF")
        self._client.switch_command(key, on)

    async def turn_on(self, name: str) -> None:
        await self.set_switch(name, True)

    async def turn_off(self, name: str) -> None:
        await self.set_switch(name, False)

    async def toggle(self, name: str) -> None:
        """Conmuta el switch basado en el último estado conocido."""
        key = self._switch_key(name)
        current = self._state_by_key.get(key)
        if current is None:
            # No tenemos estado todavía: esperá brevemente.
            for _ in range(20):  # 2 s máximo
                await asyncio.sleep(0.1)
                if key in self._state_by_key:
                    current = self._state_by_key[key]
                    break
            if current is None:
                raise RuntimeError(
                    f"Sin estado conocido para '{name}' tras 2 s. "
                    "Probá explícitamente turn_on/turn_off."
                )
        new_value = not current
        log.info("-> %s := %s (toggle desde %s)",
                 name, "ON" if new_value else "OFF",
                 "ON" if current else "OFF")
        self._client.switch_command(key, new_value)

    async def press_button(self, name: str) -> None:
        """Dispara un botón template (p.ej. 'test_luces')."""
        key = self._button_key(name)
        log.info("-> press '%s'", name)
        self._client.button_command(key)

    async def all_off(self) -> None:
        """Apaga TODOS los switches conocidos."""
        for name in self._switches_by_name:
            await self.set_switch(name, False)

    # ---- introspección -------------------------------------------------
    def list_switches(self) -> Dict[str, int]:
        """Devuelve {name: key} de todos los switches descubiertos."""
        return {n: i.key for n, i in self._switches_by_name.items()}

    def list_buttons(self) -> Dict[str, int]:
        return {n: i.key for n, i in self._buttons_by_name.items()}

    async def snapshot(self, wait_s: float = 1.0) -> Dict[str, Optional[bool]]:
        """
        Devuelve {name: True/False/None} para cada switch. None = no se
        recibió estado todavía.
        """
        await asyncio.sleep(wait_s)
        result: Dict[str, Optional[bool]] = {}
        for name, info in self._switches_by_name.items():
            result[name] = self._state_by_key.get(info.key)
        return result


# --------------------------- CLI ------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Cliente ESPHome nativo para a64_prod")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--noise-key", default=DEFAULT_NOISE_KEY)
    p.add_argument("--expected-name", default=DEFAULT_EXPECTED_NAME)
    p.add_argument("-v", "--verbose", action="store_true")

    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="Listar switches y botones disponibles")
    sp_on = sub.add_parser("on", help="Encender un switch")
    sp_on.add_argument("name")
    sp_off = sub.add_parser("off", help="Apagar un switch")
    sp_off.add_argument("name")
    sp_t = sub.add_parser("toggle", help="Conmutar un switch")
    sp_t.add_argument("name")
    sp_b = sub.add_parser("press", help="Disparar un botón template")
    sp_b.add_argument("name")
    sub.add_parser("all-off", help="Apagar TODOS los switches")
    sub.add_parser("status", help="Snapshot de estados")
    sub.add_parser("watch", help="Escuchar cambios en vivo (Ctrl-C para salir)")
    return p


async def _run(args: argparse.Namespace) -> int:
    async with A64Client(
        host=args.host,
        port=args.port,
        noise_psk=args.noise_key,
        expected_name=args.expected_name,
    ) as a64:
        if args.cmd == "list":
            print("== SWITCHES ==")
            for name in sorted(a64.list_switches()):
                print(f"  {name}")
            print("\n== BOTONES ==")
            for name in sorted(a64.list_buttons()):
                print(f"  {name}")
        elif args.cmd == "on":
            await a64.turn_on(args.name)
            await asyncio.sleep(0.5)
        elif args.cmd == "off":
            await a64.turn_off(args.name)
            await asyncio.sleep(0.5)
        elif args.cmd == "toggle":
            await a64.toggle(args.name)
            await asyncio.sleep(0.5)
        elif args.cmd == "press":
            await a64.press_button(args.name)
            await asyncio.sleep(0.5)
        elif args.cmd == "all-off":
            await a64.all_off()
            await asyncio.sleep(1.0)
        elif args.cmd == "status":
            snap = await a64.snapshot()
            for name in sorted(snap):
                val = snap[name]
                txt = "ON" if val is True else "OFF" if val is False else "?"
                print(f"  {name:25s} = {txt}")
        elif args.cmd == "watch":
            print("Escuchando cambios. Ctrl-C para salir.")
            try:
                while True:
                    await asyncio.sleep(3600)
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass
    return 0


def main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        sys.exit(asyncio.run(_run(args)))
    except RuntimeError as e:
        log.error("%s", e)
        sys.exit(1)
    except KeyError as e:
        log.error("%s", e)
        sys.exit(2)



if __name__ == "__main__":
    main()
