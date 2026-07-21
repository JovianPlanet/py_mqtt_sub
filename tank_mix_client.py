"""
Cliente para la placa Kincony KC868-A64 corriendo el firmware ESPHome `tank-mix`.

Conexión vía API nativa ESPHome (aioesphomeapi). La placa usa Ethernet (LAN8720),
no WiFi.

Parámetros tomados del YAML cargado en la placa:
  - host:        192.168.0.102 (IP estática)
  - api port:    6053 (default)
  - noise key:   configurar en env TANKMIX_NOISE_KEY
  - device name: tank-mix

Salidas (8 switches):
  - Motobomba       — MB101   canal 0
  - Agitador        — AG101   canal 1
  - Ozono           — EOZ101  canal 2
  - Entrada EPM     — EV101   canal 3
  - Peris_1         — BP101_B canal 1
  - Peris_2         — BP102_A canal 2
  - Peris_3         — BP103_A canal 4
  - Peris_4         — BP104_A canal 6

Botones (4, dosificado 10 ml en 51 s cada uno):
  - Añadir 10 ml BP_1
  - Añadir 10 ml BP_2
  - Añadir 10 ml BP_3
  - Añadir 10 ml BP_4

Uso como librería:
    import asyncio
    from tank_mix_client import TankMixClient

    async def main():
        async with TankMixClient() as tmix:
            await tmix.turn_on("Motobomba")
            await tmix.press_button("Añadir 10 ml BP_1")
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
DEFAULT_HOST = os.environ.get("TANKMIX_HOST", "192.168.0.102")
DEFAULT_PORT = int(os.environ.get("TANKMIX_PORT", "6053"))
DEFAULT_NOISE_KEY = os.environ.get(
    "TANKMIX_NOISE_KEY",
    "ojkGTpvfiDJwONH69xSDFDImSyLZZfT6IuCcNwL28gU=",
)
DEFAULT_EXPECTED_NAME = os.environ.get("TANKMIX_EXPECTED_NAME", "tank-mix")

log = logging.getLogger("tankmix")


# --------------------------- Cliente -------------------------------------
class TankMixClient:
    """
    Cliente de alto nivel sobre `aioesphomeapi` para la placa tank-mix.

    Resuelve entidades por nombre amigable (el `name:` del YAML) y mantiene
    un cache de estados en memoria.
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        noise_psk: Optional[str] = DEFAULT_NOISE_KEY,
        expected_name: Optional[str] = DEFAULT_EXPECTED_NAME,
        password: str = "",
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
            client_info="tank_mix_client.py",
        )

        self._switches_by_name: Dict[str, SwitchInfo] = {}
        self._buttons_by_name: Dict[str, ButtonInfo] = {}
        self._info_by_key: Dict[int, EntityInfo] = {}
        self._state_by_key: Dict[int, bool] = {}

        self._connected = False

    # ---- context manager ------------------------------------------------
    async def __aenter__(self) -> "TankMixClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.disconnect()

    # ---- ciclo de vida ---------------------------------------------------
    async def connect(self) -> None:
        log.info("Conectando a tank-mix en %s:%s ...", self._host, self._port)
        try:
            await self._client.connect(login=True)
        except InvalidEncryptionKeyAPIError as e:
            raise RuntimeError(
                "Encryption key inválida: comprobá TANKMIX_NOISE_KEY o el YAML."
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
        key = self._switch_key(name)
        log.info("-> %s := %s", name, "ON" if on else "OFF")
        self._client.switch_command(key, on)

    async def turn_on(self, name: str) -> None:
        await self.set_switch(name, True)

    async def turn_off(self, name: str) -> None:
        await self.set_switch(name, False)

    async def toggle(self, name: str) -> None:
        key = self._switch_key(name)
        current = self._state_by_key.get(key)
        if current is None:
            for _ in range(20):
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
        key = self._button_key(name)
        log.info("-> press '%s'", name)
        self._client.button_command(key)

    async def all_off(self) -> None:
        for name in self._switches_by_name:
            await self.set_switch(name, False)

    # ---- introspección -------------------------------------------------
    def list_switches(self) -> Dict[str, int]:
        return {n: i.key for n, i in self._switches_by_name.items()}

    def list_buttons(self) -> Dict[str, int]:
        return {n: i.key for n, i in self._buttons_by_name.items()}

    async def snapshot(self, wait_s: float = 1.0) -> Dict[str, Optional[bool]]:
        await asyncio.sleep(wait_s)
        result: Dict[str, Optional[bool]] = {}
        for name, info in self._switches_by_name.items():
            result[name] = self._state_by_key.get(info.key)
        return result


# --------------------------- CLI ------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Cliente ESPHome nativo para tank-mix")
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
    async with TankMixClient(
        host=args.host,
        port=args.port,
        noise_psk=args.noise_key,
        expected_name=args.expected_name,
    ) as tmix:
        if args.cmd == "list":
            print("== SWITCHES ==")
            for name in sorted(tmix.list_switches()):
                print(f"  {name}")
            print("\n== BOTONES ==")
            for name in sorted(tmix.list_buttons()):
                print(f"  {name}")
        elif args.cmd == "on":
            await tmix.turn_on(args.name)
            await asyncio.sleep(0.5)
        elif args.cmd == "off":
            await tmix.turn_off(args.name)
            await asyncio.sleep(0.5)
        elif args.cmd == "toggle":
            await tmix.toggle(args.name)
            await asyncio.sleep(0.5)
        elif args.cmd == "press":
            await tmix.press_button(args.name)
            await asyncio.sleep(0.5)
        elif args.cmd == "all-off":
            await tmix.all_off()
            await asyncio.sleep(1.0)
        elif args.cmd == "status":
            snap = await tmix.snapshot()
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
