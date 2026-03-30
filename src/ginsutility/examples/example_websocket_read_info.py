# -*- coding: utf-8 -*-
"""
Prints all available *meta* info for information purpose.
in your HighSpeedPortClient implementation.
"""
from __future__ import annotations

import argparse
import time

from ginsutility.highspeedport import HighSpeedPortClient
from ginsutility.models import DeviceInfoType


def dump_device_info(conn: HighSpeedPortClient) -> None:
    print("\n=== DEVICE INFO ===")

    def show(label: str, value):
        print(f"{label:25s}: {value}")

    show("Serial number", conn.read_serial_number())
    show("Sample rate", conn.read_sample_rate())
    show("Controller name", conn.read_controller_name())
    show("Controller address", conn.read_controller_address())

    for info in DeviceInfoType:
        try:
            val = conn.get_device_info(info, as_string=True)
            if val:
                show(f"{info.name} (str)", val)
        except Exception:
            pass
        try:
            val = conn.get_device_info(info, as_string=False)
            if val not in (None, 0):
                show(f"{info.name} (num)", val)
        except Exception:
            pass


def dump_channels(conn: HighSpeedPortClient) -> None:
    print("\n=== CHANNELS ===")

    count = conn.read_channel_count()
    print(f"Channel count: {count}")

    names = conn.read_channel_names()

    for i in range(count):
        name = names.get(i, "")
        unit = conn.read_index_unit(i)
        print(f"[{i:3d}] name='{name}' unit='{unit}'")


def dump_post_process_buffers(conn: HighSpeedPortClient) -> None:
    print("\n=== POST-PROCESS BUFFERS ===")

    count = conn.get_buffer_count()
    if count < 0:
        print("Post-process buffers not supported by this GiUtility")
        return

    print(f"Buffer count: {count}")
    for i in range(count):
        info = conn.get_buffer_info(i)
        if info:
            print(f"[{info.index}] id='{info.buffer_id}' name='{info.name}'")


def dump_connection_state(conn: HighSpeedPortClient) -> None:
    print("\n=== CONNECTION STATE ===")
    print(f"Connected        : {conn.is_connected}")
    print(f"Connection state : {conn.connection_state.name}")
    print(f"Last error code  : {conn.last_error}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8090)
    p.add_argument("--route", default="")
    p.add_argument("--username", default="")
    p.add_argument("--password", default="")
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--interval-ms", type=int, default=200)
    args = p.parse_args()

    with HighSpeedPortClient() as conn:
        ok = conn.init_websocket_online(
            url=args.url,
            port=args.port,
            route=args.route,
            username=args.username,
            password=args.password,
            timeout_sec=args.timeout,
            interval_ms=args.interval_ms,
        )
        if not ok:
            raise SystemExit("init_websocket_online failed")
        dump_connection_state(conn)
        dump_device_info(conn)
        dump_channels(conn)
        dump_post_process_buffers(conn)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
