# -*- coding: utf-8 -*-
"""
ONLINE WebSocket write example (values only).

Writes values to ONLINE process image channels via WebSocket.
Note: Only Input or Input/Output channels can be written to!.
"""

from __future__ import annotations

import argparse
import time

from ginsutility.highspeedport import HighSpeedPortClient


def parse_channels(s: str) -> list[int]:
    return [int(x) for x in s.split(",") if x.strip()]


def parse_values(s: str) -> list[float]:
    return [float(x) for x in s.split(",") if x.strip()]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8090)
    p.add_argument("--route", default="")
    p.add_argument("--username", default="")
    p.add_argument("--password", default="")
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--interval-ms", type=int, default=200)

    p.add_argument("--channels", required=True, help="Comma-separated ONLINE channel indices")
    p.add_argument("--values", required=True, help="Comma-separated values (same count as channels)")
    p.add_argument("--immediate", action="store_true", help="Use immediate write")
    args = p.parse_args()

    channels = parse_channels(args.channels)
    values = parse_values(args.values)

    if len(channels) != len(values):
        raise SystemExit("channels and values must have same length")

    with HighSpeedPortClient() as conn:
        if not conn.init_websocket_online(
                url=args.url,
                port=args.port,
                route=args.route,
                username=args.username,
                password=args.password,
                timeout_sec=args.timeout,
                interval_ms=args.interval_ms,
        ):
            raise SystemExit("init_websocket_online failed")

        for ch, val in zip(channels, values):
            print("Writing value", val, "to channel", ch)
            if not conn.write_online_value(ch, val, immediate=args.immediate):
                raise SystemExit(f"write failed for channel {ch}")
        for i in range(0, 100):
            time.sleep(1)
            print("Writing value", i, "to channel", channels[0])
            if not conn.write_online_value(channels[0], i, immediate=args.immediate):
                raise SystemExit(f"write failed for channel {channels[0]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
