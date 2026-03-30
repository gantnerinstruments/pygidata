# -*- coding: utf-8 -*-
"""
ONLINE WebSocket read example (values only).

Reads ONLINE process image channels via WebSocket and prints values.
"""

from __future__ import annotations

import time
import argparse

from ginsutility.highspeedport import HighSpeedPortClient


def parse_channels(s: str) -> list[int]:
    return [int(x) for x in s.split(",") if x.strip()]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8090)
    p.add_argument("--route", default="")
    p.add_argument("--username", default="")
    p.add_argument("--password", default="")
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--interval-ms", type=int, default=200)

    p.add_argument("--channels", default="0,1,2,3,4,5", help="Comma-separated ONLINE channel indices")
    p.add_argument("--loops", type=int, default=20)
    p.add_argument("--sleep", type=float, default=0.2)
    args = p.parse_args()

    channels = parse_channels(args.channels)

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

        for _ in range(args.loops):
            values = [conn.read_online_single(ch) for ch in channels]
            print(values)
            time.sleep(args.sleep)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
