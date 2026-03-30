from datetime import datetime, timezone, timedelta


def ole_ts_to_unix_ms_utc(ts: float) -> int:
    """Needs OLE TS as input like: 45894.553495370375 """
    base = datetime(1899, 12, 30, tzinfo=timezone.utc)
    dt = base + timedelta(days=float(ts))
    return int(dt.timestamp() * 1000)


def ns2000_to_unix_ms(ts: float) -> int:
    """Needs NS TS as input like: 809444712960000100"""
    ns_unix = int(ts) + 946684800 * 1_000_000_000
    return ns_unix // 1_000_000


def print_ns2000(ts: float):
    ns_unix = int(ts) + 946684800 * 1_000_000_000
    sec, ns = divmod(ns_unix, 1_000_000_000)
    dt = datetime.fromtimestamp(sec, tz=timezone.utc).replace(microsecond=ns // 1000)
    print(dt.isoformat())
