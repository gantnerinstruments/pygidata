import asyncio
import time
from pprint import pprint
from random import random
from typing import List
from uuid import UUID

import pandas as pd
import logging

from gimodules.gi_data.dataclient import GIDataClient
from gimodules.gi_data.drivers.cloud_gql import CloudGQLDriver
from gimodules.gi_data.mapping.models import VarSelector
from gimodules.gi_data.utils.logging import setup_module_logger

logger = setup_module_logger(__name__, level=logging.DEBUG)

# ─────── CONFIGURATION ─────────────────────────────────────────────────────────

ACTIVE_PROFILE = "cloud"  # Change this to: "stream", "records", "cloud", "qstation"

CONFIGS = {
    "qcore": {
        "base": "http://qcore-111006:8090",
        "auth": {"username": "admin", "password": "admin"}
    },
    "qstation": {
        "base": "http://10.1.50.36:8090",
        "auth": {"username": "admin", "password": "admin"}
    },
    "bench": {
        "base": "http://127.0.0.1:8090",
        "auth": {"username": "admin", "password": "admin"}
    },
    "cloud": {
        "base": "https://demo.gi-cloud.io",
        "auth": {"access_token": "9a430f6c-5bd1-473d-9a78-a1ec93796540"}
    },
}

def get_client(config = CONFIGS[ACTIVE_PROFILE]) -> GIDataClient:
    conf = config
    if "access_token" in conf["auth"]:
        return GIDataClient(conf["base"], access_token=conf["auth"]["access_token"])
    return GIDataClient(conf["base"], username=conf["auth"]["username"], password=conf["auth"]["password"])

# ─────── TEST CASES ────────────────────────────────────────────────────────────

def subscribe_publish():
    import asyncio

    client = get_client(CONFIGS["qstation"])


    variables = client.list_variables()
    vids = [v.id for v in variables]

    var_to_write = next((v for v in variables if v.name == "SP_1"), None)

    async def live_stream():
        async for tick in client.stream_online(vids, interval_ms=1_000, on_change=True):
            print("Subscribe tick:", tick)
            # Variable has to be Output or Input/Output
            await client.publish_online({var_to_write.id: random()})

    asyncio.run(live_stream())


async def online():
    client = get_client(CONFIGS["cloud"])

    src = client.list_buffer_sources()[0]
    logger.info("Selected src: {}".format(src))

    variables = client.list_stream_variables(src.id)[:1]
    selectors: List[VarSelector] = [
        VarSelector(SID=v.sid, VID=v.id)
        for v in variables
    ]
    logger.info(f"Selected variables: {selectors}")

    for i in range(0, 2):
        #df = client.fetch_buffer(selectors, start_ms=1752066551495.4802, end_ms=-1752070152495.4802)
        df = client.fetch_buffer(selectors, start_ms=src.last_ts-9999, end_ms=src.last_ts)
        pprint(df.head())
        time.sleep(1)
        df.to_csv("debug_output.csv")

async def buffer():
    client = get_client(CONFIGS["qstation"])

    buffers = client.list_buffer_sources()
    #src = next((s for s in buffers if s.name == "demo_otf4"), None)
    src = buffers[0]
    logger.info(f"Selected Buffer source: {src}")

    variables = client.list_stream_variables(src.id)[:99]

    selectors: List[VarSelector] = [
        VarSelector(SID=v.sid, VID=v.id)
        for v in variables
    ]

    logger.info(f"Selected variables: {selectors}")

    for i in range(0, 1):
        #df = client.fetch_buffer(selectors, start_ms=1752066551495.4802, end_ms=-1752070152495.4802)
        #1637045140000
        # resolution depending on if we have enough points set
        df = client.fetch_buffer(selectors, start_ms=src.last_ts-9999, end_ms=src.last_ts, points=10000000)
        pprint(df.head())
        print(df.tail())
        print(len(df.index))
        time.sleep(1)
        df.to_csv("debug_output.csv")

async def export_csv():
    client = get_client(CONFIGS["qstation"])
    buffers = client.list_buffer_sources()
    #src = next((s for s in buffers if s.name == "demo_otf4"), None)
    src = buffers[0]


    logger.info(f"Selected Buffer source: {src}")

    variables = client.list_stream_variables(src.id)[:99]

    selectors: List[VarSelector] = [
        VarSelector(SID=v.sid, VID=v.id)
        for v in variables
    ]
    raw = client.export_csv(
        selectors,
        start_ms=src.last_ts - 9999,
        end_ms=src.last_ts
    )

    # Save for debugging
    with open("debug_exportcsv.csv", "wb") as f:
        f.write(raw)

    # Convert to DataFrame
    from io import BytesIO
    import pandas as pd
    df = pd.read_csv(BytesIO(raw), sep=";", decimal=",")


async def history():
    #BASE = "http://10.1.50.36:8090"
    client = get_client(CONFIGS["qcore"])

    src = client.list_history_sources()[0]
    logger.info(f"Selected Buffer source: {src}")

    meas = client.list_history_measurements(src.id)[-1]
    logger.info(f"Selected Measurement : {meas}")

    vars_ = [UUID(v.id) for v in client.list_history_variables(src.id)[:2]]
    df = client.fetch_history(src.id, meas.id, vars_, start_ms=meas.absolute_start, end_ms=meas.last_ts)
    print(df.head())


def stream_kafka_test():
    async def stream():
        client = GIDataClient("http://10.1.50.36:8090")
        vids = [UUID(m["Id"]) for m in client.list_variables()[:4]]
        logger.info(vids)

        async def kafka_reader():
            async for tick in client.stream_kafka(vids):
                print(tick)

        try:
            await asyncio.wait_for(kafka_reader(), timeout=10.0)  # 10 Sek. Timeout
        except asyncio.TimeoutError:
            logger.info("Kafka stream test finished after timeout.")
        except Exception:
            logger.exception("Kafka stream failed")

    asyncio.run(stream())

def head(df: pd.DataFrame, n: int = 5) -> None:
    logging.info("\n%s", df.head(n).to_string())

async def cloud_test() -> None:
    BASE_URL = "https://demo.gistg.lovelysystems.com"
    DRIVER = CloudGQLDriver
    TOKEN = "f9795021-d81e-49fc-8c4e-66df373a07f3"

    client = GIDataClient(
        BASE_URL,
        driver_cls=DRIVER,
        access_token=TOKEN if TOKEN else None,
    )

    # 1) ─── ONLINE READ / WRITE ───────────────────────────────────── #
    online_raw = client.list_variables()
    online_vids: List[UUID] = [UUID(v["Id"]) for v in online_raw[:3]]

    logging.info("Reading online vars %s", online_vids)
    values = client.read_online(online_vids)
    logging.info("Online values: %s", values)

    logging.info("Writing %.3f back to %s", 42.123, online_vids[-1])
    client.write_online({online_vids[-1]: 42.123})

    # 2) ─── BUFFER WINDOW ------------------------------------------ #
    # src = client.list_buffer_sources()[0]
    # vars_ = client.list_stream_variables(src.id)[:99]
    # selectors = [(v.sid, v.id) for v in vars_]
    #
    # logging.info("Fetching last 5 s buffer window from %s", src.name)
    #
    # buf_df = client.fetch_buffer(selectors, start_ms=1738368000000, end_ms=1738368300000, points=10_0000)
    #
    # buf_df.to_csv("debug_output.csv")

    # 3) ─── HISTORY SLICE ------------------------------------------ #
    # meas = client.list_history_measurements(src.id)[-1]
    # hist_df = client.fetch_history(
    #     src.id,
    #     meas["Id"],
    #     [UUID(vars_[0].id)],
    #     start_ms=meas["Start"],
    #     end_ms=meas["End"],
    # )
    # head(hist_df)
    #

# ─────── ENTRY POINT ──────────────────────────────────────────────────────────

if __name__ == '__main__':
    #asyncio.run(subscribe_publish())
    #asyncio.run(buffer())

    #asyncio.run(history())
    #asyncio.run(cloud_test())
    #stream_kafka_test()
    #asyncio.run(online())
    asyncio.run(export_csv())