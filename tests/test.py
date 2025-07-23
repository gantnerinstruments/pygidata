import asyncio
import time
from pprint import pprint
from random import random
from uuid import UUID

import pandas as pd

from gimodules.gi_data.dataclient import GIDataClient
import logging

from gimodules.gi_data.drivers.cloud_gql import CloudGQLDriver
from gimodules.gi_data.utils.logging import setup_module_logger

logger = setup_module_logger(__name__, level=logging.DEBUG)

async def online():
    BASE = "https://demo.gi-cloud.io"  # cloud
    client = GIDataClient(BASE, access_token='9a430f6c-5bd1-473d-9a78-a1ec93796540')

    src = client.list_buffer_sources()[0]

    logger.info("Selected src: {}".format(src))

    vars = client.list_stream_variables(src.id)[:1]
    selectors = [(v.sid, v.id) for v in vars]

    logger.info(f"Selected variables: {selectors}")

    for i in range(0, 2):
        #df = client.fetch_buffer(selectors, start_ms=1752066551495.4802, end_ms=-1752070152495.4802)
        df = client.fetch_buffer(selectors, start_ms=-10_000, end_ms=0)
        pprint(df.head())
        time.sleep(1)
        df.to_csv("debug_output.csv")

async def buffer():
    #BASE = "http://10.1.50.41:8090"
    #BASE = "http://qcore-111001:8091" # stream
    #BASE = "http://qcore-111004:8090"  # records
    #client = GIDataClient(BASE, username="admin", password="admin")

    BASE = "https://demo.gi-cloud.io"
    client = GIDataClient(BASE, access_token='9a430f6c-5bd1-473d-9a78-a1ec93796540')


    buffers = client.list_buffer_sources()

    #src = next((s for s in buffers if s.name == "demo_otf4"), None)
    src = buffers[0]
    logger.info(f"Selected Buffer source: {src}")

    vars = client.list_stream_variables(src.id)[:1]
    selectors = [(v.sid, v.id) for v in vars]

    logger.info(f"Selected variables: {selectors}")

    for i in range(0, 1):
        #df = client.fetch_buffer(selectors, start_ms=1752066551495.4802, end_ms=-1752070152495.4802)
        #1637045140000
        df = client.fetch_buffer(selectors, start_ms=-100000, end_ms=0)
        pprint(df.head())
        print(df.tail())
        print(len(df.index))
        time.sleep(1)
        df.to_csv("debug_output.csv")

async def history():
    #BASE = "http://10.1.50.36:8090"
    BASE = "http://qcore-111004:8090"  # records

    client = GIDataClient(BASE, username="admin", password="admin")

    src = client.list_history_sources()[0]
    logger.info(f"Selected Buffer source: {src}")
    meas = client.list_history_measurements(src.id)[-1]

    logger.info(f"Selected Measurement : {meas}")
    vars_ = [UUID(v.id) for v in client.list_history_variables(src.id)[:2]]

    df = client.fetch_history(src.id, meas.id, vars_, start_ms=meas.absolute_start, end_ms=meas.last_ts)
    print(df.head())


def subscribe_publish():
    import asyncio
    from uuid import UUID
    from gimodules.gi_data.dataclient import GIDataClient

    client = GIDataClient("http://10.1.50.36:8090")


    vids = [UUID(m["Id"]) for m in client.list_variables()[:4]]

    async def live_stream():
        async for tick in client.stream_online(vids, interval_ms=10_000, on_change=True):
            #print("tick:", tick)
            # Variable has to be Output or Input/Output
            await client.publish_online({vids[3]: random()})

    asyncio.run(live_stream())

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

    BASE_URL = "https://demo.gi-cloud.io"
    DRIVER = CloudGQLDriver
    TOKEN = "61edd4db-7d7f-4da2-9f52-d6997c120e86"

    client = GIDataClient(
        BASE_URL,
        driver_cls=DRIVER,
        username="admin",
        password="asd8fAasEd",
        #access_token=TOKEN if TOKEN else None,
    )

    # 1) ─── ONLINE READ / WRITE ───────────────────────────────────── #
    # online_raw = client.list_variables()
    # online_vids: List[UUID] = [UUID(v["Id"]) for v in online_raw[:3]]
    #
    # logging.info("Reading online vars %s", online_vids)
    # values = client.read_online(online_vids)
    # logging.info("Online values: %s", values)
    #
    # logging.info("Writing %.3f back to %s", 42.123, online_vids[-1])
    # client.write_online({online_vids[-1]: 42.123})

    # 2) ─── BUFFER WINDOW ------------------------------------------ #
    src = client.list_buffer_sources()[0]

    vars_ = client.list_stream_variables(src.id)[:1]
    selectors = [(v.sid, v.id) for v in vars_]

    logging.info("Fetching last 5 s buffer window from %s", src.name)
    buf_df = client.fetch_buffer(selectors, start_ms=-5_000, end_ms=0)
    head(buf_df)

    # 3) ─── HISTORY SLICE ------------------------------------------ #
    meas = client.list_history_measurements(src.id)[-1]
    hist_df = client.fetch_history(
        src.id,
        meas["Id"],
        [UUID(vars_[0].id)],
        start_ms=meas["Start"],
        end_ms=meas["End"],
    )
    head(hist_df)

    # 4) ─── WEBSOCKET STREAM + PUBLISH LOOP ― run 5 ticks ―───────── #
    async def ws_loop() -> None:
        async for i, tick in enumerate(
            client.stream_online(online_vids, interval_ms=1_000)
        ):
            logging.info("tick %d → %s", i, tick)
            await client.publish_online({online_vids[-1]: random()})
            if i >= 4:
                break

    logging.info("Starting 5-tick WebSocket demo …")
    await ws_loop()

    client.close()
    logging.info("Demo finished ✔")


if __name__ == '__main__':
    #subscribe_publish()
    asyncio.run(buffer())
    #asyncio.run(history())
    #asyncio.run(cloud_test())
    #stream_kafka_test()
    #asyncio.run(online())