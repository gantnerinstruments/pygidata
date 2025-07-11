import asyncio
import time
from pprint import pprint
from random import random
from uuid import UUID

from gimodules.gi_data.dataclient import GIDataClient
import logging
from gimodules.gi_data.utils.logging import setup_module_logger

logger = setup_module_logger(__name__, level=logging.DEBUG)

async def buffer():
    BASE = "http://10.1.50.36:8090"
    #BASE = "http://qcore-111001:8090" # stream
    #BASE = "http://qcore-111004:8090"  # records


    client = GIDataClient(BASE, username="admin", password="admin")

    src = client.list_buffer_sources()[0]

    logger.info(f"Selected Buffer source: {src}")

    vars = client.list_stream_variables(src.id)[:1]
    selectors = [(v.sid, v.id) for v in vars]

    logger.info(f"Selected variables: {selectors}")

    for i in range(0, 2):
        #df = client.fetch_buffer(selectors, start_ms=1752066551495.4802, end_ms=-1752070152495.4802)
        df = client.fetch_buffer(selectors, start_ms=-10_000, end_ms=0)
        pprint(df.head())
        time.sleep(1)
        df.to_csv("debug_output.csv")

async def history():
    #BASE = "http://10.1.50.36:8090"
    BASE = "http://qcore-111004:8090"  # records

    client = GIDataClient(BASE, username="admin", password="admin")

    src = client.list_buffer_sources()[0]
    logger.info(f"Selected Buffer source: {src}")
    meas = client.list_history_measurements(src.id)[-1]

    logger.info(f"Selected Measurement : {meas}")
    vars_ = [UUID(v.id) for v in client.list_stream_variables(src.id)[:2]]

    df = client.fetch_history(src.id, meas.id, vars_, start_ms=meas.absolute_start, end_ms=meas.last_ts)
    print(df.head())


def subscribe_publish():
    import asyncio
    from uuid import UUID
    from gimodules.gi_data.dataclient import GIDataClient

    client = GIDataClient("http://10.1.50.36:8090")


    vids = [UUID(m["Id"]) for m in client.list_variables()[:4]]

    async def live_stream():
        async for tick in client.stream_online(vids, interval_ms=10000, on_change=True):
            #print("tick:", tick)
            # Variable has to be Output or Input/Output
            await client.publish_online({vids[3]: random()})

    asyncio.run(live_stream())



if __name__ == '__main__':
    #subscribe_publish()
    #asyncio.run(buffer())
    asyncio.run(history())