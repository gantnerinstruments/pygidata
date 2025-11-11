from __future__ import annotations

import asyncio
from random import random
from uuid import UUID

from gi_data.dataclient import GIDataClient

"""
The GI device does not always start sending Online-Data until the TCP/WS
buffers are cleared.
By waiting for the first message we guarantee that the subscription handshake
is complete before hammering it with writes.

After that, reads (recv) and writes (send) occur concurrently and the
driver logs Received Payload [WS]: … again.
"""
async def consumer(
    client: GIDataClient,
    vids: list[UUID],
    first_tick_event: asyncio.Event,
) -> None:
    async for tick in client.stream_online(
        vids,
        interval_ms=1,
        on_change=False,
    ):
        print("RX:", tick)

        # Tell main() that the subscription is alive.
        if not first_tick_event.is_set():
            first_tick_event.set()


async def publisher(
    client: GIDataClient,
    vid_to_write: UUID,
    period_s: float = 0.1,
) -> None:
    """Send a new random value every *period_s* seconds."""
    while True:
        payload = {vid_to_write: random()}
        await client.publish_online(payload)
        print("TX:", payload)
        await asyncio.sleep(period_s)


async def live() -> None:
    client = GIDataClient("http://10.1.50.36:8090")

    vids = [UUID(meta["Id"]) for meta in client.list_variables()[:4]]
    vid_out = vids[3]                      # must be Output or In/Out

    first_tick = asyncio.Event()

    # Start only the reader at first.
    consumer_task = asyncio.create_task(consumer(client, vids, first_tick))

    await first_tick.wait()

    publisher_task = asyncio.create_task(publisher(client, vid_out, 0.5))

    # Run both indefinitely.
    await asyncio.gather(consumer_task, publisher_task)


def run_live() -> None:
    asyncio.run(live())


if __name__ == "__main__":
    run_live()
