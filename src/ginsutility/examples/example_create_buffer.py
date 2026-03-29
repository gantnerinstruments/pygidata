"""Create a new buffer and fill it with random values."""
from __future__ import annotations

import argparse
import logging
import time
from typing import Iterable

import ginsutility.postprocessbuffer as postprocessbuffer_manager
import numpy as np
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a buffer",
        formatter_class=CustomHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help=(
            "Show this help message and exit. All arguments are optional; "
            "strings do not need to be quoted."
        ),
    )
    parser.add_argument(
        "-i",
        "--ID",
        type=str,
        default="ff1fbdd4-7b23-11ea-bd6d-005056c00001",
        help="Buffer UUID in GI.bench (default: %(default)s)",
        metavar="",
    )
    parser.add_argument(
        "-b",
        "--BufferName",
        type=str,
        default="PythonBuffer",
        help="Name of the buffer (default: %(default)s)",
        metavar="",
    )
    parser.add_argument(
        "-s",
        "--StreamSampleRate",
        type=int,
        default=10,
        help="Sampling rate in Hz (default: %(default)s)",
        metavar="",
    )
    parser.add_argument(
        "-v",
        "--variableID",
        type=str,
        default="vv1fbdd4-7b23-11ea-bd6d-005056c0001",
        help="Variable UUID in GI.bench",
        metavar="",
    )
    parser.add_argument(
        "-n",
        "--variableName",
        type=str,
        default="python_variable",
        help="Variable name",
        metavar="",
    )
    parser.add_argument(
        "-u",
        "--Unit",
        type=str,
        default="V",
        help="Unit of the variable (default: %(default)s)",
        metavar="",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    conn_stream = postprocessbuffer_manager.PostProcessBufferManager()

    # Create and initialize buffer with one channel
    conn_stream.create_buffer(args.ID, args.BufferName, args.StreamSampleRate)
    conn_stream.add_channel(args.variableID, args.variableName, args.Unit)
    conn_stream.initialize_buffer()

    timestamp_start_ns = np.uint64(round(time.time() - 946684800) * 1000000000)
    sleep_ms = int(conn_stream.frame_buffer_length / args.StreamSampleRate * 1000)
    ns_per_sample = int(1_000_000_000 / args.StreamSampleRate)
    nanos = np.uint64(timestamp_start_ns)

    try:
        while True:
            for frameindex in range(int(conn_stream.frame_buffer_length)):
                conn_stream.write_timestamp(int(frameindex), int(nanos))
                nanos = np.uint64(nanos + ns_per_sample)
                value = float(np.random.random_sample() * 100.0)
                conn_stream.write_value(int(frameindex), 0, value)
                logging.info(f"Writing payload to buffer: {args.ID}, {value}")
            conn_stream.append_frame_buffer()
            conn_stream.sleep_ms(sleep_ms)
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
    finally:
        try:
            conn_stream.close_buffer()
            logging.info("Buffer closed")
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
