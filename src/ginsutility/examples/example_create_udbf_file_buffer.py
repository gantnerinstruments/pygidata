"""Create a UDBF buffer, add a channel, and stream random values into it.
Hint: this buffer is only under "Records" not "Streams" !"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Iterable

import ginsutility.postprocessbuffer as postprocessbuffer_manager
import numpy as np
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_args(argv: Iterable[str] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a UDBF buffer from UDBF file",
        formatter_class=CustomHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "-h", "--help", action="help", default=argparse.SUPPRESS,
        help="Show this help message and exit. All arguments are optional; strings do not need quoting.",
    )
    parser.add_argument(
        "-f", "--FilePath", type=str,
        default=r"C:\Users\Public\Documents\Gantner Instruments\GI.bench\data\records",
        help="Full directory path for the UDBF file",
    )
    parser.add_argument("-i", "--SourceID", type=str, default="0a929f60-9536-11f0-b59d-2c58b9196a11",
                        help="Source ID (UUID)")
    parser.add_argument("-n", "--SourceName", type=str, default="python_udbf_bufferT", help="Source name")
    parser.add_argument("-r", "--SampleRateHz", type=float, default=10.0, help="Sample rate in Hz")
    parser.add_argument("-m", "--MaxLengthSeconds", type=int, default=3600, help="Max buffer length in seconds")
    parser.add_argument("-o", "--Options", type=int, default=0, help="Options bitmask")
    parser.add_argument("-vid", "--VariableId", type=str, default="vv1fbdd4-7b23-11ea-bd6d-005056c00001",
                        help="Variable ID")
    parser.add_argument("-vn", "--VariableName", type=str, default="python_udbf", help="Variable name")
    parser.add_argument("-u", "--Unit", type=str, default="V", help="Unit of measurement")
    parser.add_argument("-l", "--FrameBufferLength", type=int, default=10,
                        help="How many samples per frame ingested into buffer")
    return parser.parse_args(argv)


def main(argv: Iterable[str] = None) -> int:
    args = parse_args(argv)

    conn = postprocessbuffer_manager.PostProcessBufferManager()

    result = conn.create_udbf_file_buffer(
        args.FilePath,
        args.SourceID,
        args.SourceName,
        float(args.SampleRateHz),
        int(args.MaxLengthSeconds),
        int(args.Options),
    )
    if not result:
        logging.error("Failed to create UDBF buffer (error code %s)", result)
        return 1
    logging.info("UDBF buffer created")

    # Add channel and initialize
    conn.add_channel(args.VariableId, args.VariableName, args.Unit)
    conn.initialize_buffer(args.FrameBufferLength)

    logging.info(
        f"UDBF buffer under Records initialized with params: SourceName:{args.SourceName}, SourceID:{args.SourceID}")

    # Stream random data
    timestamp_start_ns = np.uint64(round(time.time() - 946684800) * 1_000_000_000)
    sleep_ms = int(conn.frame_buffer_length / float(args.SampleRateHz) * 1000)
    ns_per_sample = int(1_000_000_000 / float(args.SampleRateHz))
    nanos = np.uint64(timestamp_start_ns)

    try:
        while True:
            for frameindex in range(int(conn.frame_buffer_length)):
                conn.write_timestamp(int(frameindex), int(nanos))
                nanos = np.uint64(nanos + ns_per_sample)
                value = float(np.random.random_sample() * 100.0)
                conn.write_value(int(frameindex), 0, value)
            conn.append_frame_buffer()
            logging.info("wrote %d frames", int(conn.frame_buffer_length))
            conn.sleep_ms(sleep_ms)
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
    finally:
        try:
            conn.close_buffer()
            logging.info("Buffer closed")
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
