"""Read a buffer stream from a Q.station and write rows to a CSV file.

Hints for users:
- You can select the buffer by UUID (Gi.bench) or by controller buffer index.
- Use --controller-IP to target a remote controller; default is localhost.
"""
from __future__ import annotations

import argparse
import logging
import uuid
from pathlib import Path

import ginsutility.postprocessbuffer as postprocessbuffer_client
import ginsutility.highspeedport as highspeedport
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def list_available_buffers() -> None:
    """Log available buffers from the local buffer server (if any)."""
    try:
        conn_get_stream = postprocessbuffer_client.PostProcessBufferManager()
        count_buffer = conn_get_stream.get_buffer_count()
        if count_buffer <= 0:
            logging.info("No buffer found. Check in GI.bench your active buffer.")
            return
        logging.info("Available buffers: %d", count_buffer)
        for idx in range(count_buffer):
            name, buf_id = conn_get_stream.get(int(idx))
            logging.info("%d: %s (%s)", idx, name, buf_id)
    except Exception as exc:
        logging.debug("Buffer listing failed: %s", exc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read values of a buffer and save them into a .csv file",
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
        "-b",
        "--buffer_index",
        type=str,
        default="0",
        help=(
            "Gi.bench buffer UUID or controller buffer index (default: %(default)s). "
            "Ordering depends on whether it is a Gi.bench or controller connection."
        ),
    )
    parser.add_argument(
        "-c",
        "--controller_IP",
        type=str,
        default="127.0.0.1",
        help="IP address of the controller",
    )
    parser.add_argument(
        "-f",
        "--file_name",
        type=str,
        default="data.csv",
        metavar="",
        help='Name of the CSV file to write data to (default: "%(default)s")',
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    list_available_buffers()

    conn = highspeedport.HighSpeedPortClient()
    try:
        if is_uuid(args.buffer_index):
            conn.init_post_process_buffer_conn(args.buffer_index)
        else:
            conn.buffer_index = int(args.buffer_index)
        conn.init_connection(args.controller_IP)

        # Read stream info (optional but helpful)
        try:
            sr = conn.read_sample_rate()
            ch_cnt = conn.read_channel_count()
            logging.info("Sample rate: %s, Channels: %s", sr, ch_cnt)
            conn.read_channel_names()
            conn.get_channel_info(0)
        except Exception as exc:
            logging.debug("Could not read stream metadata: %s", exc)

        # Prepare CSV output
        out_dir = Path(__file__).parent / "csv"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / args.file_name
        logging.info("Writing CSV to %s", out_path)

        with out_path.open("w", encoding="utf-8", newline="") as f:
            generator = conn.yield_buffer()
            try:
                while True:
                    batch = next(generator)
                    for row in batch:
                        f.write(",".join(str(x) for x in row) + "\n")
                    if len(batch) > 0:
                        logging.info("Wrote %d rows to file", len(batch))
            except KeyboardInterrupt:
                logging.info("Interrupted by user. Stopping stream read.")
    finally:
        try:
            conn.close_connection()
            logging.info("Connection closed")
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
