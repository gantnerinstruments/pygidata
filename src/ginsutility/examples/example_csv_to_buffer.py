"""Read a CSV file and load values into a buffer."""
from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path
from typing import Iterable

import ginsutility.postprocessbuffer as postprocessbuffer_manager
import numpy as np
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read a .csv file and load values into buffer",
        formatter_class=CustomHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "-h", "--help", action="help", default=argparse.SUPPRESS,
        help=(
            "Show this help message and exit. All arguments are optional; "
            "strings do not need to be quoted. YT-Chart must be in 'interval' mode; "
            "refresh the chart to reset the y-axis."
        ),
    )
    parser.add_argument("-i", "--ID", type=str, default="ff1fbdd4-7b23-11ea-bd6d-005056c00002", metavar="",
                        help="Buffer UUID in GI.bench")
    parser.add_argument("-b", "--BufferName", type=str, default="PythonBuffer2", metavar="",
                        help="Name of the buffer")
    parser.add_argument("-s", "--StreamSampleRate", type=int, default=10, metavar="",
                        help="Sampling rate in Hz")
    parser.add_argument("-v", "--variableID", type=str, default="vv1fbdd4-7b23-11ea-bd6d-005056c0002", metavar="",
                        help="Variable UUID in GI.bench")
    parser.add_argument("-n", "--variableName", type=str, default="python_variable2", metavar="",
                        help="Variable name")
    parser.add_argument("-u", "--Unit", type=str, default="V", metavar="",
                        help="Unit of the variable")
    parser.add_argument("-f", "--fileName", type=str, default="data.csv", metavar="",
                        help="Name of the CSV file (under ./csv)")
    parser.add_argument("-p", "--progress_report", type=int, default=0, metavar="",
                        help="Log progress every N values (0 disables)")
    return parser.parse_args(argv)


def read_csv_data(path: Path) -> tuple[list[np.uint64], list[float]]:
    timestamps: list[np.uint64] = []
    data: list[float] = []
    with path.open("r", encoding="utf-8") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if len(row) < 2:
                continue
            try:
                ts = np.uint64(float(row[0]))
                val = float(row[1])
            except (ValueError, TypeError):
                continue
            timestamps.append(ts)
            data.append(val)
    return timestamps, data


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    conn_stream = postprocessbuffer_manager.PostProcessBufferManager()

    # Re-create buffer to clear old values
    # Create buffer again
    conn_stream.create_buffer(args.ID, args.BufferName, args.StreamSampleRate)
    conn_stream.add_channel(args.variableID, args.variableName, args.Unit)
    conn_stream.initialize_buffer()

    logging.info(f"Buffer {args.ID} initialized, it's only alive as long as values are written.")

    file_path = Path(__file__).parent / "csv" / args.fileName
    if not file_path.exists():
        logging.error("CSV file not found: %s", file_path)
        return 1

    timestamps, data = read_csv_data(file_path)
    if not timestamps or not data:
        logging.error("No valid rows found in CSV: %s", file_path)
        return 1

    nanos = np.uint64(timestamps[0])
    sleep_ms = int(conn_stream.frame_buffer_length / args.StreamSampleRate * 1000)
    time_diff = np.uint64(timestamps[-1] - timestamps[0])

    data_index = 0
    data_len = len(data)
    report_n = int(args.progress_report)

    logging.info("Writing...")

    try:
        while data_index < data_len:
            for frameindex in range(int(conn_stream.frame_buffer_length)):
                if data_index < data_len:
                    nanos = np.uint64(timestamps[data_index])
                    value = float(data[data_index])
                    data_index += 1
                else:
                    nanos = np.uint64(nanos + time_diff)
                    value = 0.0

                conn_stream.write_timestamp(int(frameindex), int(nanos))
                if report_n and data_index and (data_index % report_n == 0):
                    logging.info("Progress: %d/%d", data_index, data_len)
                conn_stream.write_value(int(frameindex), 0, value)

            conn_stream.append_frame_buffer()
            conn_stream.sleep_ms(sleep_ms)

    finally:
        try:
            conn_stream.close_buffer()
            logging.info("Data loading completed and buffer closed.")
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
