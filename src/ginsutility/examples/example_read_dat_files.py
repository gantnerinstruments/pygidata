"""Read a Gantner Instruments .dat file, compute basic stats, log results, and save a plot.

This example loads a GI binary data file into a NumPy array through the Q.station
connector, prints metadata, computes min/max of a user-selected channel,
writes a small log file, and saves a PNG plot of that channel over time.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from pathlib import Path
from typing import Iterable

# Use the Windows connector. For Linux, use the Linux variant of this module.
import ginsutility.highspeedport as highspeedport
import matplotlib.dates as mpdt
import numpy as np
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter
from matplotlib import pyplot as plt

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def ole2datetime(oledt: float) -> dt.datetime:
    """Convert an OLE Automation date (days since 1899-12-30) to datetime."""
    base = dt.datetime(1899, 12, 30)
    return base + dt.timedelta(days=float(oledt))


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read a .dat file and write results/plot",
        formatter_class=CustomHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help=(
            "Show this help message and exit. All arguments are optional;"
            " strings do not need to be quoted."
        ),
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        default="GinsDataloggerReadDatFiles.dat",
        help="UDDF/Dat file name to import (default: %(default)s)",
        metavar="",
    )
    parser.add_argument(
        "-t",
        "--timestamp_index",
        type=int,
        default=0,
        help="Index of the timestamp channel (default: %(default)s)",
        metavar="",
    )
    parser.add_argument(
        "-i",
        "--channel_index",
        type=int,
        default=1,
        help="Index of the data channel to analyze (default: %(default)s)",
        metavar="",
    )
    return parser.parse_args(argv)


def read_dat(
        path_udbf: Path,
        timestamp_index: int,
        channel_index: int,
):
    """Open the .dat with Q.station connector and return data and metadata.

    Returns
    -------
    dat_file: np.ndarray
        The data matrix.
    time_name: str
        Name of the timestamp channel.
    time_unit: str
        Unit of the timestamp.
    var_name: str
        Name of the selected channel.
    var_unit: str
        Unit of the selected channel.
    conn: object
        The open connector (caller must close).
    """
    logging.info("Initializing Gantner Instruments file connection")
    conn = highspeedport.HighSpeedPortClient()
    conn.init_file(str(path_udbf))

    logging.info("Reading channel names")
    conn.read_channel_names()

    max_channel = conn.read_channel_count()
    f_samples = conn.read_sample_rate()
    logging.info("Channels: %s, Sample rate: %s", max_channel, f_samples)

    time_name = conn.read_index_name(timestamp_index)
    time_unit = conn.read_index_unit(timestamp_index)
    var_name = conn.read_index_name(channel_index)
    var_unit = conn.read_index_unit(channel_index)
    logging.info("Time channel: %s [%s]", time_name, time_unit)
    logging.info("Selected channel: %s [%s]", var_name, var_unit)

    try:
        dat_file = highspeedport.read_gins_dat_file(conn)
        logging.info("Data imported as NumPy array: dat[row, col]")
    except Exception as exc:
        conn.close_connection()
        raise RuntimeError("File could not be imported") from exc

    return dat_file, time_name, time_unit, var_name, var_unit, conn


def write_results(res_dir: Path, now: dt.datetime, min_val: float, max_val: float, unit: str, var_name: str) -> Path:
    res_dir.mkdir(parents=True, exist_ok=True)
    res_path = res_dir / "resu.txt"

    timestamp = now.strftime("%d.%m.%Y %H:%M")

    if not res_path.exists():
        res_path.write_text(f"journal created on: {timestamp}\n", encoding="utf-8")

    line_block = (
        "**********************\n"
        f"1;Journal update;{timestamp}\n"
        f"2;Minimum {var_name};{min_val}; [{unit}]\n"
        f"3;Maximum {var_name};{max_val}; [{unit}]\n"
    )
    with res_path.open("a", encoding="utf-8", errors="replace") as f:
        f.write(line_block)

    logging.info("Results written to %s", res_path)
    return res_path


def plot_channel(
        dat_file: np.ndarray,
        timestamp_index: int,
        channel_index: int,
        var_name: str,
        var_unit: str,
        out_path: Path,
) -> Path:
    dates = [ole2datetime(x) for x in dat_file[:, timestamp_index]]
    date_num = mpdt.date2num(dates)

    fig, ax = plt.subplots()
    ax.xaxis_date()
    ax.fmt_xdata = mpdt.DateFormatter("%d-%m %H:%M:%S")
    fig.autofmt_xdate()

    ylabel = f"{var_name} [{var_unit}]" if var_unit else var_name
    ax.set_ylabel(ylabel, fontsize=10)
    ax.plot(
        date_num,
        dat_file[:, channel_index],
        color="green",
        linewidth=1,
        label=ylabel,
    )
    ax.legend(bbox_to_anchor=(1.01, 1), loc=2, borderaxespad=0.0, prop={"size": 5})

    out_path = out_path.with_suffix(".png")
    fig.savefig(str(out_path), format="png", dpi=500)
    plt.close(fig)
    logging.info("Plot saved to %s", out_path)
    return out_path


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    script_dir = Path(__file__).parent
    path_udbf = script_dir / args.file

    dat_file, time_name, time_unit, var_name, var_unit, conn = read_dat(
        path_udbf, args.timestamp_index, args.channel_index
    )

    try:
        if dat_file.size == 0 or dat_file.shape[0] == 0:
            logging.error("No data found in the .dat file.")
            return 1
        logging.info("Rows: %d, Columns: %d", dat_file.shape[0], dat_file.shape[1])
        init_time = dat_file[0, args.timestamp_index]
        init_val = dat_file[0, args.channel_index]
        logging.info(
            "At init time %s, %s is %s %s",
            init_time,
            var_name,
            init_val,
            var_unit,
        )

        val_col = dat_file[:, args.channel_index]
        if not np.isfinite(val_col).any():
            logging.error("Selected channel contains no finite values.")
            return 1
        min_val = float(np.nanmin(val_col))
        max_val = float(np.nanmax(val_col))
        logging.info("Min %s: %s %s", var_name, min_val, var_unit)
        logging.info("Max %s: %s %s", var_name, max_val, var_unit)

        now = dt.datetime.now()
        res_dir = script_dir / "results"
        write_results(res_dir, now, min_val, max_val, var_unit, var_name)

        plot_channel(
            dat_file,
            args.timestamp_index,
            args.channel_index,
            var_name,
            var_unit,
            res_dir / "Channel",
        )
    finally:
        conn.close_connection()
        logging.info("Connection closed")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
