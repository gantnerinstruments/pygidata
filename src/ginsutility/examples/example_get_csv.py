"""Read a CSV file and plot it using PyQtGraph (optional)."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot data of a CSV file",
        formatter_class=CustomHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "-h", "--help", action="help", default=argparse.SUPPRESS,
        help=(
            "Show this help message and exit. All arguments are optional; "
            "strings do not need to be quoted."
        ),
    )
    parser.add_argument(
        "-f", "--file_name", type=str, default="data.csv", metavar="",
        help="File name under ./csv (default: %(default)s)",
    )
    parser.add_argument(
        "--no-plot", action="store_true", help="Do not open the plot window; only validate data.",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    csv_file_path = Path(__file__).parent / "csv" / args.file_name
    if not csv_file_path.exists():
        logging.error("CSV file not found: %s", csv_file_path)
        return 1

    data = pd.read_csv(csv_file_path)
    if data.shape[1] < 2:
        logging.error("CSV must contain at least two columns: timestamp and value(s)")
        return 1

    col0 = pd.to_numeric(data.iloc[:, 0], errors="coerce")
    if col0.isna().all():
        logging.error("Timestamp column contains no numeric values")
        return 1

    # Scale timestamps if extremely large to aid plotting
    scale = 1e9 if col0.abs().max() > 1e12 else 1.0
    timestamps = (col0 / scale).to_numpy()
    if scale != 1.0:
        logging.info("Timestamps divided by 1e9 for plotting")

    values = data.iloc[:, 1:]
    logging.info("Data shape: %s", values.shape)

    if args.no_plot:
        logging.info("no-plot set; exiting after validation")
        return 0

    try:
        import pyqtgraph as pg
        from pyqtgraph.Qt import QtCore, QtGui
    except ImportError:
        logging.error("PyQtGraph not available. Install pyqtgraph to enable plotting.")
        return 1

    signal = np.zeros((len(values), len(values.columns)))

    win = pg.GraphicsLayoutWidget(title="CSV plotting example")
    win.resize(1000, 600)
    plot = win.addPlot(title="Time plot")

    curves = [plot.plot(pen=pg.intColor(i)) for i in range(len(values.columns))]

    disp = values.to_numpy()
    signal[: len(disp), :] = disp
    for row, curve in enumerate(curves):
        curve.setData(x=timestamps, y=signal[:, row])

    win.show()

    if __name__ == "__main__":
        if (sys.flags.interactive != 1) or not hasattr(QtCore, "PYQT_VERSION"):
            QtGui.QGuiApplication.instance().exec()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
