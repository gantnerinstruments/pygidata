"""Read values from a running buffer and optionally plot them.
Currently, you can only read buffers by there UUID - if you need to use Index see example_websocket_stream! - which are mostly GI.bench and Q.core"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from typing import Iterable

import ginsutility.highspeedport as highspeedport
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read values of running buffer and plot them",
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
        "-b", "--buffer_id", type=str, required=True,
        help=(
            "Gi.bench buffer UUID (default: %(default)s). "
            "Ordering depends on whether it is a Gi.bench or controller connection."
        ),
    )
    parser.add_argument(
        "-a", "--address", type=str, default="127.0.0.1", help="IP of the Gantner device",
    )
    parser.add_argument(
        "-v", "--v_idx", type=int, nargs="+", default=[1], metavar="IDX",
        help="Indices of stream variables to display (0 is timestamp).",
    )
    parser.add_argument(
        "-p", "--plot_data", action="store_true", help="Show a live plot using PyQtGraph",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    conn = highspeedport.HighSpeedPortClient()

    try:
        if is_uuid(args.buffer_id):
            conn.init_connection(args.address)
            # conn.init_post_process_buffer_conn(args.buffer_id)
        else:
            conn.buffer_index = int(args.buffer_id)
            conn.init_connection(args.address)

        buffer_gen = conn.yield_buffer()

        if args.plot_data:
            try:
                import numpy as np
                import pyqtgraph as pg
                from pyqtgraph.Qt import QtCore, QtWidgets
            except ImportError:
                logging.error("GUI components not available. Install pyqtgraph.")
                return 1

            num_vars = len(args.v_idx)
            signal = np.zeros((4096, num_vars))
            app = QtWidgets.QApplication(sys.argv)
            win = pg.GraphicsLayoutWidget(title="Time plot")
            win.resize(1000, 600)
            plot_1 = win.addPlot(title="Time plot")
            curves = [plot_1.plot(pen=pg.intColor(i)) for i in range(num_vars)]
            win.show()

            def update():
                nonlocal signal
                readbuffer = next(buffer_gen)
                signal = np.vstack((signal[len(readbuffer):, :], readbuffer[:, args.v_idx]))
                for i, curve in enumerate(curves):
                    curve.setData(y=signal[:, i])

            timer = QtCore.QTimer()
            timer.timeout.connect(update)
            timer.start(10)
            app.exec()
        else:
            try:
                for readbuffer in buffer_gen:
                    if readbuffer.shape[0] > 0:
                        logging.info("Batch shape: %s", getattr(readbuffer, "shape", None))
                        print(readbuffer)
            except KeyboardInterrupt:
                logging.info("Terminated by user.")
    finally:
        try:
            conn.close_connection()
            logging.info("Connection closed")
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
