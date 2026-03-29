from __future__ import annotations

import argparse
import logging
import uuid
from typing import Sequence

import ginsutility.highspeedport as highspeedport
import numpy as np
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Connect to controller and optionally plot data",
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
        "-c",
        "--controller_IP",
        type=str,
        default="127.0.0.1",
        help="IP address of the controller",
    )
    parser.add_argument(
        "-i",
        "--channel_index",
        type=int,
        nargs="+",
        default=[1, 2],
        help="Indices of the channels to retrieve",
    )
    parser.add_argument(
        "-g",
        "--show_gui",
        action="store_true",
        help="Display a live plot using PyQtGraph",
    )
    parser.add_argument(
        "-b",
        "--buffer_index",
        type=str,
        default="0",
        help=(
            "Gi.bench buffer UUID or Controller buffer Index (default: %(default)s). "
            "Ordering depends on whether it is a Gi.bench or controller connection."
        ),
    )
    return parser.parse_args(argv)


def is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def retrieve_data(buffer_gen, channel_nb: Sequence[int], window_size: int = 4096) -> np.ndarray:
    signal_data = np.zeros((window_size, len(channel_nb)))
    batch = next(buffer_gen)
    disp = batch[:, channel_nb]
    dim = len(disp)
    signal_data = np.vstack((signal_data[dim:window_size, :], disp))
    return signal_data


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    controller_ip = args.controller_IP
    channel_nb = args.channel_index
    window_size = 4096

    conn = highspeedport.HighSpeedPortClient()
    try:
        if is_uuid(args.buffer_index):
            conn.init_post_process_buffer_conn(args.buffer_index)
        else:
            conn.bufferindex = int(args.buffer_index)
        conn.init_connection(controller_ip)

        # Basic device info (useful hints)
        try:
            name = conn.read_controller_name()
            sn = conn.read_serial_number()
            sr = conn.read_sample_rate()
            ch_cnt = conn.read_channel_count()
            logging.info("Controller: %s (SN: %s), Sample rate: %s, Channels: %s", name, sn, sr, ch_cnt)
            conn.read_channel_names()
            logging.info("First channel: %s", conn.read_index_name(0))
        except Exception as exc:
            logging.debug("Could not read controller metadata: %s", exc)

        buffer_gen = conn.yield_buffer()

        if args.show_gui:
            try:
                import pyqtgraph as pg
                from pyqtgraph.Qt import QtCore
            except ImportError:
                logging.error("PyQtGraph not available. Install pyqtgraph to enable GUI.")
                return 1

            # Prepare rolling window
            signal_plot = np.zeros((window_size, len(channel_nb)))

            app = pg.mkQApp("Time Signal")
            win = pg.GraphicsLayoutWidget(show=True, title="Time Signal")
            win.resize(1000, 600)
            plot_1 = win.addPlot(title="Time plot")
            curves = [plot_1.plot() for _ in channel_nb]

            def update():
                nonlocal signal_plot
                batch = next(buffer_gen)
                disp = batch[:, channel_nb]
                signal_plot = np.vstack((signal_plot[len(disp):, :], disp))
                for i, curve in enumerate(curves):
                    curve.setData(y=signal_plot[:, i])

            timer = QtCore.QTimer()
            timer.timeout.connect(update)
            timer.start(100)
            app.exec()
        else:
            data = retrieve_data(buffer_gen, channel_nb, window_size)
            logging.info("Retrieved data shape: %s", data.shape)
    finally:
        try:
            conn.close_connection()
            logging.info("Connection closed")
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
