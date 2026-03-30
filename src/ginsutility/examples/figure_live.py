"""
Standalone Plotly Buffer App
Reads data from a GI.bench/Q.station buffer and visualizes it with Plotly.
"""

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import threading
import numpy as np
import time
import logging

import ginsutility.highspeedport as highspeedport


# --------------------
# Buffer Reader
# --------------------
class BufferReader(threading.Thread):
    def __init__(self, address: str, buffer_id: str, v_idx: list[int]):
        super().__init__(daemon=True)
        self.address = address
        self.buffer_id = buffer_id
        self.v_idx = v_idx
        self.data = []  # will store rows from buffer
        self.running = True

    def run(self):
        conn = highspeedport.HighSpeedPortClient()
        conn.init_connection(self.address)
        conn.init_post_process_buffer_conn(self.buffer_id)
        buffer_gen = conn.yield_buffer()

        try:
            while self.running:
                try:
                    readbuffer = next(buffer_gen)
                    if readbuffer.shape[0] > 0:
                        # store selected columns
                        self.data.extend(readbuffer[:, self.v_idx].tolist())
                        # keep only last N rows
                        if len(self.data) > 5000:
                            self.data = self.data[-5000:]
                except StopIteration:
                    break
                except Exception as e:
                    logging.error("Buffer read error: %s", e)
                    time.sleep(0.01)
        finally:
            conn.close_connection()

    def stop(self):
        self.running = False


# --------------------
# Start Buffer Thread
# --------------------
BUFFER_ID = "7b0e0352-97ab-11f0-b1d9-2c58b9196ae8"  # replace with your buffer UUID
ADDRESS = "127.0.0.1"
V_IDX = [1, 2, 3, 4]  # variable indices to visualize (e.g. 1=temp, 2=pressure)

buffer_reader = BufferReader(ADDRESS, BUFFER_ID, V_IDX)
buffer_reader.start()

# --------------------
# Dash App
# --------------------
app = dash.Dash(
    __name__,
    external_stylesheets=["https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"],
)
app.title = "Plotly Buffer App"

app.layout = html.Div(
    [
        html.H3("Live Buffer Data", className="text-center mt-3"),
        dcc.Graph(id="buffer-plot", style={"height": "600px"}),
        dcc.Interval(id="interval-update", interval=2000, n_intervals=0),  # update every 2s
    ],
    style={"margin": "20px"},
)


@app.callback(
    Output("buffer-plot", "figure"),
    Input("interval-update", "n_intervals"),
)
def update_plot(_):
    data = np.array(buffer_reader.data)

    fig = go.Figure()
    if data.size > 0:
        x = list(range(len(data)))
        for i in range(data.shape[1]):
            fig.add_trace(go.Scatter(x=x, y=data[:, i], mode="lines", name=f"Var {V_IDX[i]}"))

    fig.update_layout(
        title="Live Buffer Data",
        xaxis_title="Samples",
        yaxis_title="Values",
        template="plotly_white",
        hovermode="x unified",
    )
    return fig


if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=8061, debug=False)
    finally:
        buffer_reader.stop()
