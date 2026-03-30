"""
Advanced Analysis page for the Microphone Demo:
- Adds Python-only features: A-weighting, 1/3-octave bars, spectrogram, peak table, THD/SNR, band RMS,
  compare overlay, and CSV export.
- The app can be launched standalone and integrated into GI.bench as a dynamic link.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import List, Literal, Protocol, Tuple, Optional

import numpy as np
from dash import Dash, dcc, html, Input, Output, State, no_update
from dash.dash_table import DataTable
import plotly.graph_objects as go
from ginsutility import highspeedport

SignalScale = Literal["Amplitude", "dBFS"]
WindowKind = Literal["rectangular", "hann", "hamming", "blackman"]


class DataProvider(Protocol):
    def list_channels(self) -> List[str]: ...

    def get_window(self, channel: str, duration_s: float, fs: int) -> Tuple[np.ndarray, np.ndarray]: ...


class _DummyProvider:
    def __init__(self): self.rng = np.random.default_rng(7)

    def list_channels(self) -> List[str]: return ["Mic_Channel1", "Mic_Channel2"]

    def get_window(self, channel: str, duration_s: float, fs: int) -> Tuple[np.ndarray, np.ndarray]:
        n = max(2048, int(fs * duration_s))
        t = np.arange(n) / fs
        base = 0.5 * np.sin(2 * np.pi * 440 * t) + 0.22 * np.sin(2 * np.pi * 1200 * t)
        if channel.endswith("2"): base = 0.42 * np.sin(2 * np.pi * 1000 * t) + 0.18 * np.sin(2 * np.pi * 2500 * t)
        x = np.clip(base + 0.07 * self.rng.standard_normal(n), -1.0, 1.0)
        return t, x.astype(np.float32, copy=False)


_provider: DataProvider = _DummyProvider()


def set_data_provider(provider: DataProvider) -> None:
    global _provider
    _provider = provider


class GiBufferProvider(DataProvider):
    def __init__(
            self,
            address: str,
            buffer_uuid: str,
            var_idx_map: dict[str, int] = None,
            fs_hint: int | None = None,
            max_seconds: float = 10.0,
    ):
        self.address = address
        self.buffer_uuid = buffer_uuid
        self.var_idx_map = var_idx_map or {"Mic_Channel1": 1, "Mic_Channel2": 2}  # 0 is timestamp
        self.fs_hint = fs_hint
        self._conn = highspeedport.HighSpeedPortClient()
        self._conn.init_connection(self.address)
        self._conn.init_post_process_buffer_conn(self.buffer_uuid)
        self._gen = self._conn.yield_buffer()
        self._ring = {name: deque(maxlen=int((fs_hint or Defaults.fs) * max_seconds)) for name in self.var_idx_map}
        self._fs = fs_hint or Defaults.fs

    def list_channels(self) -> List[str]:
        return list(self.var_idx_map.keys())

    def _pump_latest(self, n: int) -> None:
        """Continuously read newest buffer data, keeping only the last n samples."""
        try:
            rb = next(self._gen)
            if rb.shape[0] == 0:
                return
            for name, col in self.var_idx_map.items():
                self._ring[name].extend(rb[:, col].tolist())
        except StopIteration:
            pass

    def get_window(self, channel: str, duration_s: float, fs: int) -> Tuple[np.ndarray, np.ndarray]:
        self._fs = int(fs or self._fs)
        cap = max(int(self._fs * 10.0), 1)
        for k in self._ring:
            if self._ring[k].maxlen != cap:
                self._ring[k] = deque(list(self._ring[k])[-cap:], maxlen=cap)

        self._pump_latest(cap)

        n = max(1, int(self._fs * float(duration_s)))
        x = np.asarray(list(self._ring[channel])[-n:], dtype=np.float32)
        t = np.arange(x.size, dtype=np.float32) / float(self._fs)
        return t, x

    # def close(self) -> None:
    #    try:
    #        self._conn.close_connection()
    #    except Exception:
    #        pass


# -------- Defaults / IDs --------

@dataclass(frozen=True)
class Defaults:
    fs: int = 48_000
    duration_s: float = 1.0
    nfft: int = 1024
    window: WindowKind = "hann"
    scale: SignalScale = "dBFS"
    overlap: int = 50
    peaks: int = 1
    band_lo: float = 100.0
    band_hi: float = 3000.0
    a_weight: bool = True
    logx: bool = False


def _id(p: str, s: str) -> str: return f"{p}-{s}"


# -------- DSP helpers --------

def _window(kind: WindowKind, n: int) -> np.ndarray:
    if kind == "hann": return np.hanning(n)
    if kind == "hamming": return np.hamming(n)
    if kind == "blackman": return np.blackman(n)
    return np.ones(n)


def _a_weighting(f: np.ndarray) -> np.ndarray:
    # IEC 61672 A-weighting magnitude (linear). f in Hz.
    f2 = np.square(f, dtype=np.float64)
    ra_num = (12200 ** 2) * (f2 ** 2)
    ra_den = (f2 + 20.6 ** 2) * np.sqrt((f2 + 107.7 ** 2) * (f2 + 737.9 ** 2)) * (f2 + 12200 ** 2)
    ra = np.where(f > 0, ra_num / ra_den, 0.0)
    a_db = 2.0 + 20.0 * np.log10(np.maximum(ra, 1e-24))
    return np.power(10.0, a_db / 20.0)


def _rfft_single_sided(x: np.ndarray, fs: int, nfft: int, window: WindowKind,
                       scale: SignalScale, a_weight: bool) -> Tuple[np.ndarray, np.ndarray]:
    n = x.size
    w = _window(window, n)
    cg = w.mean()
    X = np.fft.rfft(x * w, n=nfft)
    f = np.fft.rfftfreq(nfft, 1.0 / fs)
    mag = np.abs(X) / (n * cg)
    if nfft % 2 == 0:
        mag[1:-1] *= 2.0
    else:
        mag[1:] *= 2.0
    if a_weight:
        mag *= _a_weighting(f)
    if scale == "dBFS":
        mag = 20.0 * np.log10(np.maximum(mag, 1e-12))
    return f, mag


def _stft(x: np.ndarray, fs: int, nfft: int, overlap_pct: int,
          window: WindowKind, scale: SignalScale, a_weight: bool):
    step = max(1, int(nfft * (1 - overlap_pct / 100.0)))
    step = min(step, max(1, nfft // 2))
    w = _window(window, nfft)
    cg = w.mean()
    frames = []
    for start in range(0, max(0, x.size - nfft + 1), step):
        seg = x[start:start + nfft]
        if seg.size < nfft: break
        X = np.fft.rfft(seg * w, n=nfft)
        mag = np.abs(X) / (nfft * cg)
        if nfft % 2 == 0:
            mag[1:-1] *= 2.0
        else:
            mag[1:] *= 2.0
        frames.append(mag)
    if not frames:
        f = np.fft.rfftfreq(nfft, 1.0 / fs)
        return f, np.empty((0, f.size)), np.array([])
    S = np.vstack(frames)
    f = np.fft.rfftfreq(nfft, 1.0 / fs)
    if a_weight: S *= _a_weighting(f)[None, :]
    if scale == "dBFS": S = 20.0 * np.log10(np.maximum(S, 1e-12))
    t_frames = (np.arange(S.shape[0]) * step + nfft / 2) / fs
    return f, S, t_frames


def _peak_pick(f: np.ndarray, y: np.ndarray, k: int) -> Tuple[np.ndarray, np.ndarray]:
    if y.size == 0: return np.array([]), np.array([])
    # ignore DC
    f, y = f[1:], y[1:]
    idx = np.argpartition(y, -k)[-k:]
    idx = idx[np.argsort(-y[idx])]
    return f[idx], y[idx]


def _thd_snr(f: np.ndarray, y: np.ndarray, scale: SignalScale) -> Tuple[
    Optional[float], Optional[float], Optional[float]]:
    if y.size < 5: return None, None, None
    fi, yi = _peak_pick(f, y, 1)
    if fi.size == 0: return None, None, None
    f0 = fi[0]
    if scale == "dBFS":
        a = np.power(10.0, y / 20.0)
        a0 = np.power(10.0, yi[0] / 20.0)
    else:
        a = y
        a0 = yi[0]
    nyq = f[-1]
    harms = []
    for h in (2, 3, 4, 5):
        fh = h * f0
        if fh >= nyq: break
        idx = np.argmin(np.abs(f - fh))
        harms.append(a[idx])
    if not harms: return None, None, None
    thd = np.sqrt(np.sum(np.square(harms))) / max(a0, 1e-12)
    # crude noise estimate: exclude +/- 3 bins around fundamental and its first 5 harmonics
    mask = np.ones_like(a, dtype=bool)
    bins = [np.argmin(np.abs(f - (n * f0))) for n in range(1, 6) if (n * f0) < nyq]
    for b in bins:
        lo = max(0, b - 3);
        hi = min(a.size, b + 4)
        mask[lo:hi] = False
    noise_rms = np.sqrt(np.mean(np.square(a[mask]))) if np.any(mask) else 0.0
    snr = a0 / max(noise_rms, 1e-12)
    if scale == "dBFS":
        thd = 20 * np.log10(max(thd, 1e-12))
        snr = 20 * np.log10(max(snr, 1e-12))
    return float(f0), float(thd), float(snr)


def _octave_bands(f: np.ndarray, y: np.ndarray, scale: SignalScale, third: bool = True):
    # 1/3-octave IEC centers from ~20 Hz to ~20 kHz
    centers = []
    base = 1000.0
    k_min, k_max = -30, 20
    for k in range(k_min, k_max + 1):
        centers.append(base * (2 ** (k / (3 if third else 1))))
    centers = np.array([c for c in centers if 20 <= c <= 20000])
    # bounds
    bw = (2 ** (1 / 6)) if third else (2 ** (1 / 2))
    lows, highs = centers / bw, centers * bw
    vals = []
    for lo, hi in zip(lows, highs):
        m = (f >= lo) & (f <= hi)
        if not np.any(m): vals.append(np.nan); continue
        if scale == "dBFS":
            a = np.power(10.0, y[m] / 20.0)
            v = 20 * np.log10(max(np.sqrt(np.sum(a * a) / 2.0), 1e-12))
        else:
            a = y[m]
            v = float(np.sqrt(np.sum(a * a) / 2.0))
        vals.append(v)
    return centers, np.array(vals)


def _compute_metrics(x: np.ndarray, f: np.ndarray, y: np.ndarray, scale: SignalScale):
    """Return a dict of key metrics for the summary table."""
    rms = float(np.sqrt(np.mean(np.square(x))))
    peak = float(np.max(np.abs(x)))
    crest = peak / max(rms, 1e-12)

    # Spectral metrics
    centroid = float(np.sum(f * np.abs(y)) / max(np.sum(np.abs(y)), 1e-12))
    if scale == "dBFS":
        p5, p95 = np.percentile(y, [5, 95])
        dynrange = float(p95 - p5)
    else:
        p5, p95 = np.percentile(np.abs(y), [5, 95])
        dynrange = 20 * np.log10(max(p95 / max(p5, 1e-12), 1e-12))

    f0, thd, snr = _thd_snr(f, y, scale)

    return {
        "RMS": f"{rms:.4f}",
        "Peak": f"{peak:.4f}",
        "Crest Factor": f"{crest:.2f}",
        "Fundamental [Hz]": f"{f0:.1f}" if f0 else "-",
        "THD": f"{thd:.2f} dB" if thd is not None else "-",
        "SNR": f"{snr:.1f} dB" if snr is not None else "-",
        "Spectral Centroid [Hz]": f"{centroid:.1f}",
        "Dynamic Range [dB]": f"{dynrange:.2f}",
    }


# -------- Figures --------

def _time_fig(t: np.ndarray, x: np.ndarray) -> go.Figure:
    if t.size > 6000:
        s = int(np.ceil(t.size / 6000))
        t, x = t[::s], x[::s]

    mean_val = np.mean(x)
    peak_val = np.max(np.abs(x))
    rms_val = np.sqrt(np.mean(x ** 2))

    fig = go.Figure(go.Scatter(x=t, y=x, mode="lines", name="x(t)"))

    # Add mean and RMS indicators
    fig.add_hline(y=mean_val, line_dash="dot", line_color="green", annotation_text=f"Mean={mean_val:.3g}")
    fig.add_hline(y=rms_val, line_dash="dot", line_color="orange", annotation_text=f"RMS={rms_val:.3g}")
    fig.add_hline(y=-rms_val, line_dash="dot", line_color="orange", annotation_text=f"-RMS")

    fig.update_layout(
        template="plotly_white",
        height=320,
        autosize=True,
        margin=dict(l=40, r=20, t=32, b=40),
        xaxis_title="Time [s]",
        yaxis_title="Amplitude",
        legend=dict(x=0.01, y=0.99),
    )
    return fig


def _fft_fig(
        f: np.ndarray, y: np.ndarray, scale: SignalScale, logx: bool,
        f_cmp: Optional[np.ndarray] = None, y_cmp: Optional[np.ndarray] = None, label_cmp: Optional[str] = None,
        y_delta: Optional[np.ndarray] = None
) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=f, y=y, mode="lines", name="|X(f)|"))

    if f_cmp is not None and y_cmp is not None:
        fig.add_trace(go.Scatter(x=f_cmp, y=y_cmp, mode="lines", name=f"|X(f)| ({label_cmp})", opacity=0.6))

    if y_delta is not None:
        fig.add_trace(go.Scatter(x=f, y=y_delta, mode="lines", name="Δ |X(f)|", line=dict(dash="dot", color="red")))

    idx_peak = np.argmax(y)
    fig.add_vline(
        x=f[idx_peak],
        line_dash="dot",
        line_color="red",
        annotation_text=f"Peak {f[idx_peak]:.1f} Hz",
    )

    fig.update_layout(
        template="plotly_white",
        height=340,
        autosize=True,
        margin=dict(l=40, r=20, t=32, b=40),
        xaxis_title="Frequency [Hz]",
        yaxis_title=("Magnitude [dBFS]" if scale == "dBFS" else "Amplitude"),
        legend=dict(x=0.01, y=0.99, xanchor="right", yanchor="top"),
    )
    if logx:
        fig.update_xaxes(type="log", dtick=1)
    return fig


def _spec_fig(f: np.ndarray, S: np.ndarray, times: np.ndarray, scale: SignalScale) -> go.Figure:
    # Estimate max bin for indicator line
    avg_spectrum = np.mean(S, axis=0)
    dominant_idx = np.argmax(avg_spectrum)
    dominant_freq = f[dominant_idx]

    fig = go.Figure(
        go.Heatmap(z=S.T, x=times, y=f, colorbar=dict(title=("dBFS" if scale == "dBFS" else "Amp")))
    )
    # Add dominant frequency indicator
    fig.add_hline(
        y=dominant_freq,
        line_dash="dot",
        line_color="red",
        annotation_text=f"Dominant {dominant_freq:.1f} Hz",
    )

    fig.update_layout(
        template="plotly_white",
        height=420,
        autosize=False,
        margin=dict(l=40, r=20, t=32, b=40),
        xaxis_title="Time [s]",
        yaxis_title="Frequency [Hz]",
    )
    return fig


def _oct_fig(c: np.ndarray, v: np.ndarray, scale: SignalScale) -> go.Figure:
    fig = go.Figure(go.Bar(x=c, y=v))
    fig.update_layout(template="plotly_white", height=300, autosize=False, margin=dict(l=40, r=20, t=32, b=40),
                      xaxis_title="Center frequency [Hz]",
                      yaxis_title=("Level [dBFS]" if scale == "dBFS" else "RMS Amp"))
    fig.update_xaxes(type="log", dtick=1)
    return fig


# -------- Layout --------

def layout(prefix: str = "adv") -> html.Div:
    chs = _provider.list_channels()
    return html.Div(
        [
            dcc.Interval(
                id=_id(prefix, "interval"),
                interval=5000,
                n_intervals=0,
                disabled=True  # initial
            ),
            html.Div(
                "Advanced Analysis: adds A-weighting, 1/3-octave, spectrogram, peak/THD/SNR, overlay, and reproducible exports.",
                style={"fontSize": "1.25rem", "opacity": 0.9, "marginBottom": "10px"},
            ),
            html.Div(
                [
                    html.Div([
                        html.Button("Refresh", id=_id(prefix, "refresh"), n_clicks=0,
                                    style={"height": "38px", "alignSelf": "end", "marginRight": "6px"}),

                        dcc.Checklist(
                            id=_id(prefix, "auto"),
                            options=[{"label": " Auto Refresh (sec)", "value": "on"}],
                            value=[],
                            inline=True,
                            style={"alignSelf": "end", "marginRight": "6px"}
                        ),
                        dcc.Input(
                            id=_id(prefix, "auto_int"),
                            type="number",
                            value=5,
                            min=1,
                            step=1,
                            debounce=True,
                            style={"width": "70px", "alignSelf": "end"}
                        ),
                    ], style={"display": "flex", "alignItems": "end", "gap": 4}),
                    html.Div([html.Label("Channel", title="Input channel provided by the acquisition backend."),
                              dcc.Dropdown(id=_id(prefix, "ch"), options=[{"label": c, "value": c} for c in chs],
                                           value=(chs[0] if chs else None), clearable=False)],
                             style={"minWidth": 220, "marginRight": 12}),
                    html.Div([html.Label("Compare with",
                                         title="Overlay a second channel's FFT for comparison (requires matching sample rate)."),
                              dcc.Dropdown(id=_id(prefix, "cmp"),
                                           options=[{"label": "(none)", "value": ""}] + [{"label": c, "value": c} for c
                                                                                         in chs],
                                           value="", clearable=False)], style={"minWidth": 220, "marginRight": 12}),

                    html.Div([html.Label("FFT size",
                                         title="Number of points used in FFT. Higher = finer frequency resolution; larger CPU cost."),
                              dcc.Dropdown(id=_id(prefix, "nfft"), options=[{"label": str(n), "value": n} for n in
                                                                            (1024, 2048, 4096, 8192, 16384, 32768)],
                                           value=Defaults.nfft, clearable=False)],
                             style={"minWidth": 140, "marginRight": 12}),
                    html.Div([html.Label("Window",
                                         title="Windowing function for FFT to reduce spectral leakage. Hann is a good default."),
                              dcc.Dropdown(id=_id(prefix, "win"),
                                           options=[{"label": w.capitalize(), "value": w} for w in
                                                    ("rectangular", "hann", "hamming", "blackman")],
                                           value=Defaults.window, clearable=False)],
                             style={"minWidth": 160, "marginRight": 12}),
                    html.Div([html.Label("Sample rate [Hz]",
                                         title="Samples captured per second. Must match your device's rate."),
                              dcc.Input(id=_id(prefix, "fs"), type="number", value=Defaults.fs, min=8_000, max=192_000,
                                        step=1000, debounce=True)], style={"minWidth": 160, "marginRight": 12}),
                    html.Div([html.Label("Duration [s]", title="Length of the captured time window used for analysis."),
                              dcc.Input(id=_id(prefix, "dur"), type="number", value=Defaults.duration_s, min=0.1,
                                        max=10.0, step=0.1, debounce=True)],
                             style={"minWidth": 140, "marginRight": 12}),
                    html.Div([html.Label("Scale", title="Display amplitude linearly or in dBFS (0 dBFS = full scale)."),
                              dcc.RadioItems(id=_id(prefix, "scale"),
                                             options=[{"label": "Amplitude", "value": "Amplitude"},
                                                      {"label": "dBFS", "value": "dBFS"}],
                                             value=Defaults.scale, inline=True)],
                             style={"minWidth": 220, "marginRight": 12}),
                    html.Div([html.Label("Log-freq",
                                         title="Use a logarithmic frequency axis (better for wide-range spectra)."),
                              dcc.Checklist(id=_id(prefix, "logx"), options=[{"label": " log x", "value": "log"}],
                                            value=[])], style={"minWidth": 120, "marginRight": 12}),

                ],
                style={"display": "flex", "flexWrap": "wrap", "alignItems": "end", "gap": 8, "marginBottom": "10px"},
            ),

            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("Options",
                                               title="A-weighting approximates human hearing sensitivity; 1/3-octave shows energy per band; Spectrogram shows frequency content over time."),
                                    dcc.Checklist(
                                        id=_id(prefix, "opts"),
                                        options=[
                                            {"label": " A-weighting", "value": "A"},
                                            {"label": " 1/3-octave bars", "value": "OCT"},
                                            {"label": " Spectrogram", "value": "SPEC"},
                                        ],
                                        value=(["A", "OCT", "SPEC"] if Defaults.a_weight else ["OCT", "SPEC"]),
                                        inline=True,
                                    ),
                                    html.Div(
                                        [
                                            html.Label("Overlap [%]",
                                                       title="Window overlap used for spectrogram/STFT. Higher values smooth the view at higher compute cost."),
                                            dcc.Slider(id=_id(prefix, "ovl"), min=0, max=90, step=5,
                                                       value=Defaults.overlap, marks=None,
                                                       tooltip={"always_visible": False}),
                                        ],
                                        style={"minWidth": "260px", "display": "inline-block", "marginLeft": "16px"},
                                    ),
                                ],

                                style={"marginBottom": "6px"},
                            ),

                            dcc.Store(id=_id(prefix, "store")),
                            dcc.Store(id=_id(prefix, "store_cmp")),
                            dcc.Graph(id=_id(prefix, "time"), config={"displayModeBar": True, "responsive": False},
                                      style={"height": "320px"}),
                            dcc.Graph(id=_id(prefix, "fft"), config={"displayModeBar": True, "responsive": False},
                                      style={"height": "340px"}),

                            html.Div(id=_id(prefix, "metrics"),
                                     style={"fontSize": "1.1rem", "opacity": 2.9, "marginBottom": "8px"},
                                     title="Shows estimated fundamental frequency, total harmonic distortion (THD), and signal-to-noise ratio (SNR)."),

                            html.Label("Metric Summary",
                                       style={"fontSize": "1.25rem", "opacity": 0.9, "marginBottom": "10px"}),
                            DataTable(
                                id=_id(prefix, "metrics-table"),
                                columns=[{"name": "Metric", "id": "metric"}, {"name": "Value", "id": "value"}],
                                data=[],
                                style_table={"maxHeight": "420px", "overflowY": "auto", "marginBottom": "8px"},
                                style_cell={"padding": "4px", "fontSize": "0.9rem"},
                                style_data_conditional=[
                                    {
                                        "if": {"filter_query": "{highlight} = true"},
                                        "backgroundColor": "lightyellow",
                                    }
                                ]
                            ),
                        ],
                        style={"minWidth": "380px", "flex": "2"},
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("Top peaks",
                                               title="Number of largest spectral peaks to list in the table."),
                                    dcc.Slider(id=_id(prefix, "peaks"), min=1, max=10, step=1, value=Defaults.peaks,
                                               marks=None,
                                               tooltip={"always_visible": False}),
                                ], style={"marginBottom": 8},
                            ),
                            DataTable(
                                id=_id(prefix, "peak-table"),
                                columns=[{"name": "#", "id": "idx"}, {"name": "Freq [Hz]", "id": "f"},
                                         {"name": "Mag", "id": "y"}],
                                data=[], page_action="none",
                                style_table={"maxHeight": "410px", "overflowY": "auto"},
                            ),
                            html.Div(
                                [
                                    html.Label("Band RMS [Hz]",
                                               title="Root-mean-square level integrated over the selected frequency band."),
                                    html.Div(
                                        [
                                            dcc.Input(id=_id(prefix, "blo"), type="number", value=Defaults.band_lo,
                                                      min=0, step=10, style={"width": "45%", "marginRight": "6px"}),
                                            dcc.Input(id=_id(prefix, "bhi"), type="number", value=Defaults.band_hi,
                                                      min=0, step=10, style={"width": "45%"}),
                                        ],
                                        style={"display": "flex", "gap": "6px", "marginBottom": "8px"}
                                    ),
                                    html.Div(id=_id(prefix, "bandval"),
                                             style={"fontSize": "1.05rem", "fontWeight": "600", "marginBottom": "8px"}),
                                ]
                            ),

                            dcc.Graph(id=_id(prefix, "oct"), config={"displayModeBar": True, "responsive": False},
                                      style={"height": "300px"}),
                            dcc.Graph(id=_id(prefix, "spec"), config={"displayModeBar": True, "responsive": False},
                                      style={"height": "420px"}),
                            html.Button("Export CSV", id=_id(prefix, "exportbtn"), n_clicks=0,
                                        style={"marginTop": "6px"},
                                        title="Download the raw time series and FFT along with a JSON recipe of parameters."),
                            dcc.Download(id=_id(prefix, "download")),
                        ],
                        style={"minWidth": "320px", "flex": "1", "padding": "8px", "border": "1px solid #eee",
                               "borderRadius": "8px"},
                    ),
                ],
                style={"display": "flex", "flexWrap": "wrap", "gap": "10px"},
            ),
        ]
    )


# -------- Callbacks --------

def register_callbacks(app: Dash, prefix: str = "adv") -> None:
    @app.callback(
        Output(_id(prefix, "interval"), "interval"),
        Output(_id(prefix, "interval"), "disabled"),
        Input(_id(prefix, "auto"), "value"),
        Input(_id(prefix, "auto_int"), "value"),
    )
    def _auto_refresh(auto_values, interval_s):
        enabled = "on" in (auto_values or [])
        # Convert to milliseconds, clamp range
        interval_ms = max(1000, min(60000, int(interval_s or 5) * 1000))
        return interval_ms, not enabled

    @app.callback(
        Output(_id(prefix, "store"), "data"),
        Output(_id(prefix, "time"), "figure"),
        Input(_id(prefix, "refresh"), "n_clicks"),
        Input(_id(prefix, "interval"), "n_intervals"),  # <- add this
        State(_id(prefix, "ch"), "value"),
        State(_id(prefix, "dur"), "value"),
        State(_id(prefix, "fs"), "value"),
        prevent_initial_call=False,
    )
    def _main_data(_, __, ch, dur, fs):
        if not ch or not dur or not fs:
            return no_update, no_update
        t, x = _provider.get_window(ch, float(dur), int(fs))
        return {"fs": int(fs), "t": t.tolist(), "x": x.tolist(), "ch": ch}, _time_fig(t, x)

    @app.callback(
        Output(_id(prefix, "store_cmp"), "data"),
        Input(_id(prefix, "cmp"), "value"),
        State(_id(prefix, "dur"), "value"),
        State(_id(prefix, "fs"), "value"),
        prevent_initial_call=False,
    )
    def _cmp_data(ch_cmp, dur, fs):
        if not ch_cmp: return None
        t, x = _provider.get_window(ch_cmp, float(dur), int(fs))
        return {"fs": int(fs), "t": t.tolist(), "x": x.tolist(), "ch": ch_cmp}

    @app.callback(
        Output(_id(prefix, "fft"), "figure"),
        Output(_id(prefix, "peak-table"), "data"),
        Output(_id(prefix, "bandval"), "children"),
        Output(_id(prefix, "metrics"), "children"),
        Output(_id(prefix, "metrics-table"), "data"),
        Output(_id(prefix, "oct"), "figure"),
        Output(_id(prefix, "spec"), "figure"),

        Input(_id(prefix, "store"), "data"),
        Input(_id(prefix, "store_cmp"), "data"),
        Input(_id(prefix, "nfft"), "value"),
        Input(_id(prefix, "win"), "value"),
        Input(_id(prefix, "scale"), "value"),
        Input(_id(prefix, "logx"), "value"),
        Input(_id(prefix, "peaks"), "value"),
        Input(_id(prefix, "blo"), "value"),
        Input(_id(prefix, "bhi"), "value"),
        Input(_id(prefix, "ovl"), "value"),
        Input(_id(prefix, "opts"), "value"),
        prevent_initial_call=False,
    )
    def _analyze(store, store_cmp, nfft, win, scale, logx, npeaks, blo, bhi, ovl, opts):
        if not store: return go.Figure(), [], "", "", go.Figure(), go.Figure()
        fs = int(store["fs"]);
        x = np.asarray(store["x"], dtype=np.float32)
        nfft = int(nfft) if nfft and int(nfft) > 16 else 1024
        a_w = "A" in (opts or [])
        f, y = _rfft_single_sided(x, fs, nfft, win, scale, a_w)

        f_cmp = y_cmp = y_delta = None
        label_cmp = None
        if store_cmp:
            fs2 = int(store_cmp["fs"])
            if fs2 == fs:
                xc = np.asarray(store_cmp["x"], dtype=np.float32)
                f_cmp, y_cmp = _rfft_single_sided(xc, fs, nfft, win, scale, a_w)
                label_cmp = store_cmp.get("ch")
                # compute delta (only where freq vectors align)
                n_common = min(len(f), len(f_cmp))
                y_delta = y[:n_common] - y_cmp[:n_common]

        fft_fig = _fft_fig(
            f, y, scale, logx=("log" in (logx or [])),
            f_cmp=f_cmp, y_cmp=y_cmp, label_cmp=label_cmp,
            y_delta=y_delta

        )

        pf, py = _peak_pick(f, y, int(npeaks or Defaults.peaks))
        peaks_rows = [{"idx": i + 1, "f": round(float(pf[i]), 2), "y": round(float(py[i]), 3)} for i in range(pf.size)]

        blo = float(blo or Defaults.band_lo);
        bhi = float(bhi or Defaults.band_hi)
        if scale == "dBFS":
            # band RMS in dBFS: convert to linear, integrate, return dBFS
            a_lin = np.power(10.0, y / 20.0)
            m = (f >= blo) & (f <= bhi)
            band_val = 20 * np.log10(max(np.sqrt(np.sum(a_lin[m] * a_lin[m]) / 2.0), 1e-12)) if np.any(m) else float(
                "nan")
            band_text = f"Band RMS {blo:.0f}–{bhi:.0f} Hz: {band_val:.3f} dBFS"
        else:
            m = (f >= blo) & (f <= bhi)
            band_val = float(np.sqrt(np.sum((y[m] ** 2) / 2.0))) if np.any(m) else float("nan")
            band_text = f"Band RMS {blo:.0f}–{bhi:.0f} Hz: {band_val:.3f} A"

        f0, thd, snr = _thd_snr(f, y, scale)
        metrics = []
        # if f0 is not None: metrics.append(f"Fundamental ≈ {f0:.1f} Hz")
        # if thd is not None: metrics.append(("THD: " + (f"{thd:.2f} dB" if scale=="dBFS" else f"{thd:.4f}")))
        # if snr is not None: metrics.append(("SNR: " + (f"{snr:.1f} dB" if scale=="dBFS" else f"{snr:.4f}")))

        oct_fig = go.Figure()
        if "OCT" in (opts or []):
            centers, vals = _octave_bands(f, y, scale, third=True)
            oct_fig = _oct_fig(centers, vals, scale)

        spec_fig = go.Figure()
        if "SPEC" in (opts or []):
            fspec, S, t_frames = _stft(x, fs, nfft, int(ovl or Defaults.overlap), win, scale, a_w)
            spec_fig = _spec_fig(fspec, S, t_frames, scale)

        metric_dict = _compute_metrics(x, f, y, scale)
        metric_table = [{"metric": k, "value": v} for k, v in metric_dict.items()]

        # Delta metrics
        if y_delta is not None:
            delta_mean = np.mean(y_delta)
            delta_max = np.max(y_delta)
            metric_table.append({"metric": "Δ Mean [dB]", "value": f"{delta_mean:.2f}", "highlight": True})
            metric_table.append({"metric": "Δ Max [dB]", "value": f"{delta_max:.2f}", "highlight": True})

        return fft_fig, peaks_rows, band_text, " • ".join(metrics), metric_table, oct_fig, spec_fig

    @app.callback(
        Output(_id(prefix, "download"), "data"),
        Input(_id(prefix, "exportbtn"), "n_clicks"),
        State(_id(prefix, "store"), "data"),
        State(_id(prefix, "nfft"), "value"),
        State(_id(prefix, "win"), "value"),
        State(_id(prefix, "scale"), "value"),
        State(_id(prefix, "opts"), "value"),
        prevent_initial_call=True,
    )
    def _export(_, store, nfft, win, scale, opts):
        if not store:
            return go.Figure(), [], "", "", [], go.Figure(), go.Figure()

        fs = int(store["fs"]);
        t = np.asarray(store["t"]);
        x = np.asarray(store["x"])
        f, y = _rfft_single_sided(x, fs, int(nfft), win, scale, ("A" in (opts or [])))
        import io, csv, json
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["# Advanced Analysis export"])
        w.writerow(["# recipe", json.dumps({"fs": fs, "nfft": int(nfft), "window": win, "scale": scale, "opts": opts},
                                           separators=(",", ":"))])
        w.writerow(["time_s", "x"])
        for ti, xi in zip(t, x): w.writerow([f"{ti:.9f}", f"{xi:.9f}"])
        w.writerow([])
        w.writerow(["freq_hz", "fft_" + ("dbfs" if scale == "dBFS" else "amp")])
        for fi, yi in zip(f, y): w.writerow([f"{fi:.3f}", f"{yi:.6f}"])
        return dict(filename="mic_advanced_export.csv", content=buf.getvalue())


if __name__ == "__main__":
    set_data_provider(GiBufferProvider(
        address="127.0.0.1",
        buffer_uuid="2405a1b6-0359-11e9-86b5-6805ca34cb1e",
        var_idx_map={"Mic_Channel1": 1, "Mic_Channel2": 2},
        fs_hint=Defaults.fs,
    ))

    app = Dash(__name__, assets_folder='assets', external_stylesheets=[
        "https://cdn.jsdelivr.net/npm/bootswatch@5.3.3/dist/flatly/bootstrap.min.css"
    ])

    app.layout = layout("adv1")
    register_callbacks(app, "adv1")

    host = "127.0.0.1"
    port = 8050
    print("\n------------------------------------------------------------")
    print(f"Mic Advanced Analysis running at: http://{host}:{port}/")
    print("Use this link for the Dynamic Link widget in GI.bench (dashboard).")
    print("------------------------------------------------------------\n")

    app.run(debug=False, host=host, port=port)
