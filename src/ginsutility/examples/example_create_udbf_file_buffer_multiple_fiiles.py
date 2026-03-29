from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from typing import Iterable, List, Sequence

import ginsutility.postprocessbuffer as postprocessbuffer_manager
import numpy as np
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

UNIX_TO_GINS_EPOCH_SECONDS = 946684800  # 2000-01-01T00:00:00Z


@dataclass(frozen=True)
class VarDef:
    var_id: str
    name: str
    unit: str


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a UDBF buffer and ingest segments; optionally verify by reading via controller IP",
        formatter_class=CustomHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "-h", "--help", action="help", default=argparse.SUPPRESS,
        help="Show this help message and exit. All arguments are optional; strings do not need quoting.",
    )

    # only used for verification (read-back), not for writing the UDBF
    parser.add_argument(
        "-c", "--controller_ip",
        type=str,
        default="qcore-111004",
        help="Controller IP used for verify/read-back (default: %(default)s)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="After writing, connect via HighSpeedPortClient to the postprocess buffer and read one batch",
    )

    parser.add_argument(
        "-f", "--FilePath", type=str,
        default=r"C:\Users\Public\Documents\Gantner Instruments\GI.bench\data\records",
        help="Full directory path for the UDBF file",
    )
    parser.add_argument("-i", "--SourceID", type=str, default="0a929f60-9536-11f0-b59d-2c58b9196a11",
                        help="Source ID (UUID)")
    parser.add_argument("-n", "--SourceName", type=str, default="python_udbf_bufferT", help="Source name")
    parser.add_argument("-r", "--SampleRateHz", type=float, default=100000.0, help="Sample rate in Hz (default: 1000)")
    parser.add_argument("-m", "--MaxLengthSeconds", type=int, default=3600, help="Max buffer length in seconds")
    parser.add_argument("-o", "--Options", type=int, default=0, help="Options bitmask")
    parser.add_argument("-u", "--Unit", type=str, default="V", help="(Fallback) Unit of measurement")

    parser.add_argument(
        "--var",
        action="append",
        nargs=3,
        metavar=("VAR_ID", "VAR_NAME", "UNIT"),
        help="Add a variable/channel. Repeatable. Example: --var <uuid> Temp C --var <uuid> Pressure bar",
        default=[],
    )

    parser.add_argument("--SegmentSeconds", type=float, default=0.00001, help="Length of one segment in seconds")
    parser.add_argument("--SegmentCount", type=int, default=5, help="How many segments to ingest")
    parser.add_argument("--GapSeconds", type=float, default=0, help="Gap inserted after each segment (in seconds)")
    parser.add_argument("--NoSleep", action="store_true", help="Do not call conn.sleep_ms() between appends")
    parser.add_argument("-l", "--FrameBufferLength", type=int, default=1,
                        help="How many samples per frame ingested into buffer")

    return parser.parse_args(argv)


def now_gins_epoch_ns() -> np.uint64:
    unix_s = time.time()
    gins_s = unix_s - UNIX_TO_GINS_EPOCH_SECONDS
    return np.uint64(round(gins_s * 1_000_000_000))


def _get_vars(args: argparse.Namespace) -> List[VarDef]:
    if args.var:
        return [VarDef(var_id=v[0], name=v[1], unit=v[2]) for v in args.var]

    return [
        VarDef(
            var_id="00000000-0000-0000-0000-000000000001",
            name="time_ns",
            unit="ns",
        ),
        VarDef(
            var_id="00000000-0000-0000-0000-000000000002",
            name="signal_rand",
            unit="V",
        ),
        VarDef(
            var_id="00000000-0000-0000-0000-000000000003",
            name="signal_sin",
            unit="V",
        ),
    ]


def _add_channels(ppm: postprocessbuffer_manager.PostProcessBufferManager, vars_: Sequence[VarDef]) -> None:
    for v in vars_:
        ppm.add_channel(v.var_id, v.name, v.unit)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    vars_ = _get_vars(args)

    sample_rate_hz = float(args.SampleRateHz)
    segment_seconds = float(args.SegmentSeconds)
    segment_count = int(args.SegmentCount)
    gap_ns = int(float(args.GapSeconds) * 1_000_000_000)

    if sample_rate_hz <= 0:
        raise ValueError("SampleRateHz must be > 0")
    if segment_seconds <= 0:
        raise ValueError("SegmentSeconds must be > 0")
    if segment_count <= 0:
        raise ValueError("SegmentCount must be > 0")
    if gap_ns < 0:
        raise ValueError("GapSeconds must be >= 0")

    ns_per_sample = int(round(1_000_000_000 / sample_rate_hz))
    total_samples_per_segment = int(round(segment_seconds * sample_rate_hz))
    if total_samples_per_segment <= 0:
        raise ValueError("Computed total_samples_per_segment <= 0")

    # Create once to validate init + frame length (same as your structure)
    ppm = postprocessbuffer_manager.PostProcessBufferManager()

    ok = ppm.create_udbf_file_buffer(
        args.FilePath,
        args.SourceID,
        args.SourceName,
        sample_rate_hz,
        int(args.MaxLengthSeconds),
        int(args.Options),
    )
    if not ok:
        logging.error("Failed to create UDBF buffer (result=%s)", ok)
        return 1
    logging.info("UDBF buffer created")

    _add_channels(ppm, vars_)
    ppm.initialize_buffer(args.FrameBufferLength)

    frame_len = int(ppm.frame_buffer_length)
    if frame_len <= 0:
        raise RuntimeError(f"Invalid ppm.frame_buffer_length={frame_len}")

    appends_per_segment = total_samples_per_segment // frame_len
    remainder = total_samples_per_segment % frame_len
    if remainder != 0:
        raise RuntimeError(
            f"SegmentSeconds={segment_seconds}s at {sample_rate_hz}Hz => {total_samples_per_segment} samples, "
            f"which is not divisible by frame_buffer_length={frame_len}. "
            f"Pick SegmentSeconds such that samples are divisible (e.g. {frame_len / sample_rate_hz:.6f}s multiples)."
        )

    buffer_uuid = getattr(ppm, "source_id", None) or getattr(ppm, "SourceID", None) or args.SourceID

    logging.info(
        "Initialized: SourceName=%s SourceID=%s SampleRateHz=%.1f frame_buffer_length=%d channels=%d",
        args.SourceName, buffer_uuid, sample_rate_hz, frame_len, len(vars_)
    )

    nanos = now_gins_epoch_ns()

    for seg_idx in range(segment_count):
        ppm = postprocessbuffer_manager.PostProcessBufferManager()

        seg_source_name = f"{args.SourceName}__seg{seg_idx + 1}"

        ok = ppm.create_udbf_file_buffer(
            args.FilePath,
            args.SourceID,
            seg_source_name,
            sample_rate_hz,
            int(args.MaxLengthSeconds),
            int(args.Options),
        )
        if not ok:
            logging.error("Failed to create UDBF buffer (segment %d)", seg_idx + 1)
            return 1

        _add_channels(ppm, vars_)
        ppm.initialize_buffer()

        frame_len = int(ppm.frame_buffer_length)
        if frame_len <= 0:
            raise RuntimeError(f"Invalid ppm.frame_buffer_length={frame_len}")

        for i in range(appends_per_segment):
            for frameindex in range(frame_len):
                ts_ns = int(nanos)
                ppm.write_timestamp(frameindex, ts_ns)

                if len(vars_) >= 1:
                    ppm.write_value(frameindex, 0, i)

                if len(vars_) >= 2:
                    ppm.write_value(frameindex, 1, float(np.random.random_sample() * 100.0))

                if len(vars_) >= 3:
                    t_sec = ts_ns * 1e-9
                    ppm.write_value(frameindex, 2, float(np.sin(2.0 * np.pi * 1000.0 * t_sec)))

                for ch in range(3, len(vars_)):
                    ppm.write_value(frameindex, ch, float(np.random.random_sample() * 100.0))

                nanos = np.uint64(ts_ns + ns_per_sample)

            ppm.append_frame_buffer()

            if not args.NoSleep:
                ppm.sleep_ms(0)

        seg_end_ns = int(nanos)
        logging.info(
            "segment %d/%d done: wrote %d samples (%.6fs). end_ts_ns=%d",
            seg_idx + 1, segment_count, total_samples_per_segment, segment_seconds, seg_end_ns
        )

        try:
            ppm.close_buffer()
            logging.info("Buffer closed (segment %d)", seg_idx + 1)
        except Exception:
            pass

        nanos = np.uint64(seg_end_ns + gap_ns)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
