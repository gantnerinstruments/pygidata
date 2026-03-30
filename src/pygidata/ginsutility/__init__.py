"""Expose `ginsutility` under the `pygidata` namespace.

This keeps runtime logic untouched while allowing imports via
`pygidata.ginsutility` in addition to `ginsutility`.
"""
from importlib import import_module
import importlib.util
import sys
from pathlib import Path

_ORIG_NAME = "ginsutility"


def _load_original():
    try:
        return import_module(_ORIG_NAME)
    except Exception:
        pass

    here = Path(__file__).resolve()
    src_dir = None
    for parent in here.parents:
        if parent.name == "src":
            src_dir = parent
            break

    if src_dir is not None:
        init_py = src_dir / _ORIG_NAME / "__init__.py"
        if init_py.exists():
            spec = importlib.util.spec_from_file_location(_ORIG_NAME, str(init_py))
            module = importlib.util.module_from_spec(spec)
            sys.modules[_ORIG_NAME] = module
            spec.loader.exec_module(module)
            return module

    return None


_orig = _load_original()

if _orig is None:
    raise ImportError("Original package 'ginsutility' is not importable")

for _k in dir(_orig):
    if _k.startswith("__"):
        continue
    globals()[_k] = getattr(_orig, _k)

__all__ = [k for k in globals().keys() if not k.startswith("_")]
