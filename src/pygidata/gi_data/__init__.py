"""pygidata.gi_data shim

This module exposes the original top-level `gi_data` package under the
``pygidata.gi_data`` namespace. It imports and re-exports the original
package's public API. No logic is changed.
"""
from importlib import import_module, util
import importlib
import importlib.util
import sys
from pathlib import Path

_orig_name = "gi_data"


def _load_original():
    # 1) try normal import
    try:
        return import_module(_orig_name)
    except Exception:
        pass

    # 2) try to find a local src/gi_data package relative to this file
    p = Path(__file__).resolve()
    src_dir = None
    for parent in p.parents:
        if parent.name == "src":
            src_dir = parent
            break

    if src_dir:
        candidate = src_dir / _orig_name
        init_py = candidate / "__init__.py"
        if candidate.is_dir() and init_py.exists():
            spec = importlib.util.spec_from_file_location(_orig_name, str(init_py))
            module = importlib.util.module_from_spec(spec)
            sys.modules[_orig_name] = module
            spec.loader.exec_module(module)
            return module

    return None


_orig = _load_original()

if _orig is None:
    class _Missing:
        def __getattr__(self, name):
            raise ImportError(f"Original package '{_orig_name}' is not importable")


    _orig = _Missing()

for _k in dir(_orig):
    if _k.startswith("__"):
        continue
    globals()[_k] = getattr(_orig, _k)

__all__ = [k for k in globals().keys() if not k.startswith("_")]
