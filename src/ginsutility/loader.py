import ctypes
import os
import platform
from pathlib import Path
from typing import Optional


def _default_lib_path() -> Optional[Path]:
    """Return the OS-specific absolute default path, or None if not applicable."""
    sysname = platform.system().lower()
    if sysname == "windows":
        return Path(r"C:\Users\Public\Documents\Gantner Instruments\GI.bench\api\Windows\bin\GInsUtility.dll")
    if sysname == "linux":
        return Path("/usr/lib/x86_64-linux-gnu/libGInsUtility.so")
    return None


def _prepare_deps_dir(directory: Path) -> None:
    """On Windows, add the DLL directory so dependent DLLs can be resolved."""
    if os.name == "nt" and hasattr(os, "add_dll_directory"):
        os.add_dll_directory(str(directory))


def _cdll(path: str) -> ctypes.CDLL:
    """Load the shared library with sensible defaults per platform."""
    if os.name == "nt":
        return ctypes.CDLL(path)
    return ctypes.CDLL(path, mode=ctypes.RTLD_LOCAL)


def load_giutility() -> ctypes.CDLL:
    """
    Load GiUtility with the following precedence (no relative paths):
      1) OS default absolute path
         - Windows: C:\\Users\\Public\\Documents\\Gantner Instruments\\GI.bench\\api\\bin\\windows\\x64\\giutility.dll
         - Linux:   /usr/lib/libGInsUtility.so
      2) GINS_GIUTILITY_PATH environment variable (must be absolute)

    If not found, raise with guidance to set GINS_GIUTILITY_PATH.
    """
    sysname = platform.system().lower()
    default_path = _default_lib_path()

    if default_path and default_path.is_file():
        try:
            _prepare_deps_dir(default_path.parent)
            return _cdll(str(default_path))
        except OSError as e:
            raise OSError(
                f"Failed to load GiUtility from default path '{default_path}'. "
                f"Set GINS_GIUTILITY_PATH to the absolute path of the library. Original error: {e}"
            ) from e

    env_path = os.getenv("GINS_GIUTILITY_PATH")
    if env_path:
        p = Path(env_path)
        if not p.is_absolute():
            raise ValueError(
                f"GINS_GIUTILITY_PATH must be an absolute path, got: '{env_path}'"
            )
        if not p.is_file():
            raise FileNotFoundError(
                f"GINS_GIUTILITY_PATH points to a non-existent file: '{p}'"
            )
        try:
            _prepare_deps_dir(p.parent)
            return _cdll(str(p))
        except OSError as e:
            raise OSError(
                f"Failed to load GiUtility from GINS_GIUTILITY_PATH='{p}'. "
                f"Verify the path and its dependencies. Original error: {e}"
            ) from e

    if default_path:
        raise FileNotFoundError(
            f"GiUtility library not found at default path '{default_path}'.\n"
            f"Please set GINS_GIUTILITY_PATH to the absolute path of the library file.\n\n"
            f"Examples:\n"
            f"  Windows (PowerShell):  $env:GINS_GIUTILITY_PATH = 'C:\\Path\\to\\giutility.dll'\n"
            f"  Linux (bash):          export GINS_GIUTILITY_PATH='/usr/lib/libGInsUtility.so'\n"
        )

    lib_name = "libGInsUtility.dylib" if sysname == "darwin" else "<library>"
    raise FileNotFoundError(
        f"No default path configured for platform '{sysname}'. "
        f"Set GINS_GIUTILITY_PATH to the absolute path of the library file (e.g., '{lib_name}').\n\n"
        f"Examples:\n"
        f"  macOS (zsh/bash): export GINS_GIUTILITY_PATH='/usr/local/lib/{lib_name}'\n"
        f"  Linux:            export GINS_GIUTILITY_PATH='/usr/lib/libGInsUtility.so'\n"
        f"  Windows:          $env:GINS_GIUTILITY_PATH = 'C:\\Path\\to\\giutility.dll'\n"
    )
