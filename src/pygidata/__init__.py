"""
pygidata package

This package provides a unified namespace for the project. It exposes the
existing top-level packages `gi_data` and `ginsutility` as subpackages
``pygidata.gi_data`` and ``pygidata.ginsutility`` respectively. The wrapper
modules re-export the real implementations (no logic is changed).

Usage examples::

    import pygidata
    import pygidata.gi_data
    import pygidata.ginsutility

Notes:
 - The real implementations still live at the top-level packages
   (`gi_data`, `ginsutility`) so existing imports continue to work.
"""

# The pygidata package provides a unified namespace.
# Subpackages are available as `pygidata.gi_data` and `pygidata.ginsutility`.
