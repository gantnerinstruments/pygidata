Local Expert / Local Access (ginsutility)
=========================================

``ginsutility`` is included in ``pygidata`` as a supported expert/local module.
It provides direct access to Gantner Instruments via the Highspeedport
interface and is intended for advanced or on-site workflows where a local
DLL is required.

Scope
-----

- Purpose: local or LAN access via the Highspeedport interface.
- Runtime: requires ``giutility.dll`` from a local GI.bench / Q.core installation.
- Typical use case: advanced local diagnostics and high-speed local workflows.
- Not intended for GI.cloud-only environments.

How it relates to ``pygidata``
------------------------------

- ``pygidata`` is the primary package for REST/GraphQL and cloud access.
- ``ginsutility`` is a separate module that provides local hardware access and
  remains an optional expert component. No runtime logic is merged; users
  can choose to use the local module when appropriate.

Installation
------------

Install the optional expert dependencies:

.. code-block:: bash

    pip install "pygidata[ginsutility]"

Or for editable install from this repository:

.. code-block:: bash

    pip install -e .[ginsutility]

DLL requirement
---------------

``ginsutility`` requires that ``giutility.dll`` is available on the runtime path.
A typical default Windows location is:

``C:\\Users\\Public\\Documents\\Gantner Instruments\\GI.bench\\api\\Windows\\bin\\GInsUtility.dll``

Validation
----------

Use a quick import test after installation:

.. code-block:: bash

    python -c "import ginsutility as g; print('ginsutility import ok')"

Then run package-specific examples or tests from the ``ginsutility`` module.

Examples
--------

The original practical example pages are available under the ginsutility
documentation section:

.. toctree::
   :maxdepth: 2

   ginsutility_examples/examples



