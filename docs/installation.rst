Installation
============

**pygidata** is an open-source Python package available on PyPI.
It supports **Python 3.10-3.14**.

The package includes ``ginsutility`` as an expert/local module for local Highspeedport usage.

We recommend using a virtual environment to avoid dependency conflicts:

.. code-block:: console

    python -m venv .venv

On Windows PowerShell, activate it with ``.\.venv\Scripts\Activate.ps1``.
On Linux/macOS, use ``source .venv/bin/activate``. Then install:

.. code-block:: console

    python -m pip install pygidata

For :doc:`data_access`, also install ``matplotlib`` for the plot. ``pandas`` is
already a dependency of ``pygidata``.

In JupyterLab, install into the selected kernel instead of an unrelated terminal:

.. code-block:: python

    %pip install pygidata matplotlib

Restart the kernel after installation.

The distribution name is ``pygidata``, but the documented API import is:

.. code-block:: python

    from gi_data.dataclient import GIDataClient

Connection settings
-------------------

Set these in the process launching your kernel, or use ``getpass.getpass()``
in a private setup cell to assign credentials to ``os.environ``:

.. list-table::
   :header-rows: 1

   * - Variable
     - Value
   * - ``GI_BASE_URL``
     - Target URL, e.g. ``https://your-tenant.gi-cloud.io`` or
       ``http://your-controller:8090``.
   * - ``GI_TOKEN``
     - Access token. Takes precedence in the notebook examples.
   * - ``GI_USER``, ``GI_PASSWORD``
     - Username/password alternative; unset ``GI_TOKEN`` to use it.

Download :download:`.env.example <.env.example>` for the base URL and credentials.
Copy it to ``.env`` in the notebook's working directory and fill in your URL
and token or username/password. To load it explicitly, install ``python-dotenv`` with
``%pip install python-dotenv``, then run this before the notebook's Setup cell:

.. code-block:: python

    from dotenv import load_dotenv

    load_dotenv(".env", override=False)

Existing environment variables take precedence. The examples do not load the
file automatically. Keep the populated file private; ``docs/.env`` is ignored
by Git. Use HTTPS for remote connections.

Source/variable/measurement IDs, file paths, and write values belong in
the relevant notebook cells, not in ``.env``.

Local expert installation (``ginsutility`` module)
--------------------------------------------------

Use this only for local/LAN Highspeedport workflows that require
``GInsUtility.dll`` (Windows) or ``libGInsUtility.so`` (Linux).

.. code-block:: bash

    pip install "pygidata[ginsutility]"

For editable development in this repository:

.. code-block:: bash

    pip install -e .[ginsutility]

.. warning::

    ``ginsutility`` is for local expert access and is not meant as a GI.cloud-only integration path.

You can also install a specific version like

.. code-block:: bash

    python -m pip install pygidata==0.5.3

The latest release and installation requirements are available on PyPI:
📦 https://pypi.org/project/pygidata

If you want to contribute or setup a local development environment,
pygidata is open source and available on
`GitHub <https://github.com/gantnerinstruments/gimodules-python>`_.

Make sure to read the README for installation and development instructions.
