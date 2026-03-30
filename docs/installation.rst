Installation
============

**pygidata** is an open-source Python package available on PyPI.
It requires **Python 3.10 or newer**.

The package includes ``ginsutility`` as an expert/local module for local Highspeedport usage.

We recommend using a virtual environment to avoid dependency conflicts:

.. code-block:: bash

    python -m venv .venv
    source .venv/bin/activate   # On Windows: .venv\Scripts\activate

    # Install the module
    pip install pygidata

Local expert installation (``ginsutility`` module)
--------------------------------------------------

Use this only for local/LAN Highspeedport workflows that require ``giutility.dll``.

.. code-block:: bash

    pip install "pygidata[ginsutility]"

For editable development in this repository:

.. code-block:: bash

    pip install -e .[ginsutility]

.. warning::

    ``ginsutility`` is for local expert access and is not meant as a GI.cloud-only integration path.

You can also install a specific version like

.. code-block:: bash

    pip install pygidata==0.4.0

The latest release and installation requirements are available on PyPI:
📦 https://pypi.org/project/pygidata

If you want to contribute or setup a local development environment,
pygidata is open source and available on `GitHub <https://github.com/gantnerinstruments/pygidata>`_.

Make sure to read the README for installation and development instructions.

