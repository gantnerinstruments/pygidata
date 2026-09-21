API notes
=========

Examples live in :doc:`data_access` and :doc:`measurements`; connection settings
are in :doc:`installation`. Inspect the installed version for full signatures:

.. code-block:: python

   import inspect
   from gi_data.dataclient import GIDataClient

   print(inspect.signature(GIDataClient.fetch_buffer))
   help(GIDataClient.import_data)

Return values
-------------

.. list-table::
   :header-rows: 1

   * - Operation
     - Result
   * - Source / variable discovery
     - Model lists; use ``.id`` for requests and ``.name`` / ``.unit`` for display.
   * - ``fetch_buffer`` / ``fetch_history``
     - pandas DataFrame with variable-ID strings as columns.
   * - ``read_online``
     - Current values keyed by online variable UUID.
   * - ``stream_online`` / ``stream_kafka``
     - Async iterators; service availability depends on the target.
   * - ``export_csv`` / ``export_udbf``
     - File bytes, not a path or DataFrame.
   * - ``import_csv`` / ``import_udbf``
     - Import session ID, not the destination source ID.
   * - ``get_measurement``
     - Measurement model or ``None``; local-only.

Conventions
-----------

``VarSelector`` comes from ``gi_data.mapping.models`` and uses uppercase
``SID`` / ``VID`` fields. Discovered model attributes are lowercase. Names
need not be unique; source IDs may be strings, UUIDs, or integers.

Fetch time bounds use Unix milliseconds. The default point budget is 2048;
it is not a guarantee about raw samples or memory use. Cloud fixed-resolution
fetches return averages, except RAW/NANOS. Local fetches reject explicit
resolution; local exports ignore it.

Enums come from ``gi_data.mapping.enums``. An available ``DataFormat`` enum
member does not imply driver support: current exporters advertise CSV and UDBF.
``CSVSettings`` configures local/raw CSV, not aggregated cloud CSV.
``CSVImportSettingsDefaultCloud`` describes a multi-row layout, not arbitrary CSV.

``close()`` closes HTTP resources, including on context-manager exit; it does
not currently close WebSocket/Kafka resources. ``set_log_level()`` affects the
whole ``gi_data`` package. Review DEBUG logs before sharing them.

Common errors
-------------

.. list-table::
   :header-rows: 1

   * - Symptom
     - Action
   * - ``KeyError`` for credentials or an ID
     - Load credentials into the environment; edit cell inputs using discovered IDs.
   * - Empty data
     - Check permissions, retention, recording bounds, and milliseconds vs seconds.
   * - ``NotImplementedError``
     - Check the notebook's cloud/local scope and the target's enabled services.
   * - CSV parsing errors
     - Inspect separators, decimal symbols, time columns, and metadata rows first.
