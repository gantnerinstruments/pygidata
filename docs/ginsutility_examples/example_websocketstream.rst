Example read online values from websocket
=========================================
Description
-----------
In this example we read online values from a websocket.

Requirements
------------
No additional packages are required to run the example

You need an active datastream on your connection. You can create one for testing in GI.bench like this:

.. figure:: add_system_datastream.png
   :scale: 100%
   :alt: map to buried treasure

Now the access type needs to be changed to ``ProcessImage``

.. figure:: access_type.png
    :scale: 100%
    :alt: map to buried treasure

And finally the variables need to be added to the datastream.

.. figure:: select_variables.png
    :scale: 100%
    :alt: map to buried treasure

Arguments
---------

  - ``-h / --help``: Shows a help message and exits.
  - ``-w / --websocket_url``: URL of websocket. Default is ``127.0.0.1``.
  - ``-: / --port``: Port of websocket. Default is ``8090``.
  - ``-r / --route``: Route of anything connected to websocket. Default is empty.
  - ``-u / --username``: Username for websocket. Default is empty.
  - ``-p / --password``: Password for websocket. Default is empty.
  - ``-t / --timeout``: Timeout for websocket connection initialisation in seconds. Default is ``10``.
  - ``-s / --sample_rate``: Sampling rate in Hz. Default is ``1``.
  - ``-d / --stream_id``: Stream ID of the stream. Default is ``0``.
  - ``-x / --start_time``:
      - ``>1``: Value explicitly set
      - ``1``: Start at the very end, end must not be ``0``
      - ``-n``: If start is negative, start will be ``n`` milliseconds before end
      - ``0``: Start at the beginning
      - Default is ``0``
  - ``-y / --end_time``: Can be set to a specific value or ``0`` or ``-1`` to deactivate. Default is ``0``.
  - ``-b / --buffer_type``: Accepted values are ``BUFFER``, ``HSP_ARCHIVES`` and ``HSP_FILES``. Default is ``BUFFER``.

Code
----
.. literalinclude:: ../../src/ginsutility/examples/example_websocket_read_buffer.py
    :language: python
