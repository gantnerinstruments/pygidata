Example write online values with websocket connection
=====================================================
Description
-----------
In this example we write values on a connected controller channel.

Requirements
------------
No additional packages are required to run the example

You need GI.bench running and have at least one variable added to your current project.
Virtual variables for testing can be created with this button:

.. figure:: create_VV.png
   :scale: 100 %
   :alt: map to buried treasure

Tip
---
Do not forget to enter the IP of your connected controller.
The method ``init_online_connection`` is called to get the total index of channels independently of the configured data buffer.

Arguments
---------

  - ``-h / --help``: Shows a help message and exits.
  - ``--url``: Controller IP address / websocket host. Default is ``127.0.0.1``.
  - ``--port``: WebSocket port. Default is ``8090``.
  - ``--route``: Optional WebSocket route. Default is empty.
  - ``--username``: Username for authentication. Default is empty.
  - ``--password``: Password for authentication. Default is empty.
  - ``--timeout``: Connection timeout in seconds. Default is ``10.0``.
  - ``--interval-ms``: Online update interval in milliseconds. Default is ``200``.
  - ``--channels``: Comma-separated ONLINE channel indices to write to. Required.
  - ``--values``: Comma-separated values to write. Must have the same count as ``--channels``.
  - ``--immediate``: Use immediate write mode. Default is ``False`` (flag).

Code
----
.. literalinclude:: ../../src/ginsutility/examples/example_websocket_write_online.py
    :language: python
