Example read online values from controller
==========================================
Description
-----------
In this example we read online values from a connected controller channel.

Requirements
------------
No additional packages are required to run the example

You need a Gantner Instrument controller connected to your network.

    - enter the IP of the controller
	
    - the channel indices to be changed

Tip
---
Do not forget to enter the IP of your connected controller.
The method ``init_online_connection`` is called to get the total index of channels independently of the configured data buffer.

Arguments
---------
  - ``-h / --help``: Shows a help message and exits.
  - ``--url``: Controller IP address. Default is ``127.0.0.1``.
  - ``--port``: WebSocket port. Default is ``8090``.
  - ``--route``: Optional WebSocket route. Default is empty.
  - ``--username``: Username for authentication. Default is empty.
  - ``--password``: Password for authentication. Default is empty.
  - ``--timeout``: Connection timeout in seconds. Default is ``10.0``.
  - ``--interval-ms``: Online update interval in milliseconds. Default is ``200``.
  - ``--channels``: Comma-separated ONLINE channel indices to read. Default is ``0,1,2,3,4,5``.
  - ``--loops``: Number of read iterations. Default is ``20``.
  - ``--sleep``: Sleep time between reads in seconds. Default is ``0.2``.

Code
----
.. literalinclude:: ../../src/ginsutility/examples/example_websocket_read_online.py
    :language: python
