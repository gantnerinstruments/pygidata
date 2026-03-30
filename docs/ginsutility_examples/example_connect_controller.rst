Example connect to a controller
===============================
Description
-----------
In this example the measurement buffer data measurement of a connected controller will be read and imported into a Python object.
The measurement will be ploted in a pyqtgraph time diagram. 

Requirements
------------
To run the example you need to install the following packages

    - numpy https://numpy.org/install/
	  data manipulation and scientific calculation
	
    - pyqtgraph http://www.pyqtgraph.org/
	  scientific graphics and GUI Library for Python to generate fast graph

You also need a Gantner Instrument controller connected to your network.

    - enter the IP of the controller
	
    - the channel index to be plotted

Tip
----
Do not forget to enter the IP of your connected controller and verify that the correct channel index is enterd to plot.

.. figure:: change_IP_controller.jpg
   :scale: 75 %
   :alt: map to buried treasure
   
   Enter IP of your controller, buffer and channel index

Arguments
---------
  - ``-h / --help``: Shows a help message and exits.
  - ``-c / --controller_IP``: IP address of the controller. Default is ``127.0.0.1``.
  - ``-i / --channel_index``: Indices of the channels to retrieve. Default is ``1 2``. Multiple arguments can be given, space separated ``1 2 3``.
  - ``-g / --show_gui``: Display a live plot using PyQtGraph. Default is ``False`` (flag).
  - ``-b / --buffer_index``: Gi.bench buffer UUID or controller buffer index. Default is ``0``.

Code
----
.. literalinclude:: ../../src/ginsutility/examples/example_connect_controller.py
    :language: python
