Example read a GI.data stream
=============================
Description
-----------
In this example a GI.data stream will be read and imported into a Python object (numpy array).

.. figure:: plot_python_variable.jpg
   :scale: 75 %
   :alt: map to buried treasure
   
   Stream from GI.bench
   
The measurement will be plotted into a pyqtgraph time diagram. 

.. figure:: plot_online_values.jpg
   :scale: 75 %
   :alt: map to buried treasure

   Stream plotted in Python with pyqtgraph library
   
Requirements
------------
To run the example you need to install the following packages

    - numpy https://numpy.org/install/
	  data manipulation and scientific calculation
	
    - pyqtgraph http://www.pyqtgraph.org/
	  scientific graphics and GUI Library for Python to generate fast graph

You need also a Gantner Instrument controller connected to your network to fill GI.data or run ``Example_CreateBuffer.py``
 
Arguments
---------
  - ``-h / --help``: Shows a help message and exits.
  - ``-b / --buffer_id``: Gi.bench buffer UUID (required).
  - ``-a / --address``: IP of the Gantner device. Default is ``127.0.0.1``.
  - ``-v / --v_idx``: Indices of stream variables to display (0 is timestamp). Default is ``1``. Multiple arguments can be given, space separated ``1 2 3``.
  - ``-p / --plot_data``: Show a live plot using PyQtGraph. Default is ``False`` (flag).

Code
----
.. literalinclude:: ../../src/ginsutility/examples/example_get_buffer.py
    :language: python
