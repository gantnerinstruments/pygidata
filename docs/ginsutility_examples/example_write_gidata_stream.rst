Example write a new stream to GI.data
=====================================
Description
-----------
In this example some random data will be generated in Python and written in GI.bench into a new stream.

.. figure:: buffer_python_variable.jpg
   :scale: 75 %
   :alt: map to buried treasure

   A new buffer is created in GI.data.

.. figure:: plot_python_variable.jpg
   :scale: 75 %
   :alt: map to buried treasure

   Random data generated in Python can be plotted in GI.bench.
   
Requirements
------------
To run the example you need to install the following packages

    - numpy https://numpy.org/install/
	  data manipulation and scientific calculation

You need also GI.bench (GI.data) running on a PC

Arguments
---------
  - ``-h / --help``: Shows a help message and exits.
  - ``-i / --ID``: The ID of the buffer in GI.bench. Default is ``ff1fbdd4-7b23-11ea-bd6d-005056c00001``.
  - ``-b / --BufferName``: Name of the buffer; Default is ``PythonBuffer``.
  - ``-s / --StreamSampleRate``: Sampling rate of the stream in Hz. Default is ``10``.
  - ``-v / --variableID``: Id of the variable in GI.bench. Default is ``vv1fbdd4-7b23-11ea-bd6d-005056c0001``.
  - ``-n / --variableName``: Name of the variable. Default is ``python_variable``.
  - ``-u / --Unit``: Unit of the variable. Default is ``V``.

Code
----
.. literalinclude:: ../../src/ginsutility/examples/example_create_buffer.py
    :language: python
