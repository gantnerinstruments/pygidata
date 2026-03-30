Example write a new stream to GI.data using .csv file
=====================================================
Description
-----------
In this example data will be taken from a .csv file and written in GI.bench into a new stream.
   
Requirements
------------
To run the example you need to install the following packages

    - numpy https://numpy.org/install/
	  data manipulation and scientific calculation

You need also GI.bench (GI.data) running on a PC

Arguments
---------
  - ``-h / --help``: Shows a help message and exits.
  - ``-i / --ID``: The ID of the buffer in GI.bench. Default is ``ff1fbdd4-7b23-11ea-bd6d-005056c00002``.
  - ``-b / --BufferName``: Name of the buffer; Default is ``PythonBuffer2``.
  - ``-s / --StreamSampleRate``: Sampling rate of the stream in Hz. Default is ``10``.
  - ``-v / --variableID``: Id of the variable in GI.bench. Default is ``vv1fbdd4-7b23-11ea-bd6d-005056c0002``.
  - ``-n / --variableName``: Name of the variable. Default is ``python_variable2``.
  - ``-u / --Unit``: Unit of the variable. Default is ``V``.
  - ``-f / --fileName``: The name of the .csv file to read from. Default is ``data.csv``.
  - ``-p / --progress_report``: Prints progress report every n values that have been read; Default is ``0`` (disabled).

Code
----
.. literalinclude:: ../../src/ginsutility/examples/example_csv_to_buffer.py
    :language: python
