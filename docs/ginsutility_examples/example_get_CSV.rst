Example read a .csv file
========================
Description
-----------
In this example a .csv file will be read and imported into a Python object (numpy array).

.. figure:: plot_online_values.jpg
   :scale: 75 %
   :alt: map to buried treasure

   Stream plotted in Python with pyqtgraph library
   
Requirements
------------
To run the example you need to install the following packages

    - numpy https://numpy.org/install/
	  data manipulation and scientific calculation
	
    - pandas https://pandas.pydata.org/docs/getting_started/index.html#getting-started
      data manipulation and analysis

    - pyqtgraph http://www.pyqtgraph.org/
	  scientific graphics and GUI Library for Python to generate fast graph

You need also GI.bench (GI.data) running on a PC

Arguments
---------
  - ``-h / --help``: Shows a help message and exits.
  - ``-f / --file_name``: The name of the .csv file to write to. Default is ``data.csv``.
  - ``--no-plot``: Do not open the plot window; only validate data. Default is ``False`` (flag).

Code
----
.. literalinclude:: ../../src/ginsutility/examples/example_get_csv.py
    :language: python
