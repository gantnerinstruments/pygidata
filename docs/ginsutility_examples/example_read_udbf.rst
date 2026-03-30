Example read udbf file
======================
Description
-----------
In this example the information and measurement data written in the Gantner-Instruments binary file ``GinsDataloggerReadDatFiles.dat`` will be imported into a Python object (numpy matrix). This allow the user  for further data processing operation.

.. figure:: GinsDataloggerReadDatFiles.jpg
   :scale: 25 %
   :alt: map to buried treasure

   GINS udbf measurement example.

    - The minimum and maximum temperature of channel 1, will be calculated and exported in the file ``resu.txt``
	
    - A plot of channel 1, will be generated as ``Temperature.png``

Requirements
------------
To run the example you need to install the following packages

    - numpy https://numpy.org/install/
	  data manipulation and scientific calculation
	
    - matplotlib https://matplotlib.org/
	  generate plots of the measurements

Arguments
---------
  - ``-h / --help``: Shows a help message and exits.
  - ``-f / --file``: The name of ubdf file to be imported. Default is ``GinsDataloggerReadDatFiles.dat``.
  - ``-t / --timestamp_index``: Channel index of timestamp (usually ``0``). Default is ``0``.
  - ``-i / --channel_index``: Channel index of the data channel to analyze. Default is ``1``.

Code
----
.. literalinclude:: ../../src/ginsutility/examples/example_read_dat_files.py
    :language: python
