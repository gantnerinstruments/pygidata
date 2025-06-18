What Is JupyterLab?
===================

JupyterLab enables you to work with documents and activities such as Jupyter notebooks,
text editors, terminals, and custom components in a flexible, integrated, and extensible manner.

The core target is to have a tool for data analysis and data visualization.
For that manner, Jupyter Notebooks are being used, which can be run interactively.


Getting Started with JupyterLab
===============================

Accessing JupyterLab in GI.cloud
--------------------------------

Open the JupyterLab via the Tools embedded in the GI.cloud web UI.

This is your working environment where you can inspect files and run Python scripts.

.. image:: ./_images/j-lab.png
    :width: 600
    :alt: Gantner J-lab


Installing Additional Python Packages
-------------------------------------

To manually install required packages, you can open a ``terminal`` to execute Linux commands.

.. note::

    If you want to experiment with different packages and dependencies, it is recommended to create a virtual environment.

Set up a virtual environment (venv) with pip:
https://dev.gantner-instruments.com/webfiles/public/Download/Software/Python/ginsapy/doc/build/html/installation.html#set-up-a-virtual-environment

Or with conda:
https://www.digitalocean.com/community/tutorials/how-to-install-anaconda-on-ubuntu-18-04-quickstart


Running Python Notebooks
------------------------

Most of the delivered Jupyter Notebook scripts can simply be executed sequentially.
Once you've opened a ``.ipynb`` file, you can either run each cell using ``Shift + Enter``, or select **Run → Run All Cells** from the menu.

.. image:: ./_images/run_all.png
    :width: 350
    :alt: Run all cells


Managing Kernels and Memory Usage
---------------------------------

.. warning::

    The Python **kernel** of **JupyterLab can only persist** if enough memory is available to be allocated.
    This can be an issue if memory is limited, but currently running kernels still occupy resources.

To shut down running kernels that are not needed, open the "Running Terminals and Kernels" tab on the left-hand side.
There you can terminate unused kernels and free up memory.

.. image:: ./_images/view_kernels.png
    :width: 350
    :alt: View terminals and kernels
