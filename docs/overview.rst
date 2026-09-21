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

See :doc:`installation` for virtual environments and installing into the selected
notebook kernel with ``%pip``.

Or with conda:
https://www.digitalocean.com/community/tutorials/how-to-install-anaconda-on-ubuntu-18-04-quickstart


Running Python Notebooks
------------------------

Open :doc:`data_access` or :doc:`measurements`. Run **Setup**, then the section
you need with ``Shift + Enter``; unrelated sections do not need to run first.
Configure IDs using the discovery tables, and run **Close** when finished.
After closing, re-run Setup before making more API calls.

Markdown code blocks are opt-in recipes: copy one into a new code cell only
when needed. Imports, writes, and deletes are not executable cells.
Sphinx and Dash show saved outputs; download the notebook to run it in JupyterLab.

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
