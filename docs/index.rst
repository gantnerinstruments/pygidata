.. GI-Jupyter documentation master file, created by
   sphinx-quickstart on Wed Feb 10 16:55:10 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Pygidata (python)
===================

pygidata is a Python package providing interfaces to Gantner Instruments data
APIs. It centralises the project's functionality under a single package
namespace and includes two main subcomponents:

- ``pygidata.gi_data``: the primary module for REST/GraphQL and cloud access.
- ``pygidata.ginsutility``: an expert/local module that provides direct access to
  Highspeedport hardware via a local DLL. This is intended for
  advanced/local usage and requires additional, platform-specific setup.

To get started, follow the Installation section below. The documentation is
ordered for clarity: Installation, pygidata (overview of the package),
ginsutility (expert/local), and the JupyterLab usage notes.

.. toctree::
   :maxdepth: 2
   :caption: Developer documentation

   installation
   local_expert_access
   Usage
   overview
