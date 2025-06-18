from setuptools import setup, find_packages
import codecs
import os
import sys

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

VERSION = '0.3.0'
DESCRIPTION = 'Python package providing an interface to the Gantner Instruments Cloud API'
LONG_DESCRIPTION = ''

if sys.version_info[:2] == (3, 7):
    req_file = 'requirements37.txt'
elif sys.version_info[:2] == (3, 12):
    req_file = 'requirements312.txt'
else:
    req_file = 'requirements.txt'

with open(req_file) as f:
    required = f.read().splitlines()

# Setting up
setup(
    name="gimodules",
    version=VERSION,
    author="Gantner Instruments GmbH",
    author_email="",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(),
    install_requires=required,
    keywords=['python'],
    python_requires='>=3.7',
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
