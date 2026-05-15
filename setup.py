from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

VERSION = '0.3.0'
DESCRIPTION = 'Python package providing an interface to the Gantner Instruments Cloud API'

# hotfix requirements to work for 3.10 - 3.14
required = [
    "certifi>=2022.6.15",
    "ipython>=8.12.0,<9; python_version < '3.11'",
    "ipython>=9.0.0; python_version >= '3.11'",
    "ipywidgets>=7.7",
    "matplotlib>=3.5",
    "numpy>=1.23",
    "pandas>=1.4",
    "pymysql>=1.0",
    "python-dotenv>=1.0",
    "python-dateutil>=2.8.2",
    "pytz>=2024.1",
    "requests>=2.31",
    "seaborn>=0.11.2",
    "sqlalchemy>=1.4",
    "websocket-client>=1.0",
]

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
    python_requires='>=3.10,<3.15',
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
