# pygidata

`pygidata` provides Python access to Gantner Instruments data on GI.cloud,
GI.bench, Q.core, and Q.station. Install **pygidata** and import **gi_data**
for the REST/GraphQL client. Python 3.10-3.14 is supported.

## Quick start

```bash
python -m pip install pygidata
```

Set `GI_BASE_URL` and `GI_TOKEN` in your environment.
[docs/.env.example](docs/.env.example) contains the base URL and credentials; see
[Installation](docs/installation.rst) for opt-in loading.

List the available streams:

```python
import logging
import os

from gi_data.dataclient import GIDataClient

base_url = os.environ["GI_BASE_URL"]
GIDataClient.set_log_level(logging.WARNING)

with GIDataClient(
        base_url,
        access_token=os.environ["GI_TOKEN"],
) as client:
    for source in client.list_buffer_sources():
        print(source.id, source.name)
```

For username/password authentication, replace the client construction with
`GIDataClient(base_url, username=os.environ["GI_USER"],
password=os.environ["GI_PASSWORD"])` and unset `GI_TOKEN`. A local target URL
may look like `http://your-controller:8090`; use HTTPS for remote connections.
These examples read environment variables, not `.env` files.

## User guide

| I want to...                                     | Start here                                                                |
|--------------------------------------------------|---------------------------------------------------------------------------|
| Read buffer/online data, plot, or exchange files | [Data access](docs/data_access.ipynb)                                     |
| Read recordings or manage measurement metadata   | [Measurements](docs/measurements.ipynb)                                   |
| Look up return types and backend conventions     | [API notes](docs/api_reference.rst)                                       |
| Set up Python or the notebook kernel             | [Installation](docs/installation.rst) and [JupyterLab](docs/overview.rst) |
| Access Highspeedport through a local DLL         | [Local expert access](docs/local_expert_access.rst)                       |

Each notebook is standalone: run Setup, then the workflow you need.
Imports, writes, and deletes are opt-in code blocks, not executable cells.

### Local Highspeedport access

`ginsutility` is included for expert/local workflows, not as a GI.cloud-only
integration path. The examples in `src/ginsutility/examples` require both the
optional Python dependencies and `GInsUtility.dll` (Windows) or
`libGInsUtility.so` (Linux), typically supplied with GI.bench / Q.core.

```bash
python -m pip install "pygidata[ginsutility]"
```

For an editable development installation, use
`python -m pip install -e ".[ginsutility]"`.

# Development

### Used as submodule in
* gi-sphinx
* gi-jupyterlab
* gi-analytics-examples

### Information on how to manually distribute this package can be found here

https://packaging.python.org/en/latest/tutorials/packaging-projects/

**Hint:** If you are debugging the source code with a jupyter notebook, run this code in the `first cell` to enable autoreloading source code changes.

```bash
%load_ext autoreload
%autoreload 2
```

## Distribute with CI / CD

Edit pyproject.toml version number and create a release.
-> Creating a release will trigger the workflow to push the package to PyPi

## Tests

run tests locally:

```bash
pipenv run test -v
```

or 

```bash
pytest
```

### Generate loose requirements

**Do this in a bash shell using the lowest version you want to support!**

Install uv to easily install all needed python versions (coss-platform)

``` bash
pip install uv
```

```bash
python -m pip install -U pip tox
```

```bash
python -m pip install pip-tools
```
```bash
python -m pip install pipreqs
```


To ensure we support multiple python versions we don't want to pin every dependency.
Instead, we pin everything on the lowest version (that we support) and make
it loose for every version above.

from root package dir (/gimodules-python)

```bash
./gen-requirements.sh
```

#### Ensure python-package version compatibility

```bash
uv python install 3.10 3.11 3.12 3.13 3.14
```

Now run for all envs

```bash
tox
```

of for a specific version only -> look what you defined in pyproject.toml

```bash
tox -e py310
```
---

**_NOTE:_** Remove the old gimodules version from requirements.txt before pushing (dependency conflict).

---

## Documentation

Build docs from repository root.

`nbsphinx` requires a local Pandoc installation.

### Install Pandoc

Windows (winget):

```powershell
winget install --id JohnMacFarlane.Pandoc -e
```

Linux (Debian/Ubuntu):

```bash
sudo apt update
sudo apt install -y pandoc
```

### Windows (PowerShell)

```powershell
python -m pip install -r .\docs\requirements.txt
Set-Location .\docs
.\make.bat html
```

HTML output is written to `docs\_build\html`.

### Linux/macOS

```bash
python -m pip install -r ./docs/requirements.txt
make -C docs html
```

HTML output is written to `docs/_build/html`.

## Linting / Type hints

This project follows the codestyle PEP8 and uses the linter flake8 (with line length = 100).

You can format and check the code using lint.sh:
    
```bash
./lint.sh [directory/]
```

Type hints are highly recommended.
Type hints in Python specify the expected data types of variables,
function arguments, and return values, improving code readability,
catching errors early, and aiding in IDE autocompletion.

To include type hints in the check:

```bash
mpypy=true ./lint.sh [directory])
```
