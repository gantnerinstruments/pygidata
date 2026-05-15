# pygidata

This repository provides one Python package, `pygidata`, for Gantner Instruments data access.

Inside `pygidata`, the `ginsutility` module is included for expert/local Highspeedport access via `GInsUtility.dll`.
The logic remains separated by module scope (`gi_data` for API access, `ginsutility` for local DLL workflows).

## Choose the right package

- Use `pygidata`/`gi_data` for standard backend/API integrations and GI.cloud workflows.
- Use `ginsutility` only for local expert scenarios that require the Highspeedport DLL.
- Do not use `ginsutility` as GI.cloud-only integration path.

## Usage

### Install `pygidata` from PyPI

```bash 
pip install pygidata
```

### Install with local expert module dependencies

Use this only if you need local Highspeedport access.

> **Required for `ginsutility` examples**
>
> All examples in `src/ginsutility/examples` require:
> 1. `pygidata` installed with extras: `pip install "pygidata[ginsutility]"`
> 2. local access to `GInsUtility.dll` or on Linux `libGInsUtility.so` (typically installed with GI.bench / Q.core).

```bash
pip install "pygidata[ginsutility]"
```

Editable install from this repository:

```bash
pip install -e .[ginsutility]
```

Quick check after installation:

```bash
python -c "import ginsutility; print('ginsutility import OK')"
```

If this import fails, run the examples only after fixing the ginsutility installation and DLL availability.

Import module in python script and call functions.

A detailed description of `pygidata` can be found
under [docs](https://github.com/gantner-instruments/gimodules-python/blob/main/docs/Usage.ipynb) or in the
Gantner Documentation.

```python
from gi_data.dataclient import GIDataClient
import os

PROFILES = {
    "qstation": {
        "base": os.getenv("GI_QSTATION_BASE", "http://10.1.50.36:8090"),
        "auth": {"username": os.getenv("GI_QSTATION_USER", "admin"),
                 "password": os.getenv("GI_QSTATION_PASS", "admin")},
    },
    "cloud": {
        "base": os.getenv("GI_CLOUD_BASE", "https://demo.gi-cloud.io"),
        "auth": {"access_token": os.getenv("GI_CLOUD_TOKEN", "")},
    },
}

ACTIVE_PROFILE = os.getenv("GI_PROFILE", "qstation")

def get_client(profile: str = ACTIVE_PROFILE) -> GIDataClient:
    cfg = PROFILES[profile]
    if cfg["auth"].get("access_token"):
        return GIDataClient(cfg["base"], access_token=cfg["auth"]["access_token"]) 
    return GIDataClient(cfg["base"],
                        username=cfg["auth"].get("username"),
                        password=cfg["auth"].get("password"))

client = get_client()

```


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
