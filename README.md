# pygidata

# Usage

### Install from PyPi

```bash 
pip install pygidata
```

Import module in python script and call functions.

A detailed description of the package and other APIs can be found under [docs/](docs/Usage.ipynb) or in the
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
Edit setup.py version number and create a release.
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

The documentation is being built as extern script in the GI.Sphinx repository.

The documentation consists of partially generated content. 
To **generate .rst files** from the code package, run the following command from the root directory of the project:

```bash
sphinx-apidoc -o docs/source/ src
```
You need pandoc installed on the system itself first to build:

```bash
sudo apt install pandoc
```

Then, to **build the documentation**, run the following commands:

```bash
cd docs
sudo apt update
pip install -r requirements.txt
make html
```

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