# gimodules-python

# Usage

### Install from PyPi

```bash 
pip install gimodules
```

Import module in python script and call functions.

A detailed description of the package and other APIs can be found in the Gantner Documentation.

```python
from gimodules.cloudconnect.cloud_request import CloudRequest

cloud = CloudRequest()
cloud.login(url='https://example.gi-cloud.io', access_token='TOKEN') # Create a token under Tools -> Monitor
cloud.get_all_stream_metadata()
```


# Development

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
pytest`
```

## Requirements

When starting to develop you can install the requirements with:

```bash
pip install -r requirements.txt
```

When you add new components and the requirements change, 
you can find out what packages are needed by the project and create new requirements:

```bash
pipreqs .
```

To create project all current packages installed in your venv for requirements automatically:

```bash
pip3 freeze > requirements.txt
```
---

**_NOTE:_** Remove the old gimodules version from requirements.txt before pushing (dependency conflict).

---

## Documentation

The documentation is being built as extern script in the GI.Sphinx repository.

The documentation consists of partially generated content. 
To **generate .rst files** from the code package, run the following command from the root directory of the project:

```bash
sphinx-apidoc -o docs/source/ gimodules
```

To **build the documentation**, run the following command:

```bash
cd docs
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

#### TODO
1. [x] lint.sh diff between black and flake8 etc
2. [x] Usage with python 3.12
3. [x] Doc requirements 
