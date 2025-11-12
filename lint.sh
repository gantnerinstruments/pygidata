#!/bin/bash
#echo "Running black (code formatting)..."
#black --safe --line-length 100 "$1"
echo "Running flake8 (pep8)..."
# ignore E203 and W503 conflicts of black and flake8
flake8 --max-line-length=120 --ignore=E203,W503,E701 src/gi_data/

# Check if mypy is set to "true" before running mypy
if [ "${mypy:-false}" = "true" ]; then
    echo "Running mypy (type hints)..."
    mypy "$1"
else
    echo "Skipping mypy (type hints)...(to run with mypy run: $ mpypy=true ./lint.sh [directory])"
fi
