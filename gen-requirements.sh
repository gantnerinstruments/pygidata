#!/usr/bin/env bash
set -euo pipefail

REQ_IN="./requirements.in"

# Generate requirements from package
pipreqs src/ --force --savepath "$REQ_IN"

# remove pinned version (which is latest)
sed -E -i 's/[[:space:]]*(==|>=|<=|!=|~=|>|<).*$//' "$REQ_IN"

# get the min. versions for the current used python version
pip-compile \
  --resolver=backtracking \
  --upgrade \
  --allow-unsafe \
  --strip-extras \
  --output-file requirements.txt \
  "$REQ_IN"

# set all requirements to be allowed above the min
sed -i 's/==/>=/g' requirements.txt

rm -f "$REQ_IN"

echo "requirements.txt generated with minimum floors for your Python version."
