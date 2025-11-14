#!/usr/bin/env bash
set -euo pipefail

REQ_IN="./requirements.in"
REQ_OUT="./requirements.txt"

# Generate requirements.in based only on imports
pipreqs src/gi_data --force --savepath "$REQ_IN"

# Remove any version specifiers
sed -E -i 's/[[:space:]]*(==|>=|<=|!=|~=|>|<).*$//' "$REQ_IN"

# Copy into requirements.txt
cp "$REQ_IN" "$REQ_OUT"

# Cleanup
rm -f "$REQ_IN"

echo "requirements.txt generated without version pins."
