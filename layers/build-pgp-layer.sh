#!/usr/bin/env bash
# Rebuilds pgp-layer.zip for the decrypt-vendor-file Lambda.
#
# The pins are deliberate. PGPy 0.5.4 is the newest release on PyPI and it
# calls cryptography.utils.register_interface, which was removed in
# cryptography 37. Anything newer fails at import with:
#   AttributeError: module 'cryptography.utils' has no attribute 'register_interface'
# cryptography 36.0.2 ships an abi3 wheel, so it runs on Python 3.12 despite
# predating it. See OPEN-ITEMS.md for the security trade-off.
#
# --platform / --python-version / --only-binary target the Lambda runtime
# (Amazon Linux, x86_64, Python 3.12) rather than whatever machine you run this on.
set -euo pipefail

rm -rf build pgp-layer.zip
mkdir -p build/python

pip install --target build/python \
  --platform manylinux2014_x86_64 \
  --python-version 3.12 \
  --implementation cp \
  --only-binary=:all: \
  "pgpy==0.5.4" "cryptography==36.0.2"

find build/python -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
rm -rf build/python/bin

cd build && zip -qr ../pgp-layer.zip python && cd ..
rm -rf build
echo "built pgp-layer.zip ($(du -h pgp-layer.zip | cut -f1))"
