#!/bin/sh
# Rebuild vendor/ with the libraries spotify_sync uses (ss-deemx + deezer-py),
# as wheels for the DroppedNeedle image: python:3.13 slim, linux.
# Usage: ./vendor.sh            (x86_64, e.g. Unraid)
#        ARCH=aarch64 ./vendor.sh
set -eu
cd "$(dirname "$0")"
ARCH="${ARCH:-x86_64}"
rm -rf vendor
python3 -m pip install --quiet --target vendor \
  --platform "manylinux2014_${ARCH}" --platform "manylinux_2_17_${ARCH}" \
  --python-version 3.13 --implementation cp --only-binary=:all: \
  ss-deemx deezer-py
rm -rf vendor/bin vendor/share
echo "vendor/ rebuilt for linux ${ARCH}, cp313"
