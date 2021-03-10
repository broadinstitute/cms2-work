#!/usr/bin/bash

set -e -o pipefail

cd bioconda-recipes
./bootstrap.py /tmp/miniconda
source ~/.config/bioconda/activate
bioconda-utils build --docker --mulled-test --packages selscan


