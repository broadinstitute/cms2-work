#!/usr/bin/bash

set -eu -o pipefail -x

cd bioconda-recipes
./bootstrap.py /tmp/miniconda
source ~/.config/bioconda/activate
bioconda-utils build --docker --mulled-test --packages selscan
cd ..
ls -lR /tmp/miniconda/miniconda/conda-bld
cp -prv /tmp/miniconda/miniconda/conda-bld .

docker build -t test01 .
docker run test01 selscan --help 2>&1 | grep 'selscan v1.3.0'



