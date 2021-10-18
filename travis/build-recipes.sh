#!/usr/bin/bash

set -eu -o pipefail -x

cd bioconda-recipes
./bootstrap.py /tmp/miniconda
source ~/.config/bioconda/activate
bioconda-utils build --docker --mulled-test --packages selscan
cd ..
ls -lR /tmp/miniconda/miniconda/conda-bld
cp -prv /tmp/miniconda/miniconda/conda-bld .

echo "Successfully copied selscan build"

#echo "${QUAY_CMS_TOKEN}" | docker login -u="${QUAY_IO_USER}" --password-stdin quay.io

#DOCKER_TAG="${TRAVIS_BRANCH}--${TRAVIS_COMMIT}"

#docker build -t quay.io/${QUAY_IO_USER}/cms:${DOCKER_TAG} .
#docker push quay.io/${QUAY_IO_USER}/cms:${DOCKER_TAG}





