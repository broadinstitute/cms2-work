#!/usr/bin/bash

set -e -o pipefail -x

TMP_MINICONDA=$(mktemp -d "${TMPDIR:-/tmp/}miniconda.XXXXXXXXXXXX")
#TMP_MINICONDA=/tmp/miniconda
git submodule init bioconda-recipes
git submodule update --recursive bioconda-recipes
cd bioconda-recipes
pwd
ls -l
rm -rf ${TMP_MINICONDA}
./bootstrap.py ${TMP_MINICONDA}
source ~/.config/bioconda/activate
bioconda-utils build --docker --mulled-test --packages selscan
cd ..
ls -lR ${TMP_MINICONDA}/miniconda/conda-bld
cp -prv ${TMP_MINICONDA}/miniconda/conda-bld .

echo "Successfully copied selscan build"

#echo "${QUAY_CMS_TOKEN}" | docker login -u="${QUAY_IO_USER}" --password-stdin quay.io

#DOCKER_TAG="${TRAVIS_BRANCH}--${TRAVIS_COMMIT}"

#docker build -t quay.io/${QUAY_IO_USER}/cms:${DOCKER_TAG} .
#docker push quay.io/${QUAY_IO_USER}/cms:${DOCKER_TAG}
