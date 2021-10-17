#!/usr/bin/bash

set -eux -o pipefail

cd bioconda-recipes
./bootstrap.py /tmp/miniconda
source ~/.config/bioconda/activate
bioconda-utils build --docker --mulled-test --packages selscan
cd ..
ls -lR /tmp/miniconda/miniconda/conda-bld
cp -prv /tmp/miniconda/miniconda/conda-bld .

echo "${QUAY_IO_PW}" | docker login -u="${QUAY_IO_USER}" --password-stdin quay.io

git --version
export GIT_DESCRIBE=$(git describe --always --long --abbrev=40)
echo "GIT_DESCRIBE is ${GIT_DESCRIBE}"
DOCKER_TAG="${TRAVIS_BRANCH}--${GIT_DESCRIBE}"
echo "DOCKER_TAG is ${DOCKER_TAG}"

docker build -t quay.io/${QUAY_IO_USER}/cms:${DOCKER_TAG} .
docker push quay.io/${QUAY_IO_USER}/cms:${DOCKER_TAG}
