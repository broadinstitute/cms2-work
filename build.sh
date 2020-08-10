#!/bin/bash

set -e -o pipefail -x

DOCKER_HOST=quay.io
DOCKER_REPO=broadinstitute/cms2
DOCKER_TAG="${TRAVIS_EVENT_TYPE}.${TRAVIS_JOB_ID}.${TRAVIS_COMMIT}"
DOCKER_IMAGE_NAME_AND_TAG="${DOCKER_HOST}/${DOCKER_REPO}:${DOCKER_TAG}"

echo ${QUAY_DOCKER_PASSWORD} | docker login -u="${QUAY_DOCKER_USER}" --password-stdin quay.io

cd component_stats
docker build -t ${DOCKER_IMAGE_NAME_AND_TAG} .
docker push ${DOCKER_IMAGE_NAME_AND_TAG}

echo "----------------"
echo "DONE WITH BUILD"
echo "----------------"
