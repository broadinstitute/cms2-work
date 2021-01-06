#!/usr/bin/bash

set -e -o pipefail -x

TRAVIS_BRANCH=$(git branch --show-current) TRAVIS_BUILD_DIR=$PWD TRAVIS_REPO_SLUG=notestaff/dockstore-tool-cms2 TRAVIS_COMMIT=$(git rev-parse HEAD) ./travis/create-staging-branch.sh



