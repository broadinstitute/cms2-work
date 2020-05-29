#!/bin/bash

set =e -o pipefail -x

docker run -v $PWD:/user-data quay.io/ilya_broad/docker-tool-cosi2:latest /bin/bash -c coalescent "$@"
