#!/bin/bash

set -e -o pipefail -x

cd component_stats
docker build -t local:test01 .
