#!/bin/bash

set -eu -o pipefail -x

WOMTOOL_VERSION=63
rm -f womtool-${WOMTOOL_VERSION}.jar
wget --no-verbose https://github.com/broadinstitute/cromwell/releases/download/${WOMTOOL_VERSION}/womtool-${WOMTOOL_VERSION}.jar
java -jar womtool-${WOMTOOL_VERSION}.jar validate -i multithread-test.wdl.json Dockstore.wdl
rm -f womtool-${WOMTOOL_VERSION}.jar
