#!/bin/bash

set -eu -o pipefail -x

CROMWELL_RELEASE=62
rm -f womtool-${CROMWELL_RELEASE}.jar
wget --no-verbose https://github.com/broadinstitute/cromwell/releases/download/${CROMWELL_RELEASE}/womtool-${CROMWELL_RELEASE}.jar
java -jar womtool-${CROMWELL_RELEASE}.jar validate -i multithread-test.wdl.json Dockstore.wdl
rm -f womtool-${CROMWELL_RELEASE}.jar

