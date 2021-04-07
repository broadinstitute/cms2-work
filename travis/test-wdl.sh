#!/bin/bash

set -eu -o pipefail -x

rm -f womtool-58.jar
wget --no-verbose https://github.com/broadinstitute/cromwell/releases/download/58/womtool-58.jar
java -jar womtool-58.jar validate -i multithread-test.wdl.json Dockstore.wdl
rm -f womtool-58.jar
