#!/bin/bash

set -eux -o pipefail

docker run -v $PWD:$PWD quay.io/ilya_broad/cms:webdriver-6dba18313775d137217c8a45c4bdd53d6b4e4441 bash /data/ilya-work/proj/dockstore-tool-cms2/misc/test_nre.sh
