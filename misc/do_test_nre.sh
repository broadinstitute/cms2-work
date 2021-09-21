#!/bin/bash

set -eux -o pipefail

docker run -v $PWD:$PWD quay.io/ilya_broad/cms:webdriver-0.1 bash /data/ilya-work/proj/dockstore-tool-cms2/misc/test_nre.sh
