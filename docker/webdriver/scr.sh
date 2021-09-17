#!/bin/bash

set -eux -o pipefail

python3 /data/ilya-work/proj/dockstore-tool-cms2/fetch_neutral_regions_nre.py --input-json /data/ilya-work/proj/dockstore-tool-cms2/tmp/i.json --out-nre-results-tsv /data/ilya-work/proj/dockstore-tool-cms2/tmp/out.tsv
