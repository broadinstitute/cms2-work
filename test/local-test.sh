#!/bin/bash

set -eu -o pipefail -x

./fetch_empirical_regions.py --empirical-regions-bed /data/ilya-work/proj/dockstore-tool-cms2/tmp/empirical-regions-test.bed --genetic-maps-tar-gz /data/ilya-work/proj/dockstore-tool-cms2/tmp/t8/hg19_maps.tar.gz --pops-outgroups-json /data/ilya-work/proj/dockstore-tool-cms2/tmp/outpops.json --tmp-dir /data/ilya-work/proj/dockstore-tool-cms2/tmp/newtmp


