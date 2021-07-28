#!/bin/bash

set -eu -o pipefail -x

./fetch_empirical_regions.py --empirical-regions-bed /data/ilya-work/proj/dockstore-tool-cms2/test/pos_con_sorted.hg19.bed --genetic-maps-tar-gz /data/ilya-work/proj/dockstore-tool-cms2/tmp/t8/hg19_maps.tar.gz --pops-outgroups-json

