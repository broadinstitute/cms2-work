#!/bin/bash

set -eu -o pipefail -x

mkdir -p tmp
#time ./fetch_empirical_regions.py --empirical-regions-bed "${PWD}/tmp/empirical-regions-test.bed" --genetic-maps-tar-gz /data/ilya-work/proj/dockstore-tool-cms2/tmp/t8/hg19_maps.tar.gz --superpop-to-representative-pop "${PWD}/misc/superpop_to_representative_pop.json" --tmp-dir /data/ilya-work/proj/dockstore-tool-cms2/tmp/newtmp >& tmp/out.txt
time ./fetch_empirical_regions.py --empirical-regions-bed "/data/ilya-work/proj/dockstore-tool-cms2/test/pos_con_sorted.hg19.bed" --genetic-maps-tar-gz /data/ilya-work/proj/dockstore-tool-cms2/tmp/t8/hg19_maps.tar.gz --superpop-to-representative-pop "${PWD}/misc/superpop_to_representative_pop.json" --tmp-dir /data/ilya-work/proj/dockstore-tool-cms2/tmp/newtmp >& tmp/out.longer.txt

