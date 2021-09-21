#!/bin/bash

set -eux -o pipefail

#conda env list
#which chromedriver-binary
#apt search libnss3
#python3 /data/ilya-work/proj/dockstore-tool-cms2/fetch_neutral_regions_nre.py --nre-params /data/ilya-work/proj/dockstore-tool-cms2/misc/test_nre.json --neutral-regions-tsv /data/ilya-work/proj/dockstore-tool-cms2/tmp/neutral_regions.test.tsv --nre-submitted-form-html  /data/ilya-work/proj/dockstore-tool-cms2/tmp/nre_submitted_form.test.html
python3 /data/ilya-work/proj/dockstore-tool-cms2/fetch_neutral_regions_nre.py --nre-params /data/ilya-work/proj/dockstore-tool-cms2/misc/test_nre.json --neutral-regions-tsv /data/ilya-work/proj/dockstore-tool-cms2/tmp/neutral_regions.test.tsv --neutral-regions-bed /data/ilya-work/proj/dockstore-tool-cms2/tmp/neutral_regions.test.bed --nre-submitted-form-html  /data/ilya-work/proj/dockstore-tool-cms2/tmp/nre_submitted_form.test.html
