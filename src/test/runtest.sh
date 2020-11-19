#!/bin/bash

set -e -o pipefail -x

#./norm  --ihs --files /data/ilya-work/proj/dockstore-tool-cms2/cromwell-executions/compute_cms2_components/4e62eebd-4fbb-41b3-8938-645f5ff775ae/call-compute_cms2_components_for_one_replica/shard-0/execution/tpeds__model_default_112115_825am__demography_sel1_origin1__block_0__of_4__tar_gz__rep_0.ihs.out --save-bins tmp/mybins.xml
NBINS=100
VALGRIND=
BINS_FNAME=test/data/testbins.dat
BINS_LIST_FNAME=test/data/testbins.fromlist.dat
IHS_FILE1=test/data/test01.ihs.out
IHS_FILE2=test/data/test02.ihs.out
IHS_FILE_LIST=test/data/testlist.txt
${VALGRIND} ./norm  --ihs --files ${IHS_FILE1} --save-bins ${BINS_FNAME}
${VALGRIND} ./norm  --ihs --files ${IHS_FILE2} --load-bins ${BINS_FNAME}
diff "${IHS_FILE1}.${NBINS}bins.norm" "${IHS_FILE2}.${NBINS}bins.norm"

${VALGRIND} ./norm  --ihs --files @${IHS_FILE_LIST} --save-bins ${BINS_LIST_FNAME}
