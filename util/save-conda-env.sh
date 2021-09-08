#!/bin/bash

#
# Script: save-conda-env.sh
#
# Save the conda environment used for CMS2 development work.
#

set -eux -o pipefail

conda env export > "conda-envs/${CONDA_DEFAULT_ENV}.yml"
conda list --explicit > "conda-envs/${CONDA_DEFAULT_ENV}.txt"
cp ../../scripts/update-vngs-conda-env conda-envs/ || true
chmod u+x conda-envs/update-vngs-conda-env || true
git add conda-envs
git commit -m "[skip ci] saved ${CONDA_DEFAULT_ENV}" conda-envs
