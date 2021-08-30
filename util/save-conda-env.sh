#!/bin/bash

set -eux -o pipefail

conda env export > "conda-envs/${CONDA_DEFAULT_ENV}.yml"
conda list --explicit > "conda-envs/${CONDA_DEFAULT_ENV}.txt"
git add conda-envs
git commit -m '[skip ci] saved ${CONDA_DEFAULT_ENV}' conda-envs

