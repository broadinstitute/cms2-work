#!/bin/bash

set -e -o pipefail -x

. "/ilya/miniconda3/etc/profile.d/conda.sh"

conda activate master_env_v178_py36
PYTHONPATH=/data/ilya-work/benchmarks/viral-ngs-benchmarks/viral-ngs:$PYTHONPATH python3 /data/ilya-work/benchmarks/viral-ngs-benchmarks/viral-ngs/file_utils.py json_to_org $1
conda deactivate
