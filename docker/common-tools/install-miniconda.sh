#!/bin/bash
set -e -o pipefail

MINICONDA_VERSION="4.11.0"
MINICONDA_PY_VERSION="py38"
MINICONDA_INSTALLER="Miniconda3-${MINICONDA_PY_VERSION}_${MINICONDA_VERSION}-Linux-x86_64.sh"
#MINICONDA_URL="https://repo.anaconda.com/miniconda/${MINICONDA_INSTALLER}"
MINICONDA_URL="https://repo.anaconda.com/miniconda/${MINICONDA_INSTALLER}"

# download and run miniconda installer script
curl -sSL $MINICONDA_URL > "/tmp/${MINICONDA_INSTALLER}"
chmod a+x "/tmp/${MINICONDA_INSTALLER}"
/tmp/${MINICONDA_INSTALLER} -b -f -p "$MINICONDA_PATH"
rm /tmp/${MINICONDA_INSTALLER}

PATH="$MINICONDA_PATH/bin:$PATH"
hash -r
conda config --set always_yes yes --set changeps1 no
conda config --set channel_priority strict
conda config --add channels r
conda config --add channels defaults
conda config --add channels bioconda
conda config --add channels conda-forge
#conda config --add channels ilya239
#conda config --add channels notestaff
conda config --set auto_update_conda false
conda init bash
conda update -y -n base -c defaults conda
conda clean -y --all
