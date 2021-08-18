#!/bin/bash

set -eu -o pipefail -x

womtool validate -i test-input.cms2_empirical.wdl.json cms2_empirical.wdl
