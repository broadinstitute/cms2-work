#!/bin/bash

set -eux -o pipefail

womtool validate -i multithread-test.wdl.json cms2_main.wdl 
womtool validate -i cms2_empirical-test.wdl.json cms2_empirical.wdl
