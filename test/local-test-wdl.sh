#!/bin/bash

set -eux -o pipefail

womtool validate -i multithread-test.wdl.json cms2_main.wdl 
womtool validate -i cms2_empirical-test.wdl.json cms2_empirical.wdl
#womtool validate -i test.cms2_empirical.wdl.json cms2_empirical.wdl
womtool validate -i cms2_test_fetch.wdl.json cms2_test_fetch.wdl


