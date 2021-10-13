#!/bin/bash

set -eu -o pipefail

./terra/terra_utils.py list_submissions --expand-subworkflows "$@"

