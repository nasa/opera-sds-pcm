#!/bin/bash

set -e

source /usr/local/bin/_activate_current_env.sh
micromamba activate update

set -x

python /update/disp-s1-bperp-update.py "${@}"
