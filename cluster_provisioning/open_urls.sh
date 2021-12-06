#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)


# source IPs
source mozart_ip.sh
source metrics_ip.sh
source grq_ip.sh
source factotum_ip.sh

# open mozart urls
open -a "Google Chrome" https://${MOZART_IP}/hysds_ui/ \
                        https://${MOZART_IP}/mozart/api/v0.1/ \
                        https://${MOZART_IP}/grq/api/v0.1/ \
                        https://${MOZART_IP}/pele/api/v0.1/ \
                        https://${MOZART_IP}:15673 \
                        https://${MOZART_IP}/metrics \
                        https://opera-pcm-ci.jpl.nasa.gov
