#!/bin/bash
#PKG=container-iems-sds_opera-pcm
#PKG=container-nasa_opera-sds-pcm
SDS_PKG_NAME=container-nasa_opera-sds-pcm
SDS_PKGS=( $(sds pkg ls | grep $SDS_PKG_NAME) )

# check args
if [ "$#" -eq 1 ]; then
  sds_version=$1
elif [ "$#" -eq 0 ]; then
  sds_version=""
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# package version to use for trigger rules
SDS_PKG=""

# determine SDS package version
if [ ! -z "${sds_version}" ]; then
  # Check to see if given version exists in the installed packages
  SDS_PKG_VERSIONS=( $(sds pkg ls | grep $SDS_PKG_NAME | cut -d ':' -f2) )
  for v in "${SDS_PKG_VERSIONS[@]}"; do
    if [ "${sds_version}" == "${v}" ]; then
      SDS_PKG=${SDS_PKG_NAME}:${v}
      echo "Found matching package ${SDS_PKG}"
      break
    fi
  done
  if [ -z "${SDS_PKG}" ]; then
    echo "Could not find any package with tag ${sds_version}. Installed packages:"
    for element in "${SDS_PKGS[@]}"; do
      echo "  ${element}"
    done
    exit 1
  fi
else
  if [ ${#SDS_PKGS[@]} -gt 1 ]; then
    echo "Too many versions installed for $SDS_PKG_NAME:"
    for element in "${SDS_PKGS[@]}"; do
       echo "  ${element}"
    done
    exit 1
  elif [ ${#SDS_PKGS[@]} -eq 0 ]; then
    echo "No installed version of $SDS_PKG_NAME."
    exit 1
  else
    SDS_PKG=${SDS_PKGS[0]}
  fi
fi

# extract version
MOZART_ES_PVT_IP=$(grep ^MOZART_ES_PVT_IP ~/.sds/config | awk '{print $2}')
if [ -z "${MOZART_ES_PVT_IP}" ]
then
  MOZART_ES_PVT_IP=$(grep -A1 ^MOZART_ES_PVT_IP ~/.sds/config | tail -n 1 | awk '{print $2}')
fi

MOZART_ES_URL="http://${MOZART_ES_PVT_IP}:9200"
SDS_VERSION=$(curl -s -XGET $MOZART_ES_URL/containers/_doc/${SDS_PKG}?_source=version | python -c "import json,sys;obj=json.load(sys.stdin);print(obj['_source']['version']);")

# write out rules import
TMP_RULES=/tmp/user_rules-cnm.json.$$
cat ~/.sds/rules/user_rules-cnm.json | sed "s/__TAG__/${SDS_VERSION}/" > $TMP_RULES

# import rules
sds -d rules import $TMP_RULES

# cleanup
rm -f $TMP_RULES
