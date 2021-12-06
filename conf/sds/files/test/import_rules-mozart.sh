#!/bin/bash
PKG=container-hysds_lightweight-jobs
SDS_PKGS=( $(sds pkg ls | grep $PKG) )
if [ ${#SDS_PKGS[@]} -gt 1 ]; then
  echo "Too many versions installed for $PKG:"
  for element in "${SDS_PKGS[@]}"; do
     echo "  ${element}"
  done
  exit 1
elif [ ${#SDS_PKGS[@]} -eq 0 ]; then
  echo "No installed version of $PKG."
  exit 1
fi

# extract version
MOZART_ES_URL="http://$(grep ^MOZART_ES_PVT_IP ~/.sds/config | awk '{print $2}'):9200"
VERSION=$(curl -s -XGET $MOZART_ES_URL/containers/_doc/${SDS_PKGS[0]}?_source=version | python -c "import json,sys;obj=json.load(sys.stdin);print(obj['_source']['version']);")

# write out rules import
TMP_RULES=/tmp/user_rules.json.$$
cat ~/.sds/rules/user_rules-mozart.json | sed "s/__LW_TAG__/${VERSION}/" > $TMP_RULES

# import rules
sds -d rules import $TMP_RULES

# cleanup
rm -f $TMP_RULES
