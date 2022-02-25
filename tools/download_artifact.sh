#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)
 
 
cmdname=$(basename $0)


echoerr() { echo "$@" 1>&2; }


usage() {
  cat << USAGE >&2
Usage:
    $cmdname <artifactory_url>
    -m <mirror_root_url> | --mirror <mirror_root_url>
                                Root URL of artifactory mirror, e.g.
                                s3://nisar-dev/artifactory_mirror
    -b <artifactory_base_url> | --base <artifactory_base_url>
                                Root URL of artifactory being mirrored, e.g.
                                https://https://artifactory.jpl.nasa.gov/artifactory
    -h | --help                 Print help
    artifactory_url             Artifactory URL to download
	-k <artifactory_api_key> | --key <artifactory_api_key>
USAGE
  exit 1
}
 
 
# unset env vars
unset MIRROR
unset BASE
unset ART_URLS
unset ART_API_KEY

# parse options
PARAMS=""
while (( "$#" )); do
  case "$1" in
    --help|-h)
      usage
      shift 1
      ;;
    -m|--mirror)
      MIRROR=$2
      shift 2
      ;;
    -b|--base)
      BASE=$2
      shift 2
      ;;
	-k|--key)
	  ART_API_KEY=$2
	  shift 2
	  ;;
    --) # end argument parsing
      shift
      break
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *) # preserve positional arguments
      ART_URLS=("$@")
      break
      ;;
  esac
done


# if mirror was set, make sure base was set as well
if [ ! -z ${MIRROR+x} ] && [ -z ${BASE+x} ]; then
  echoerr "Error: you need to provide the -b|--base option to use the mirror at $MIRROR."
  usage
fi  


# loop over art urls and download
for art_url in "${ART_URLS[@]}"; do
  filename=$(basename $art_url)
  echo "Downloading $filename"

  # download from mirror first
  if [ ! -z ${MIRROR+x} ]; then
    mirror_url=$(echo $art_url | sed "s#${BASE}#${MIRROR}#")
    echo "First will try mirror url: $mirror_url"
    aws s3 cp $mirror_url .
    if [ "$?" -ne 0 ]; then
      echoerr "Error: failed to download from mirror: $mirror_url"
    else
      echo "Successfully downloaded $filename"
      continue
    fi
  fi

  echo "MIRROR = ${MIRROR}"
  echo "ART_API_KEY = ${ART_API_KEY}"
  echo "ART_URLS = ${ART_URLS}"

  # download from arifactory as last resort
  echo "Will try artifactory url: $art_url"
  if [ ! -z ${ART_API_KEY+X} ]; then
     curl -O --fail --silent -H "X-JFrog-Art-Api:${ART_API_KEY}" $art_url
  else
     curl -O --fail --silent $art_url 
  fi
  if [ "$?" -ne 0 ]; then
    echoerr "Error: failed to download from artifactory: $art_url"
    exit 1
  fi
  echo "Successfully downloaded $filename"
done
