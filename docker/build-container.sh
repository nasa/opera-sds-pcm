#!/bin/bash
if (( $# < 2 ))
then
    echo "[ERROR] Build script requires REPO and TAG"
    exit 1
fi
#Setup input variables
DIR=$(dirname ${0})
REPO="${1}"
TAG="${2}"
shift
shift

HYSDS_COMMONS_DIR=/home/hysdsops/verdi/ops/hysds_commons/hysds_commons

# detect branch or tag
IS_TAG=0
DESCRIBED_TAG=$(git describe --exact-match --tags HEAD)
if (( $? == 0 ))
then
    IS_TAG=1
fi
echo "[CI] Is checkout a tag: ${IS_TAG} ${DESCRIBED_TAG}"

#Get last log message
LAST_LOG=$(git log -1)
echo "[CI] Last log: ${LAST_LOG}"

# extract any flags from last log
SKIP_IMAGE_BUILD_FLAG=0
GREP_SKIP_IMAGE_BUILD=$(echo $LAST_LOG | grep -i SKIP_IMAGE_BUILD)
if (( $? == 0 ))
then
    SKIP_IMAGE_BUILD_FLAG=1
fi
echo "[CI] Skip image build flag: ${SKIP_IMAGE_BUILD_FLAG}"

# skip image build? only if checkout is a branch and SKIP_IMAGE_BUILD is set
SKIP_IMAGE_BUILD=0
if [[ $IS_TAG -eq 0 ]] && [[ $SKIP_IMAGE_BUILD_FLAG -eq 1 ]]
then
    SKIP_IMAGE_BUILD=1
    echo "[CI] Image build will be skipped."
fi

#Use git to cleanly remove any artifacts
git clean -ffdq -e repos
if (( $? != 0 ))
then
   echo "[ERROR] Failed to force-clean the git repo"
   exit 3
fi

#Run the validation script here
${HYSDS_COMMONS_DIR}/validate.py docker/
if (( $? != 0 ))
then
    echo "[ERROR] Failed to validate hysds-io and job-spec JSON files under ${REPO}/docker. Cannot continue."
    exit 1
fi
if [ -f docker/setup.sh ]
then
    docker/setup.sh
    if (( $? != 0 ))
    then
        echo "[ERROR] Failed to run docker/setup.sh"
        exit 2
    fi
fi
# Loop across all Dockerfiles, build and ingest them
for dockerfile in docker/Dockerfile*
do
    dockerfile=${dockerfile#docker/}
    #Get the name for this container, from repo or annotation to Dockerfile
    NAME=${REPO}
    if [[ "${dockerfile}" != "Dockerfile" ]]
    then
        NAME=${dockerfile#Dockerfile.}
    fi
    #Setup container build items
    PRODUCT="container-${NAME}:${TAG}"
    #Docker tags must be lower case
    PRODUCT=${PRODUCT,,}
    TAR="${PRODUCT}.tar"
    GZ="${TAR}.gz" 
    #Remove previous container if exists
    PREV_ID=$(docker images -q $PRODUCT)
    if (( ${SKIP_IMAGE_BUILD} == 0 )); then
        if [[ ! -z "$PREV_ID" ]]
        then
            echo "[CI] Removing current image for ${PRODUCT}: ${PREV_ID}"
            docker system prune -f
            docker rmi -f $(docker images | grep $PREV_ID | awk '{print $1":"$2}')
        fi

        #Build container
        echo "[CI] Build for: ${PRODUCT} and file ${NAME}"

        #Build docker container
        echo " docker build --no-cache --rm --force-rm -f docker/${dockerfile} -t ${PRODUCT} $@ ."
        docker build --no-cache --rm --force-rm -f docker/${dockerfile} -t ${PRODUCT} "$@" .
        if (( $? != 0 ))
        then
            echo "[ERROR] Failed to build docker container for: ${PRODUCT}" 1>&2
            exit 4
        fi

        ##Save out the docker image
        #docker save -o ./${TAR} ${PRODUCT}
        #if (( $? != 0 ))
        #then
        #    echo "[ERROR] Failed to save docker container for: ${PRODUCT}" 1>&2
        #    exit 5
        #fi

        ##GZIP it
        #pigz -f ./${TAR}
        #if (( $? != 0 ))
        #then
        #    echo "[ERROR] Failed to GZIP container for: ${PRODUCT}" 1>&2
        #    exit 6
        #fi

        # get image digest (sha256)
        digest=$(docker inspect --format='{{index .Id}}' ${PRODUCT} | cut -d'@' -f 2)
    fi
    #Attempt to remove dataset
    #rm -f ${GZ}
done

exit 0
