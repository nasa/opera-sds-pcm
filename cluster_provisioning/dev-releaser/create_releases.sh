#!/bin/bash

# This script will create release tags for the OPERA PCM repo and its dependencies. It can
# be used to create the formal releases, nightly releases, or test releases.
#
set -ex

export GIT_OAUTH_TOKEN=$1
export OPERA_PCM_BRANCH=$2
export OPERA_PCM_RELEASE=$3
export PCM_COMMONS_BRANCH=$4
export PCM_COMMONS_RELEASE=$5
export PRODUCT_DELIVERY_BRANCH=$6
export PRODUCT_DELIVERY_RELEASE=$7
export LAMBDA_PACKAGE_BRANCH=$8
export LAMBDA_PACKAGE_RELEASE=$9
export BACH_API_BRANCH=${10}
export BACH_API_RELEASE=${11}
export BACH_UI_BRANCH=${12}
export BACH_UI_RELEASE=${13}
export OPERA_BACH_API_BRANCH=${14}
export OPERA_BACH_API_RELEASE=${15}
export OPERA_BACH_UI_BRANCH=${16}
export OPERA_BACH_UI_RELEASE=${17}

echo -e "\n\n========================================================="
echo "Creating ${OPERA_PCM_RELEASE} tag for opera-pcm repo."
echo "========================================================="
rm -rf opera-pcm/
git clone -b ${OPERA_PCM_BRANCH} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/opera-pcm.git
cd opera-pcm/
git branch -D ${OPERA_PCM_RELEASE} || :
sleep 1
git tag --delete ${OPERA_PCM_RELEASE} || :
sleep 1
git push origin :refs/heads/${OPERA_PCM_RELEASE}
sleep 1
git push origin :refs/tags/${OPERA_PCM_RELEASE}
sleep 1
git checkout -b ${OPERA_PCM_RELEASE}
sleep 1
git push origin refs/heads/${OPERA_PCM_RELEASE}:refs/heads/${OPERA_PCM_RELEASE}
sleep 1
git tag -a ${OPERA_PCM_RELEASE} -m "Tagging release ${OPERA_PCM_RELEASE}"
sleep 1
git push origin tag ${OPERA_PCM_RELEASE}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${PCM_COMMONS_RELEASE} tag for pcm_commons repo."
echo "========================================================="
rm -rf pcm_commons/
git clone -b ${PCM_COMMONS_BRANCH} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/pcm_commons.git
cd pcm_commons/
git branch -D ${PCM_COMMONS_RELEASE} || :
sleep 1
git tag --delete ${PCM_COMMONS_RELEASE} || :
sleep 1
git push origin :refs/heads/${PCM_COMMONS_RELEASE}
sleep 1
git push origin :refs/tags/${PCM_COMMONS_RELEASE}
sleep 1
git checkout -b ${PCM_COMMONS_RELEASE}
sleep 1
git push origin refs/heads/${PCM_COMMONS_RELEASE}:refs/heads/${PCM_COMMONS_RELEASE}
sleep 1
git tag -a ${PCM_COMMONS_RELEASE} -m "Tagging release ${PCM_COMMONS_RELEASE}"
sleep 1
git push origin tag ${PCM_COMMONS_RELEASE}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${PRODUCT_DELIVERY_RELEASE} tag for CNM_product_delivery repo."
echo "========================================================="
rm -rf CNM_product_delivery/
git clone -b ${PRODUCT_DELIVERY_BRANCH} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/CNM_product_delivery.git
cd CNM_product_delivery/
git branch -D ${PRODUCT_DELIVERY_RELEASE} || :
sleep 1
git tag --delete ${PRODUCT_DELIVERY_RELEASE} || :
sleep 1
git push origin :refs/heads/${PRODUCT_DELIVERY_RELEASE}
sleep 1
git push origin :refs/tags/${PRODUCT_DELIVERY_RELEASE}
sleep 1
git checkout -b ${PRODUCT_DELIVERY_RELEASE}
sleep 1
git push origin refs/heads/${PRODUCT_DELIVERY_RELEASE}:refs/heads/${PRODUCT_DELIVERY_RELEASE}
sleep 1
git tag -a ${PRODUCT_DELIVERY_RELEASE} -m "Tagging release ${PRODUCT_DELIVERY_RELEASE}"
sleep 1
git push origin tag ${PRODUCT_DELIVERY_RELEASE}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${LAMBDA_PACKAGE_RELEASE} tag for pcm-lambdas repo."
echo "========================================================="
rm -rf pcm-lambdas/
git clone -b ${LAMBDA_PACKAGE_BRANCH} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/pcm-lambdas.git
cd pcm-lambdas/
git branch -D ${LAMBDA_PACKAGE_RELEASE} || :
sleep 1
git tag --delete ${LAMBDA_PACKAGE_RELEASE} || :
sleep 1
git push origin :refs/heads/${LAMBDA_PACKAGE_RELEASE}
sleep 1
git push origin :refs/tags/${LAMBDA_PACKAGE_RELEASE}
sleep 1
git checkout -b ${LAMBDA_PACKAGE_RELEASE}
sleep 1
git push origin refs/heads/${LAMBDA_PACKAGE_RELEASE}:refs/heads/${LAMBDA_PACKAGE_RELEASE}
sleep 1
git tag -a ${LAMBDA_PACKAGE_RELEASE} -m "Tagging release ${LAMBDA_PACKAGE_RELEASE}"
sleep 1
git push origin tag ${LAMBDA_PACKAGE_RELEASE}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${BACH_API_RELEASE} tag for bach-api repo."
echo "========================================================="
# FIXME: This will need to be changed in the future when we figure out a better way to release the Bach API
rm -rf bach-api/
git clone -b ${BACH_API_BRANCH} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/bach-api.git
cd bach-api/
git branch -D ${BACH_API_RELEASE} || :
sleep 1
git tag --delete ${BACH_API_RELEASE} || :
sleep 1
git push origin :refs/heads/${BACH_API_RELEASE}
sleep 1
git push origin :refs/tags/${BACH_API_RELEASE}
sleep 1
git checkout -b ${BACH_API_RELEASE}
sleep 1
git push origin refs/heads/${BACH_API_RELEASE}:refs/heads/${BACH_API_RELEASE}
sleep 1
git tag -a ${BACH_API_RELEASE} -m "Tagging release ${BACH_API_RELEASE}"
sleep 1
git push origin tag ${BACH_API_RELEASE}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${BACH_UI_RELEASE} tag for bach-ui repo."
echo "========================================================="
# FIXME: This will need to be changed in the future when we figure out a better way to release the Bach UI
rm -rf bach-ui/
git clone -b ${BACH_UI_BRANCH} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/bach-ui.git
cd bach-ui/
git branch -D ${BACH_UI_RELEASE} || :
sleep 1
git tag --delete ${BACH_UI_RELEASE} || :
sleep 1
git push origin :refs/heads/${BACH_UI_RELEASE}
sleep 1
git push origin :refs/tags/${BACH_UI_RELEASE}
sleep 1
git checkout -b ${BACH_UI_RELEASE}
sleep 1
git push origin refs/heads/${BACH_UI_RELEASE}:refs/heads/${BACH_UI_RELEASE}
sleep 1
git tag -a ${BACH_UI_RELEASE} -m "Tagging release ${BACH_UI_RELEASE}"
sleep 1
git push origin tag ${BACH_UI_RELEASE}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${OPERA_BACH_API_RELEASE} tag for opera-bach-api repo."
echo "========================================================="
rm -rf opera-bach-api/
git clone -b ${OPERA_BACH_API_BRANCH} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/opera-bach-api.git
cd opera-bach-api/
git branch -D ${OPERA_BACH_API_RELEASE} || :
sleep 1
git tag --delete ${OPERA_BACH_API_RELEASE} || :
sleep 1
git push origin :refs/heads/${OPERA_BACH_API_RELEASE}
sleep 1
git push origin :refs/tags/${OPERA_BACH_API_RELEASE}
sleep 1
git checkout -b ${OPERA_BACH_API_RELEASE}
sleep 1
git push origin refs/heads/${OPERA_BACH_API_RELEASE}:refs/heads/${OPERA_BACH_API_RELEASE}
sleep 1
git tag -a ${OPERA_BACH_API_RELEASE} -m "Tagging release ${OPERA_BACH_API_RELEASE}"
sleep 1
git push origin tag ${OPERA_BACH_API_RELEASE}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${OPERA_BACH_UI_RELEASE} tag for opera-bach-ui repo."
echo "========================================================="
rm -rf opera-bach-ui/
git clone -b ${OPERA_BACH_UI_BRANCH} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/opera-bach-ui.git
cd opera-bach-ui/
git branch -D ${OPERA_BACH_UI_RELEASE} || :
sleep 1
git tag --delete ${OPERA_BACH_UI_RELEASE} || :
sleep 1
git push origin :refs/heads/${OPERA_BACH_UI_RELEASE}
sleep 1
git push origin :refs/tags/${OPERA_BACH_UI_RELEASE}
sleep 1
git checkout -b ${OPERA_BACH_UI_RELEASE}
sleep 1
git push origin refs/heads/${OPERA_BACH_UI_RELEASE}:refs/heads/${OPERA_BACH_UI_RELEASE}
sleep 1
git tag -a ${OPERA_BACH_UI_RELEASE} -m "Tagging release ${OPERA_BACH_UI_RELEASE}"
sleep 1
git push origin tag ${OPERA_BACH_UI_RELEASE}
cd ../

