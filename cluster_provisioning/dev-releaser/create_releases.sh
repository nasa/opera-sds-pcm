#!/usr/bin/env bash

#######################################################################
# This script will create release tags for the OPERA PCM repo and its dependencies. It can
# be used to create the formal releases, nightly releases, or test releases.
#######################################################################
set -e
# DEV: uncomment when debugging
#set -x

cmdname=$(basename $0)

######################################################################
# Function definitions
######################################################################

echoerr() { if [[ $QUIET -ne 1 ]]; then echo "$@" 1>&2; fi }

# Output script usage information.
usage()
{
    cat << USAGE >&2
Usage:
  $cmdname [options]
Examples:
  $cmdname --default-release=3.0.0.-rc.4.0 --tag-msg="Tagging to support the OPERA PCM Release 0 Release Candidate 0 delivery" ...
Options:
      --default-release          REQUIRED. The version number (tag) for the builds.
      --git-oauth-token          REQUIRED. Internal Github Enterprise OAuth token for authenticating git commands.
      --pub-git-oauth-token      REQUIRED. (Public) Github OAuth token for authenticating git commands.
      --tag-msg                  Override the default tag messages. Typically a message like "Tagging to support the
                                 OPERA PCM Release X Release Candidate Y delivery"
      --delete-on-conflict       Delete any existing tag that may conflict with this release.
                                 Useful for re-creating a release.
      --slow                     Enables brief sleeps throughout the script to aid in live monitoring.
      --in-automation            Use when in automation. Otherwise will invoke "gh auth login"
      --default-branch           Override the default branch to use for tagging. Typically "develop".
                                 Defaults to "develop".
      --pcm-branch               PCM branch to use for tagging. Typically "develop".
                                 Defaults to "develop".
      --pcm-release              The version number (tag) for the PCM build.
      --pcm-commons-branch       PCM commons branch to use for tagging. Typically "develop".
                                 Defaults to "develop".
      --pcm-commons-release      The version number (tag) for the PCM commons build.
      --product-delivery-branch  Product delivery branch to use for tagging. Typically "develop".
                                 Defaults to "develop".
      --product-delivery-release The version number (tag) for the product delivery build.
      --lambda-package-branch    Lambda package branch to use for tagging. Typically "develop".
                                 Defaults to "develop".
      --lambda-package-release   The version number (tag) for the lambda package build.
      --bach-api-branch          bach-api branch to use for tagging. Typically "develop".
                                 Defaults to "develop".
      --bach-api-release         The version number (tag) for the bach-api build.
      --bach-ui-branch           bach-ui branch to use for tagging. Typically "develop".
                                 Defaults to "develop".
      --bach-ui-release          The version number (tag) for the bach-ui build.
USAGE
}

######################################################################
# Argument parsing
######################################################################

# parse args
if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

for i in "$@"; do
  case $i in
    -h|--help)
      usage
      exit 0
      shift
      ;;
    *)
      # PASS
      ;;
  esac
done

# initialize required default params
default_branch="develop"
default_release=""

# override default params
for i in "$@"; do
  case $i in
    --default-branch=*)
      default_branch="${i#*=}"
      shift
      ;;
    --default-release=*)
      default_release="${i#*=}"
      shift
      ;;
    --git-oauth-token=*)
      GIT_OAUTH_TOKEN="${i#*=}"
      shift
      ;;
    --pub-git-oauth-token=*)
      PUB_GIT_OAUTH_TOKEN="${i#*=}"
      shift
      ;;
    *)
      # PASS
      ;;
  esac
done

# check if required params unset or empty
if [[ -z $default_release ]]; then
  echo Missing required arg \"--default-release\". See usage.
  exit 1
fi
if [[ -z $GIT_OAUTH_TOKEN ]]; then
  echo Missing required arg \"--git-oauth-token\". See usage.
  exit 1
fi
if [[ -z $PUB_GIT_OAUTH_TOKEN ]]; then
  echo Missing required arg \"--pub-git-oauth-token\". See usage.
  exit 1
fi

# defaults for optional args
pcm_branch=${default_branch}
pcm_commons_branch=${default_branch}
product_delivery_branch=${default_branch}
lambda_package_branch=${default_branch}
bach_api_branch=${default_branch}
bach_ui_branch=${default_branch}

pcm_release=${default_release}
pcm_commons_release=${default_release}
product_delivery_release=${default_release}
lambda_package_release=${default_release}
bach_api_release=${default_release}
bach_ui_release=${default_release}

for i in "$@"; do
  case $i in
    --slow)
      slow=1
      shift
      ;;
    --in-automation)
      in_automation=1
      shift
      ;;
    --delete-on-conflict)
      delete_on_conflict=1
      shift
      ;;
    --tag-msg=*)
      tag_msg="${i#*=}"
      shift
      ;;
    --bach-api-branch=*)
      bach_api_branch="${i#*=}"
      shift
      ;;
    --bach-api-release=*)
      bach_api_release="${i#*=}"
      shift
      ;;
    --bach-ui-branch=*)
      bach_ui_branch="${i#*=}"
      shift
      ;;
    --bach-ui-release=*)
      bach_ui_release="${i#*=}"
      shift
      ;;
    --lambda-package-branch=*)
      lambda_package_branch="${i#*=}"
      shift
      ;;
    --lambda-package-release=*)
      lambda_package_release="${i#*=}"
      shift
      ;;
    --pcm-branch=*)
      pcm_branch="${i#*=}"
      shift
      ;;
    --pcm-release=*)
      pcm_release="${i#*=}"
      shift
      ;;
    --pcm-commons-branch=*)
      pcm_commons_branch="${i#*=}"
      shift
      ;;
    --pcm-commons-release=*)
      pcm_commons_release="${i#*=}"
      shift
      ;;
    --product-delivery-branch=*)
      product_delivery_branch="${i#*=}"
      shift
      ;;
    --product-delivery-release=*)
      product_delivery_release="${i#*=}"
      shift
      ;;
    --default-branch=*)
      # SKIP
      shift
      ;;
    --default-release=*)
      # SKIP
      shift
      ;;
    --git-oauth-token=*)
      # SKIP
      shift
      ;;
    --pub-git-oauth-token=*)
      # SKIP
      shift
      ;;
    -h|--help)
      # SKIP
      shift
      ;;
    *)
      # unknown option
      echoerr "Unsupported argument $i. Exiting."
      usage
      exit 1
      ;;
  esac
done

if [[ -z $tag_msg ]]; then
  tag_msg="Tagging to support the OPERA PCM ${pcm_release}"
fi

# print all variables that have been set
(set -o posix ; set)


######################################################################
# Main script body
######################################################################

echo "This script will tag the repositories for pcm_commons, CNM_product_delivery, bach_api, bach_ui, pcm_lambdas, and opera_pcm"
if [[ -z $in_automation ]]; then
  echo; read -p "Press enter to continue"; echo
fi

if [[ -z $in_automation ]]; then
  cmd_test=`which python3`
  if [ $? -ne 0 ]; then
    echo "Python 3 must be installed. Exiting."
    exit 1
  fi

  cmd_test=`which git`
  if [ $? -ne 0 ]; then
    echo "Git must be installed. Exiting."
    exit 1
  fi

  cmd_test=`which gh`
  if [ $? -ne 0 ]; then
    echo "GitHub CLI must be installed. Exiting."
    exit 1
  fi

  # DEBUG: printing environment information.
  echo $SHELL
  python3 --version
  git --version
  gh --version

  # ensure git/github CLI session is valid
  echo $PUB_GIT_OAUTH_TOKEN | gh auth login --hostname GitHub.com --git-protocol https --with-token
  echo $GIT_OAUTH_TOKEN | gh auth login --hostname github.jpl.nasa.gov --git-protocol https --with-token

fi

mkdir -p OPERA/releases/$pcm_release
pushd OPERA/releases/$pcm_release

echo -e "\n\n========================================================="
echo "Creating ${pcm_commons_release} tag for pcm_commons repo."
echo "========================================================="
rm -rf pcm_commons/
git clone -b ${pcm_commons_branch} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/pcm_commons.git
cd pcm_commons/

if [[ -n $delete_on_conflict ]]; then
  git branch --delete --force ${pcm_commons_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
fi

# delete any existing matching tag
if [[ -n $delete_on_conflict ]]; then
  git tag --delete ${pcm_commons_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/heads/${pcm_commons_release}
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/tags/${pcm_commons_release}
  if [[ -n $slow ]]; then sleep 1; fi
fi

# create branch, then tag branch
git checkout -b ${pcm_commons_release}
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin refs/heads/${pcm_commons_release}:refs/heads/${pcm_commons_release}
if [[ -n $slow ]]; then sleep 1; fi
git tag -a ${pcm_commons_release} -m "${tag_msg}"
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin tag ${pcm_commons_release}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${product_delivery_release} tag for CNM_product_delivery repo."
echo "========================================================="
rm -rf CNM_product_delivery/
git clone -b ${product_delivery_branch} https://${GIT_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/CNM_product_delivery.git
cd CNM_product_delivery/

if [[ -n $delete_on_conflict ]]; then
  git branch --delete --force ${product_delivery_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
fi

# delete any existing matching tag
if [[ -n $delete_on_conflict ]]; then
  git tag --delete ${product_delivery_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/heads/${product_delivery_release}
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/tags/${product_delivery_release}
  if [[ -n $slow ]]; then sleep 1; fi
fi

# create branch, then tag branch
git checkout -b ${product_delivery_release}
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin refs/heads/${product_delivery_release}:refs/heads/${product_delivery_release}
if [[ -n $slow ]]; then sleep 1; fi
git tag -a ${product_delivery_release} -m "${tag_msg}"
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin tag ${product_delivery_release}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${bach_api_release} tag for opera-bach-api repo."
echo "========================================================="
rm -rf opera-sds-bach-api/
git clone -b ${bach_api_branch} https://${PUB_GIT_OAUTH_TOKEN}@github.com/nasa/opera-sds-bach-api.git
cd opera-sds-bach-api/

if [[ -n $delete_on_conflict ]]; then
  git branch --delete --force ${bach_api_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
fi

# delete any existing matching tag
if [[ -n $delete_on_conflict ]]; then
  git tag --delete ${bach_api_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/heads/${bach_api_release}
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/tags/${bach_api_release}
  if [[ -n $slow ]]; then sleep 1; fi
fi

# create branch, then tag branch
git checkout -b ${bach_api_release}
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin refs/heads/${bach_api_release}:refs/heads/${bach_api_release}
if [[ -n $slow ]]; then sleep 1; fi
git tag -a ${bach_api_release} -m "${tag_msg}"
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin tag ${bach_api_release}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${bach_ui_release} tag for opera-bach-ui repo."
echo "========================================================="
rm -rf opera-sds-bach-ui/
git clone -b ${bach_ui_branch} https://${PUB_GIT_OAUTH_TOKEN}@github.com/nasa/opera-sds-bach-ui.git
cd opera-sds-bach-ui/

if [[ -n $delete_on_conflict ]]; then
  git branch --delete --force ${bach_ui_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
fi

# delete any existing matching tag
if [[ -n $delete_on_conflict ]]; then
  git tag --delete ${bach_ui_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/heads/${bach_ui_release}
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/tags/${bach_ui_release}
  if [[ -n $slow ]]; then sleep 1; fi
fi

# create branch, then tag branch
git checkout -b ${bach_ui_release}
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin refs/heads/${bach_ui_release}:refs/heads/${bach_ui_release}
if [[ -n $slow ]]; then sleep 1; fi
git tag -a ${bach_ui_release} -m "${tag_msg}"
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin tag ${bach_ui_release}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${lambda_package_release} tag for pcm-lambdas repo."
echo "========================================================="
rm -rf opera-sds-lambdas/
git clone -b ${lambda_package_branch} https://${PUB_GIT_OAUTH_TOKEN}@github.com/nasa/opera-sds-lambdas.git
cd opera-sds-lambdas/

if [[ -n $delete_on_conflict ]]; then
  git branch --delete --force ${lambda_package_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
fi

# delete any existing matching tag
if [[ -n $delete_on_conflict ]]; then
  git tag --delete ${lambda_package_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/heads/${lambda_package_release}
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/tags/${lambda_package_release}
  if [[ -n $slow ]]; then sleep 1; fi
fi

# create branch, then tag branch
git checkout -b ${lambda_package_release}
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin refs/heads/${lambda_package_release}:refs/heads/${lambda_package_release}
if [[ -n $slow ]]; then sleep 1; fi
git tag -a ${lambda_package_release} -m "${tag_msg}"
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin tag ${lambda_package_release}
cd ../

echo -e "\n\n========================================================="
echo "Creating ${pcm_release} tag for opera-pcm repo."
echo "========================================================="
rm -rf opera-sds-pcm/
git clone -b ${pcm_branch} https://${PUB_GIT_OAUTH_TOKEN}@github.com/nasa/opera-sds-pcm.git
cd opera-sds-pcm/

if [[ -n $delete_on_conflict ]]; then
  git branch --delete --force ${pcm_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
fi

# delete any existing matching tag
if [[ -n $delete_on_conflict ]]; then
  git tag --delete ${pcm_release} || :
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/heads/${pcm_release}
  if [[ -n $slow ]]; then sleep 1; fi
  git push --dry-run origin :refs/tags/${pcm_release}
  if [[ -n $slow ]]; then sleep 1; fi
fi

# create branch, then tag branch
git checkout -b release/${pcm_release}
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin refs/heads/release/${pcm_release}:refs/heads/release/${pcm_release}
if [[ -n $slow ]]; then sleep 1; fi
git tag -a ${pcm_release} -m "${tag_msg}"
if [[ -n $slow ]]; then sleep 1; fi
git push --dry-run origin tag ${pcm_release}

cd ../

popd
