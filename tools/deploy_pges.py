#!/usr/bin/env python

"""

Tool to dynamically deploy PGEs to the cluster

author mcayanan

"""

import multiprocessing as mp
import os
import argparse
import sys
import boto3

from commons.logger import logger
from commons.logger import LogLevels
from util.conf_util import YamlConf

from pcm_commons.deploy.transfer_pge_utils import download_images
from pcm_commons.deploy.transfer_pge_utils import upload_to_s3
from pcm_commons.deploy.transfer_pge_utils import upload

from pcm_commons.deploy.docker_utils import push_to_docker_registry
from pcm_commons.deploy.docker_utils import retag_image
from pcm_commons.deploy.docker_utils import load_image
from pcm_commons.deploy.docker_utils import pull_image

from pcm_commons.deploy.cluster_utils import register_container
from pcm_commons.deploy.cluster_utils import retag_trigger_rules
from pcm_commons.deploy.cluster_utils import retag_lambdas
from pcm_commons.deploy.cluster_utils import register_jobs

BASE_URL = "https://cae-artifactory.jpl.nasa.gov/artifactory/general/gov/nasa/jpl/nisar/sds/pge/build"

FAILED = "failed"
PCM_CONTAINER_NAME = "container-iems-sds_opera-pcm"
JOB_SPEC_DIR = "/export/home/hysdsops/mozart/ops/opera-pcm/docker"


def get_parser():
    """
    Get a parser for this application
    @return: parser to for this application
    """
    parser = argparse.ArgumentParser(description="Tool to dynamically deploy PGEs to the cluster.")
    parser.add_argument("--pge_release", type=str, required=True,
                        help="Specify the PGE release version to get from Artifactory."),
    parser.add_argument("--artifactory_url", required=False,
                        help="Specify the Artifactory URL to the PGEs. Default is {}/<PGE_Release_Tag>".format(
                            BASE_URL))
    parser.add_argument("--image_names", type=str, required=True, help="Specify one or more PGE image names "
                                                                       "to get from Artifactory.")
    parser.add_argument("--username", type=str, required=False, help="Optionally specify a JPL Artifactory username.")
    parser.add_argument("--api_key", type=str, required=False, help="Optionally a JPL Artifactory API Key.")
    parser.add_argument("--sds_config", type=str, required=True, help="Specify the path to the SDS config file.")
    parser.add_argument("--processes", required=False, default=2,
                        help="Specify number of processes to run in parallel. Defaults to 2.")
    parser.add_argument("--force", required=False, action="store_true", default=False,
                        help="Specify this flag to force the tool to overwrite the existing file in s3.")
    parser.add_argument("--verbose_level",
                        type=lambda verbose_level: LogLevels[verbose_level].value,
                        choices=LogLevels.list(),
                        help="Specify a verbosity level. Default is {}".format(LogLevels.INFO))
    parser.add_argument("--output_dir", required=False, help="Specify the output directory. "
                                                             "Default is the current working directory.")
    parser.add_argument("--skip_download", action="store_true", default=False,
                        help="Specify to skip the download step. Tool will find the images from the given output_dir "
                             "directory.")
    parser.add_argument("--skip_upload", action="store_true", default=False,
                        help="Specfiy to skip the upload to S3 step.")
    parser.add_argument("--skip_registry", action="store_true", default=False,
                        help="Specify to skip pushing the artifacts to a Docker Registry.")
    parser.add_argument("--skip_register_new_pcm", action="store_true", default=False,
                        help="Specify to skip registering a new PCM container. Assumes it's been previously "
                             "registerd.")
    parser.add_argument("--download_from_s3", action="store_true", default=False,
                        help="Specify to download the PGEs from S3. Artifacts are assumed to be found in the given "
                             "code bucket.")
    parser.add_argument("--update_cluster", action="store_true", default=False,
                        help="Tells the tool to update the cluster accordingly with these new PGEs.")
    parser.add_argument("--old_pcm_release_tag", help="Specify the current PCM tag being used by the cluster.")
    parser.add_argument("--new_pcm_release_tag", help="Specify the new PCM tag that will be associated with this PGE "
                                                      "update.")
    parser.add_argument("--job_spec_dir", default=JOB_SPEC_DIR, required=False,
                        help="Specify the directory path to the PCM job specs. Default is {}".format(JOB_SPEC_DIR))

    return parser


def get_docker_image_name(image_name):
    # Docker file tar.gz name: i.e. nisar_pge-l0a-R1.0.1.tar.gz
    # Docker Image name: i.e. nisar_pge/l0a
    return image_name.replace("-", "/", 1)


def main():
    """
    Main entry point
    """
    args = get_parser().parse_args()
    pge_release = args.pge_release
    output_dir = os.getcwd()
    image_names = [item.strip() for item in args.image_names.split(',')]
    pool_handler = mp.Pool(int(args.processes))

    sds_config = YamlConf(args.sds_config)
    code_bucket = sds_config.get("CODE_BUCKET")
    s3_endpoint = sds_config.get("S3_ENDPOINT")
    registry_url = sds_config.get("CONTAINER_REGISTRY")
    venue = sds_config.get("VENUE")

    username = None
    if args.username:
        username = args.username

    api_key = None
    if args.api_key:
        api_key = args.api_key

    if args.verbose_level:
        LogLevels.set_level(args.verbose_level)

    if args.output_dir:
        output_dir = args.output_dir

    force = False
    if args.force:
        force = args.force

    artifactory_url = os.path.join(BASE_URL, pge_release)
    if args.artifactory_url:
        artifactory_url = args.artifactory_url

    old_pcm_tag = None
    new_pcm_tag = None
    job_spec_dir = None

    if args.update_cluster is True:
        if not args.old_pcm_release_tag or not args.new_pcm_release_tag:
            raise Exception("Need to specify --old_tag and --new_tag when updating cluster.")
        old_pcm_tag = args.old_pcm_release_tag
        new_pcm_tag = args.new_pcm_release_tag

        if args.job_spec_dir:
            job_spec_dir = os.path.abspath(args.job_spec_dir)
        else:
            raise Exception("Must specify --job_spec_dir when updating cluster.")

    if args.skip_download:
        pge_files = dict()
        for image_name in image_names:
            pge_file = os.path.join(output_dir, "{}-{}.tar.gz".format(image_name, pge_release))
            if os.path.exists(pge_file):
                pge_files.update({"{}:{}".format(image_name, pge_release): pge_file})
            else:
                logger.error("Could not find docker file: {}".format(pge_file))
    else:
        s3_bucket = None
        if args.download_from_s3:
            s3_bucket = code_bucket

        pge_files = download_images(artifactory_url, image_names, pge_release, username, api_key, pool_handler,
                                    output_dir=output_dir, s3_bucket=s3_bucket)

    failed = list()
    successful_uploads = dict()
    if args.skip_upload:
        successful_uploads = pge_files
    else:
        results = upload_to_s3(pge_files, code_bucket, pool_handler, force)
        for result in results:
            if FAILED in result:
                failed.append(os.path.basename(result[FAILED]))
            else:
                successful_uploads.update(result)

    temp_dict = dict()
    for image_name in successful_uploads.keys():
        docker_image_name = get_docker_image_name(image_name)
        temp_dict[docker_image_name] = successful_uploads[image_name]

    successful_uploads = temp_dict

    if not args.skip_registry:
        # Limit the parallel processes to 2 here as we have exprienced timeout issues
        # when pushing to the registry. Will need to investigate how to optimize this.
        docker_pool_handler = mp.Pool(2)
        results = push_to_docker_registry(successful_uploads, docker_pool_handler, registry_url)
        for result in results:
            if FAILED in result:
                failed.append(os.path.basename(result[FAILED]))

    if len(failed) != 0:
        logger.error("The following PGEs failed to get deployed: {}".format(failed))
        sys.exit(1)

    if args.update_cluster is True:
        mozart_base_url = "https://{}".format(sds_config.get("MOZART_PVT_IP"))
        mozart_rest_url = "{}/mozart/api/v0.1".format(mozart_base_url)

        client = boto3.client("s3")
        old_pcm_container_file_name = "{}:{}.tar.gz".format(PCM_CONTAINER_NAME, old_pcm_tag.lower())
        local_old_pcm_container_file = os.path.join(output_dir, old_pcm_container_file_name)
        retagged_container_name = "{}:{}".format(PCM_CONTAINER_NAME, new_pcm_tag.lower())
        storage = "s3://{}/{}".format(s3_endpoint, code_bucket)
        if args.skip_register_new_pcm is False:
            # Pull the old PCM container
            old_pcm_container_image = "{}:{}".format(PCM_CONTAINER_NAME, old_pcm_tag.lower())
            try:
                pull_image(old_pcm_container_image, registry_url)
            except Exception as e:
                logger.warning("Could not pull {}: {}\nManually loading it in.".format(old_pcm_container_image,
                                                                                       str(e)))
                logger.info("Downloading {} from {}".format(old_pcm_container_file_name, code_bucket))
                client.download_file(Bucket=code_bucket, Key=old_pcm_container_file_name,
                                     Filename=local_old_pcm_container_file)
                load_image(local_old_pcm_container_file, registry_url)
            # Re-tag the old PCM container with a new tag
            retagged_pcm_container_file = retag_image("{}:{}".format(PCM_CONTAINER_NAME, old_pcm_tag.lower()),
                                                      new_pcm_tag.lower(), output_dir, registry_url)
            # register new container
            upload(retagged_container_name, retagged_pcm_container_file, code_bucket, force_upload=True)
            container_url = "{}/{}".format(storage, os.path.basename(retagged_pcm_container_file))
            register_container(retagged_container_name, new_pcm_tag, container_url, mozart_rest_url)

        hysds_io_ids, job_spec_ids = register_jobs(successful_uploads, job_spec_dir, retagged_container_name,
                                                   new_pcm_tag, storage, mozart_base_url)
        retag_trigger_rules(mozart_base_url, old_pcm_tag, hysds_io_ids, job_spec_ids)
        retag_lambdas(venue, old_pcm_tag, new_pcm_tag, job_spec_ids)


if __name__ == "__main__":
    main()
