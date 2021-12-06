# !/usr/bin/env python
import json
import os
import sys
import shutil
import traceback
from datetime import datetime
from util.conf_util import SettingsConf
from util.exec_util import exec_wrapper, call_noerr
from util.ctx_util import JobContext, DockerParams
from opera_chimera.constants.opera_chimera_const import (
    OperaChimeraConstants as nc_const,
)
from commons.logger import logger
from hysds.utils import download_file


def run_net(context, work_dir):

    # get depedency image
    dep_img = context.get('job_specification')['dependency_images'][0]
    dep_img_name = dep_img['container_image_name']
    logger.info("dep_img_name: {}".format(dep_img_name))
    product_paths = context.get("product_paths")
    metadata = context.get("product_metadata")[0]["metadata"]
    file_name = metadata["FileName"]
    localize_url = "{}/{}".format(product_paths[0], file_name)
    logger.info("Localizing {}".format(localize_url))
    loc_t1 = datetime.utcnow()
    job_id_date = work_dir.split("-")[-1].strip().split('.')[0]
    print("job_id_date : {}".format(job_id_date))

    id = "NET_{}".format(os.path.splitext(file_name)[0])
    print("id : {}".format(id))
    prod_dir = id
    os.makedirs(prod_dir, 0o755)

    try:
        download_file(localize_url, work_dir)
    except Exception as e:
        trace = traceback.format_exc()
        error = str(e)
        raise RuntimeError(
            "Failed to download {}: {}\n{}".format(
                localize_url, error, trace)
        )
    loc_t2 = datetime.utcnow()
    loc_dur = (loc_t2 - loc_t1).total_seconds()
    print("Time to download input : {}".format(loc_dur))

    dep_img_name_tokens = dep_img_name.split(":", 1)
    logger.debug("Splitting the PGE Docker Image Name: {}".format(dep_img_name_tokens))

    try:
        # get docker params
        docker_params_file = os.path.join(work_dir, "_docker_params.json")

        dp = DockerParams(docker_params_file)
        docker_params = dp.params
        logger.info("docker_params: {}".format(json.dumps(docker_params, indent=2)))

        docker_img_params = docker_params[dep_img_name]
        uid = docker_img_params["uid"]
        gid = docker_img_params["gid"]
        input_file_name = file_name
        data_dir = work_dir
        output_file_name = "{}.net".format(prod_dir)

        # create directory to house PGE's _docker_stats.json
        pge_stats_dir = os.path.join(work_dir, 'pge_stats')
        os.makedirs(pge_stats_dir, 0o755)

        cmd = [
            'docker run --rm -u {uid}:{gid} -v {work_dir}:/tmp/test'.format(
                uid=uid, gid=gid, work_dir=work_dir
            ),
            '-v {}:/input'.format(data_dir),
            '-w /tmp/test',
            '--entrypoint /opt/docker/bin/run_docker_stats.sh',
            '-i {} sh -ci'.format(dep_img_name),
            '\"noise_evd_estimate.py -i /input/{} -r -o {}\"'.format(input_file_name, output_file_name),
            '--stats /tmp/test/pge_stats/_docker_stats.json'
        ]

        cmd_line = " ".join(cmd)
        logger.info("Calling PGE: {}".format(cmd_line))
        try:
            call_noerr(cmd_line, work_dir)
        except Exception as e:
            logger.error("PGE failure: {}".format(e))
            raise

        met_file = os.path.join(prod_dir, "{}.met.json".format(id))
        with open(output_file_name, 'r') as data_in, open(met_file, "w") as json_out:
            md = {}
            md["input_file_name"] = input_file_name
            md["input_file_metadata"] = metadata
            md["net_value"] = data_in.readline()
            json.dump(md, json_out)

        ds = {
            'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
            'label': id,
            'version': '1.0'
        }
        ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
        with open(ds_file, 'w') as f:
            json.dump(ds, f, indent=2)
        shutil.copy(output_file_name, prod_dir)

    except Exception as e:
        logger.error("PGE failure: {}".format(e))
        raise


@exec_wrapper
def main(args):
    cfg = SettingsConf().cfg
    context_file = "_context.json"
    jc = JobContext(context_file)

    # set force publish (disable no-clobber)
    if cfg.get(nc_const.FORCE_INGEST, {}).get('NET', False):
        jc.set("_force_ingest", True)
        jc.save()

    ctx = jc.ctx
    logger.debug(json.dumps(ctx, indent=2))

    run_net(context=ctx, work_dir=os.getcwd())


if __name__ == "__main__":
    main(sys.argv)
