import logging.handlers
from collections import defaultdict
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
from mypy_boto3_s3 import S3ServiceResource

from opera.grib_to_netcdf_runner import run_grib_to_netcdf
from opera.job_result_subsetter_pairs import JobResultSubsetterPairs

import xarray

from temp import with_inserted_suffix

logger = logging.getLogger(__name__)


def main(*, bucket_name, target_bucket_name, s3_keys):

    s3: S3ServiceResource = boto3.resource("s3")

    paired_paths = defaultdict(partial(defaultdict, list))
    for s3_key in s3_keys:
        # raw/<YYYYMMDD>/<A2|A3>/<filename>
        segments = s3_key.split("/")
        paired_paths[segments[1]][segments[2]].append(s3_key)

    for date in paired_paths:
        if not paired_paths[date]["A2"] and not paired_paths[date]["A3"]:
            logger.warning(f"The A2+A3 dirs for {date} were not found. Check S3 bucket {bucket_name}.")
            continue

        a2_files = sorted(paired_paths[date]["A2"])
        a3_files = sorted(paired_paths[date]["A3"])

        a2_a3_s3_path_pairs = zip(a2_files, a3_files)

        for s3_a2, s3_a3 in a2_a3_s3_path_pairs:
            with TemporaryDirectory(dir=Path(".").resolve()) as tmpdirname:
                logger.info(f"Downloading from {s3_a2=} to {tmpdirname=}")
                a2_object = s3.Object(bucket_name, key=s3_a2)
                a2_grib_filepath = Path(tmpdirname, a2_object.key.rsplit("/", maxsplit=1)[1])
                a2_object.download_file(str(a2_grib_filepath))
                logger.info(f"Downloaded to {a2_grib_filepath=!s}")

                logger.info(f"Downloading from {s3_a3=} to {tmpdirname=}")
                a3_object = s3.Object(bucket_name, key=s3_a3)
                a3_grib_filepath = Path(tmpdirname, a3_object.key.rsplit("/", maxsplit=1)[1])
                a3_object.download_file(str(a3_grib_filepath))
                logger.info(f"Downloaded to {a3_grib_filepath=!s}")

                a2_nc_filepath = a2_grib_filepath.with_suffix(".nc")
                a3_nc_filepath = a3_grib_filepath.with_suffix(".nc")

                logger.info(f"Converting to netCDF4")
                a2_nc_filepath = run_grib_to_netcdf(grib_file=a2_grib_filepath, nc_file=a2_nc_filepath)
                a3_nc_filepath = run_grib_to_netcdf(grib_file=a3_grib_filepath, nc_file=a3_nc_filepath)
                logger.info(f"Converted to netCDF4")

                # result_transferer = JobResultTransfererPairs(ecmwf_service=None, dao=None)
                result_transferer = JobResultSubsetterPairs()
                a2_a3_nc_filepath_pair = (a2_nc_filepath, a3_nc_filepath)
                logger.info(f"Merge input: {a2_a3_nc_filepath_pair=}")


                #D06130000061300001.nc
                merged_filepath_tmp = a2_nc_filepath.resolve().with_name(a2_nc_filepath.name.removeprefix("A2D").removeprefix("A3D"))

                merged_filepath = merged_filepath_tmp.parent
                merged_filename = merged_filepath_tmp.name
                analysis_hour=str(merged_filename)[4:8]

                tmp_new_filename = f"tmp_ECMWF_A2A3_{date}{analysis_hour}_{date}{analysis_hour}_1.nc"
                new_filename = f"ECMWF_A2A3_{date}{analysis_hour}_{date}{analysis_hour}_1.nc"

                tmp_merged_filepath = merged_filepath.parent/tmp_new_filename
                tmp_merged_filepath = result_transferer.do_merge([a2_a3_nc_filepath_pair], target=tmp_merged_filepath)
                logger.info(f"Merged input: {a2_a3_nc_filepath_pair=}")
                logger.info(f"Merged output: {tmp_merged_filepath=}")

                logger.info("Compressing merged A2/A3 NetCDF file: %", tmp_merged_filepath)
                #compressed_filepath = with_inserted_suffix(merged_filepath, ".zz")
                compressed_filepath = merged_filepath.parent/new_filename


                result_transferer.compress_netcdf(tmp_merged_filepath, compressed_filepath)
                
                #nc = xarray.open_dataset(str(merged_filepath.resolve()), chunks="auto")
                #result_transferer.to_netcdf_compressed(nc, compressed_filepath, complevel=4)
                #nc.close()
                logger.info("Compressed")

                logging.info(f"Uploading results for {date=}")
                result_transferer.ancillaries_bucket_name = target_bucket_name
                result_transferer.do_upload(date, compressed_filepath, raise_=True)
                logging.info(f"Uploaded results for {date=}")
