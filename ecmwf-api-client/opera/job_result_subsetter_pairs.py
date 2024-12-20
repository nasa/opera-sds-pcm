import logging
from pathlib import Path
from typing import Optional, Union, Literal
import subprocess

import backoff
import boto3
import xarray
from dateutil.parser import isoparse
from more_itertools import first
from mypy_boto3_s3 import S3ServiceResource
from rioxarray.exceptions import NoDataInBounds

from opera import subset, merge
from temp import with_inserted_suffix

logger = logging.getLogger(__name__)

COMPLEVEL = Literal[-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


class JobResultSubsetterPairs():
    bucket_name = "opera-ecmwf"
    ancillaries_bucket_name = "opera-ancillaries"
    enabled_upload_to_s3 = True
    delete_local_file_after_s3_upload = True
    """Toggle for deleting local job files.
    
    NetCDF files can be about 13 GB each. grib2 files are around 5 GB. It is recommended to set this to True in production.
    
    False is supported for development purposes so devs can inspect the job results.
    """

    is_smoke_test = False
    is_dev_test = False

    @backoff.on_exception(backoff.constant, Exception, max_tries=2)
    def to_netcdf_compressed(self, nc: Union[Path, xarray.Dataset], target: Optional[Path],
                             complevel: COMPLEVEL = 9):
        """
        Compress the given xarray Dataset, writing out to a file. A path can be provided instead, which will read the Dataset file into memory.

        :param nc: the Dataset to compress.
        :param target: the output filepath. Required if `nc` is a Dataset. Otherwise, defaults to the source Dataset filepath with an added ".zz" suffix.
        :param complevel: compression level. 0-9. 0 yields no compression. 9 Yields highest compression. None == -1 == 6.
        """
        if type(nc) is Path:
            target = target or with_inserted_suffix(nc, ".zz")
            nc = xarray.open_dataset(str(nc.resolve()), chunks="auto")
        else:
            if not target:
                raise Exception("Missing target filepath for compressed netCDF4")

        comp = dict(zlib=True, complevel=complevel)
        encoding = {var: comp for var in nc.data_vars}
        nc.to_netcdf(path=target.resolve(), encoding=encoding)

        return target.resolve()

    def compress_netcdf(nc_file, compressed_nc_file, use_bzip2=False):
        if use_bzip2:
            subprocess.run(
                ["bzip2", nc_file],
                shell=False,
                check=False,
            )
        else:
            subprocess.run(
                ["nccopy", "-d", "5", "-s", "-m", "500000000", str(nc_file), str(compressed_nc_file)],
                shell=False,
                check=False,
            )
        if not os.path.exists(compressed_nc_file):
            raise RuntimeError(f"Failed to run nccopy compression for {nc_file}.")


    def do_merge(self, a2_a3_nc_filepath_pairs: list[tuple[Path, Path]],
                 target: Optional[Path] = None):
        """
        Perform the merge operation step of result transfer on the given A2+A3 file pairs. Merges all files into a single file.
        File data should be contiguous for predictable results.

        :param a2_a3_nc_filepath_pairs: pairs of netCDF4 filepaths.
        :param target: the output filepath. Defaults to the source Dataset filepath with an added ".merged" suffix, saving 2 parent directories up, which assumes the files are under `YYYYMMDD` and `A2`/`A3` subdirectories.
        """
        logger.info("MERGING")
        logger.info("This may take a few minutes...")

        merged_filepath = self.try_do_merge_netcdf(a2_a3_nc_filepath_pairs, target=target)

        logger.info("MERGED")
        return merged_filepath

    def try_do_merge_netcdf(self, a2_a3_nc_filepath_pairs: list[tuple[Path, Path]],
                            target: Optional[Path] = None):
        """
        Merge netCDF4 file pairs.
        Note that an exception may be raised during the merge process.

        :param target: the output filepath. Defaults to the source Dataset filepath with an added ".merged" suffix, saving 2 parent directories up, which assumes the files are under `YYYYMMDD` and `A2`/`A3` subdirectories.
        """
        try:
            merged_filepath = self.do_merge_netcdf(a2_a3_nc_filepath_pairs, target=target)
        except Exception as e:
            if not self.is_dev_test:
                logger.exception("", exc_info=e)
            raise e

        return merged_filepath

    def do_merge_netcdf(self, a2_a3_nc_filepath_pairs: list[tuple[Path, Path]],
                        target: Optional[Path] = None):
        """
        Perform netCDF file pair merge.
        Clients should instead call `try_do_merge_netcdf`.

        :param target: the output filepath. Defaults to the source Dataset filepath with an added ".merged" suffix, saving 2 parent directories up, which assumes the files are under `YYYYMMDD` and `A2`/`A3` subdirectories.
        """
        a2_a3_nc_filepath_pair = first(a2_a3_nc_filepath_pairs)
        a2_nc_filepath, _ = a2_a3_nc_filepath_pair

        merged_filepath = target.resolve() if target else a2_nc_filepath.resolve().parent.parent / (a2_nc_filepath.name.removeprefix("A2"))
        merged_filepath = self.try_merge_netcdf_pairs(a2_a3_nc_filepath_pairs, merged_filepath)

        logger.info("Done merging")
        return merged_filepath

    @backoff.on_exception(backoff.constant, Exception, max_tries=2)
    def try_merge_netcdf_pairs(self, a2_a3_pair_paths, target: Path):
        return merge.merge_netcdf_pairs(a2_a3_pair_paths, target=target)

    def do_upload_subset(self, req_dt_str: str, subset_filepath, raise_=False, key_prefix="ecmwf/"):
        return self.do_upload(req_dt_str=req_dt_str, filepath=subset_filepath, raise_=raise_, key_prefix=key_prefix)

    def do_upload(self, req_dt_str: str, filepath, raise_=False, key_prefix=""):
        """
        Upload results to S3 to `self.ancillaries_bucket_name`.

        :param req_dt_str: the datetime string associated with the request. Used to place in a YYYYMMDD subfolder.
        :param filepath: the file to upload.
        :param raise_: raise any exception that occurs.
        :param key_prefix: additional S3 key fragment to prepend to the final S3 location.
        """
        logger.info(f"UPLOADING TO {self.ancillaries_bucket_name}")
        logger.info("This may take a few minutes...")

        try:
            if not self.enabled_upload_to_s3:
                logger.warning("SKIPPING S3 UPLOAD")
            if self.enabled_upload_to_s3:
                s3: S3ServiceResource = boto3.resource("s3")
                bucket = s3.Bucket(self.ancillaries_bucket_name)

                yyyymmdd = str(isoparse(req_dt_str).strftime("%Y%m%d"))
                key = f'{key_prefix}{yyyymmdd}/{filepath.name}'

                bucket.upload_file(Filename=str(filepath.resolve()), Key=key)

                uploaded_s3path = f"s3://{bucket.name}/{key}"
                logger.info(f"{uploaded_s3path=}")
                return uploaded_s3path
        except Exception as e:
            logger.exception("An error occurred while uploading to S3", exc_info=e)
            if raise_:
                raise e

            logger.info(f"Deleting {filepath}")
            if not self.is_dev_test:
                filepath.unlink(missing_ok=True)
        finally:
        # TODO chrisjrd: fix file handling
        #     logger.info(f"Deleting {subset_filepath}")
        #     if not self.is_dev_test:
        #         subset_filepath.unlink(missing_ok=True)
            pass

        logger.info(f"UPLOADED TO {self.ancillaries_bucket_name}")

    def do_subset(self, src: Path, target: Optional[Path] = None):
        """
        Performs subsetting step of result transfer.

        :param src: the file to subset. Expected to a merged netCDF4 file.
        :param target: the target filepath.
        """
        logger.info("SUBSETTING FILE")
        logger.info("This may take a few minutes...")

        subset_filepath = self.try_subset(src, target=target)

        logger.info("SUBSET FILE")
        return subset_filepath

    def try_subset(self, src: Path, target: Optional[Path] = None):
        """
        Performs subset operations on the given file.

        :param src: the file to subset. Expected to a merged netCDF4 file.
        :param target: the target filepath.
        """
        try:
            subset_filepath = self.subset_by_bbox(src, target=target)
            # clipped_filepath = self.subset_by_geojson(merged_filepath)
        except NoDataInBounds as e:
            logger.warning("Data wasn't in the geojson area. Nothing to subset.")
            logger.exception("", exc_info=e)
            raise e
        finally:
            # TODO chrisjrd: fix file handling
            # logger.info(f"Deleting {merged_filepath}")
            # if not self.is_dev_test:
            #     merged_filepath.unlink(missing_ok=True)
            pass
        return subset_filepath

    def subset_by_bbox(self, src: Path, target: Optional[Path] = None):
        """
        Subset the given file using a predetermined bbox (North America), outputting to the same directory.

        :param src: the file to subset. Expected to a merged netCDF4 file.
        :param target: the target filepath.
        """
        subset_filepath = target or with_inserted_suffix(src, ".subset")
        subset.subset_netcdf_file(
            in_=src.resolve(),
            out_=subset_filepath.resolve(),
            bbox=(-172, 18, -67, 72)  # US (-172, 18, -67, 72)
        )
        logger.info("Done subsetting")
        return subset_filepath