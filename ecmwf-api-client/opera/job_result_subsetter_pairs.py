import logging
from pathlib import Path
from typing import Optional

import backoff
import boto3
from dateutil.parser import isoparse
from more_itertools import first
from mypy_boto3_s3 import S3ServiceResource
from rioxarray.exceptions import NoDataInBounds

from opera import subset, merge

logger = logging.getLogger(__name__)


class JobResultSubsetterPairs():
    bucket_name = "opera-ecmwf"
    subset_bucket_name = "opera-ancillaries"
    enabled_upload_to_s3 = True
    delete_local_file_after_s3_upload = True
    """Toggle for deleting local job files.
    
    NetCDF files can be about 13 GB each. grib2 files are around 5 GB. It is recommended to set this to True in production.
    
    False is supported for development purposes so devs can inspect the job results.
    """

    is_smoke_test = False
    is_dev_test = False

    @backoff.on_exception(backoff.constant, Exception, max_tries=2)
    def to_netcdf_compressed(self, nc, target: Path):
        comp = dict(zlib=True, complevel=9)
        encoding = {var: comp for var in nc.data_vars}
        nc.to_netcdf(path=target.expanduser().resolve(), encoding=encoding)

    def do_merge(self, a2_a3_nc_filepath_pairs: list[tuple[Path, Path]],
                 target: Optional[Path] = None):
        """Perform the merge operation step of result transfer on the given A2+A3 file pairs"""
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

        :param target: the target merge filepath. See `do_merge_netcdf` for details.
        :param engine: the engine to use when reading input files. Not required if files use conventional file extensions.
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

        :param target: the target merge filepath. Defaults to saving 2 parent directories up, which assumes the files are under `YYYYMMDD` and `A2`/`A3` subdirectories.
        :param engine: the engine to use when reading input files. Not required if files use conventional file extensions.
        """
        a2_a3_nc_filepath_pair = first(a2_a3_nc_filepath_pairs)
        a2_nc_filepath, _ = a2_a3_nc_filepath_pair

        a2_a3_pair_resolved_paths = [
            (
                pair[0].expanduser().resolve(),
                pair[1].expanduser().resolve()
            )
            for pair in a2_a3_nc_filepath_pairs
        ]
        merged_filepath = target.expanduser().resolve() if target else a2_nc_filepath.expanduser().resolve().parent.parent / (a2_nc_filepath.stem.removeprefix("A2") + ".merged.nc")
        merged_filepath = self.try_nisar_open_and_concat_and_save_netcdf_pairs(a2_a3_pair_resolved_paths, merged_filepath)

        logger.info("Done merging")
        return merged_filepath

    @backoff.on_exception(backoff.constant, Exception, max_tries=2)
    def try_nisar_open_and_concat_and_save_netcdf_pairs(self, a2_a3_pair_resolved_paths, target):
        return merge.nisar_open_and_concat_and_save_netcdf_pairs(a2_a3_pair_resolved_paths, target=target)

    def do_upload_subset(self, req_dt_str: str, subset_filepath, raise_):
        logger.info("UPLOADING SUBSET TO S3")
        logger.info("This may take a few minutes...")

        try:
            if not self.enabled_upload_to_s3:
                logger.warning("SKIPPING SUBSET S3 UPLOAD")
            if self.enabled_upload_to_s3:
                s3: S3ServiceResource = boto3.resource("s3")
                bucket = s3.Bucket(self.subset_bucket_name)
                yyyymmdd = str(isoparse(req_dt_str).strftime("%Y%m%d"))
                key = f'ecmwf/{yyyymmdd}/{subset_filepath.name}'
                bucket.upload_file(Filename=str(subset_filepath.expanduser().resolve()), Key=key)
                uploaded_s3path = f"s3://{bucket.name}/{key}"
                logger.info(f"{uploaded_s3path=}")
        except Exception as e:
            logger.exception("An error occurred while uploading to S3", exc_info=e)
            if raise_:
                raise e

            logger.info(f"Deleting {subset_filepath}")
            if not self.is_dev_test:
                subset_filepath.unlink(missing_ok=True)
        finally:
        # TODO chrisjrd: fix file handling
        #     logger.info(f"Deleting {subset_filepath}")
        #     if not self.is_dev_test:
        #         subset_filepath.unlink(missing_ok=True)
            pass

        logger.info("UPLOADED SUBSET TO S3")

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
        subset_filepath = target or src.expanduser().resolve().parent / (src.name.removesuffix("".join(src.suffixes)) + ".subset.nc")
        subset.subset_netcdf_file(
            in_=src.expanduser().resolve(),
            out_=subset_filepath.expanduser().resolve(),
            bbox=(-172, 18, -67, 72)  # US (-172, 18, -67, 72)
        )
        logger.info("Done subsetting")
        return subset_filepath