import json
import logging
import netrc
import os
from datetime import datetime, timedelta
from pathlib import PurePath, Path

from data_subscriber import ionosphere_download
from data_subscriber.asf_download import DaacDownloadAsf
from tools import stage_orbit_file
from tools.stage_ionosphere_file import IonosphereFileNotFoundException
from tools.stage_orbit_file import (parse_orbit_time_range_from_safe,
                                    NoQueryResultsException,
                                    NoSuitableOrbitFileException,
                                    T_ORBIT,
                                    ORBIT_PAD)

logger = logging.getLogger(__name__)


class AsfDaacSlcDownload(DaacDownloadAsf):
    def download_orbit_file(self, dataset_dir, product_filepath, additional_metadata):
        logger.info("Downloading associated orbit file")

        # Get the PCM username/password for authentication to Copernicus Dataspace
        username, _, password = netrc.netrc().authenticators('dataspace.copernicus.eu')

        (_, safe_start_time, safe_stop_time) = parse_orbit_time_range_from_safe(product_filepath)
        safe_start_datetime = datetime.strptime(safe_start_time, "%Y%m%dT%H%M%S")
        safe_stop_datetime = datetime.strptime(safe_stop_time, "%Y%m%dT%H%M%S")

        sensing_start_range = safe_start_datetime - timedelta(seconds=T_ORBIT + ORBIT_PAD)
        sensing_stop_range = safe_stop_datetime + timedelta(seconds=ORBIT_PAD)

        try:
            logger.info(f"Querying for Precise Ephemeris Orbit (POEORB) file")

            stage_orbit_file_args = stage_orbit_file.get_parser().parse_args(
                [
                    f"--output-directory={str(dataset_dir)}",
                    "--orbit-type=POEORB",
                    f"--username={username}",
                    f"--password={password}",
                    f"--sensing-start-range={sensing_start_range.strftime('%Y%m%dT%H%M%S')}",
                    f"--sensing-stop-range={sensing_stop_range.strftime('%Y%m%dT%H%M%S')}",
                    str(product_filepath)
                ]
            )
            stage_orbit_file.main(stage_orbit_file_args)
        except (NoQueryResultsException, NoSuitableOrbitFileException):
            try:
                logger.warning("POEORB file could not be found, querying for Restituted Orbit (ROEORB) file")
                stage_orbit_file_args = stage_orbit_file.get_parser().parse_args(
                    [
                        f"--output-directory={str(dataset_dir)}",
                        "--orbit-type=RESORB",
                        f"--username={username}",
                        f"--password={password}",
                        f"--sensing-start-range={sensing_start_range.strftime('%Y%m%dT%H%M%S')}",
                        f"--sensing-stop-range={sensing_stop_range.strftime('%Y%m%dT%H%M%S')}",
                        str(product_filepath)
                    ]
                )
                stage_orbit_file.main(stage_orbit_file_args)
            except (NoQueryResultsException, NoSuitableOrbitFileException):
                logger.warning("Single RESORB file could not be found, querying for consecutive RESORB files")

                logger.info("Querying for RESORB with range [sensing_start - 1 min, sensing_end + 1 min]")
                sensing_start_range = safe_start_datetime - timedelta(seconds=ORBIT_PAD)
                sensing_stop_range = safe_stop_datetime + timedelta(seconds=ORBIT_PAD)

                stage_orbit_file_args = stage_orbit_file.get_parser().parse_args(
                    [
                        f"--output-directory={str(dataset_dir)}",
                        "--orbit-type=RESORB",
                        f"--username={username}",
                        f"--password={password}",
                        f"--sensing-start-range={sensing_start_range.strftime('%Y%m%dT%H%M%S')}",
                        f"--sensing-stop-range={sensing_stop_range.strftime('%Y%m%dT%H%M%S')}",
                        str(product_filepath)
                    ]
                )
                stage_orbit_file.main(stage_orbit_file_args)

                logger.info("Querying for RESORB with range [sensing_start – T_orb – 1 min, sensing_start – T_orb + 1 min]")
                sensing_start_range = safe_start_datetime - timedelta(seconds=T_ORBIT + ORBIT_PAD)
                sensing_stop_range = safe_start_datetime - timedelta(seconds=T_ORBIT - ORBIT_PAD)

                stage_orbit_file_args = stage_orbit_file.get_parser().parse_args(
                    [
                        f"--output-directory={str(dataset_dir)}",
                        "--orbit-type=RESORB",
                        f"--username={username}",
                        f"--password={password}",
                        f"--sensing-start-range={sensing_start_range.strftime('%Y%m%dT%H%M%S')}",
                        f"--sensing-stop-range={sensing_stop_range.strftime('%Y%m%dT%H%M%S')}",
                        str(product_filepath)
                    ]
                )
                stage_orbit_file.main(stage_orbit_file_args)
        finally:
            # Clear the username and password from memory
            del username
            del password

        logger.info("Added orbit file(s) to dataset")

    def download_ionosphere_file(self, dataset_dir, product_filepath):
        try:
            output_ionosphere_filepath = ionosphere_download.download_ionosphere_correction_file(
                dataset_dir=dataset_dir, product_filepath=product_filepath
            )
            ionosphere_url = ionosphere_download.get_ionosphere_correction_file_url(
                dataset_dir=dataset_dir, product_filepath=product_filepath
            )

            # add ionosphere metadata to the dataset about to be ingested
            ionosphere_metadata = ionosphere_download.generate_ionosphere_metadata(
                output_ionosphere_filepath, ionosphere_url=ionosphere_url,
                s3_bucket="...", s3_key="..."
            )
            self.update_pending_dataset_metadata_with_ionosphere_metadata(dataset_dir, ionosphere_metadata)
        except IonosphereFileNotFoundException:
            logger.warning("Ionosphere file not found remotely. Allowing job to continue.")
            pass

    def update_pending_dataset_metadata_with_ionosphere_metadata(self, dataset_dir: PurePath, ionosphere_metadata: dict):
        logger.info("Updating dataset's met.json with ionosphere metadata")

        with Path(dataset_dir / f"{dataset_dir.name}.met.json").open("r") as fp:
            met_json: dict = json.load(fp)

        met_json.update(ionosphere_metadata)

        with Path(dataset_dir / f"{dataset_dir.name}.met.json").open("w") as fp:
            json.dump(met_json, fp)

