import logging
import json
import netrc
import os
from pathlib import PurePath, Path
import requests
import requests.utils
from datetime import datetime, timedelta
from data_subscriber import ionosphere_download
from data_subscriber.url import _has_url, _to_url, _to_https_url, _slc_url_to_chunk_id, form_batch_id

from tools import stage_orbit_file
from tools.stage_ionosphere_file import IonosphereFileNotFoundException
from tools.stage_orbit_file import (parse_orbit_time_range_from_safe,
                                    NoQueryResultsException,
                                    NoSuitableOrbitFileException,
                                    T_ORBIT,
                                    ORBIT_PAD)

from data_subscriber.download import DaacDownload

logger = logging.getLogger(__name__)

class DaacDownloadAsf(DaacDownload):

    def perform_download(self,
            session: requests.Session,
            es_conn,
            downloads: list[dict],
            args,
            token,
            job_id
    ):

        for download in downloads:
            if not _has_url(download):
                continue

            if args.transfer_protocol == "https":
                product_url = _to_https_url(download)
            else:
                product_url = _to_url(download)

            logger.info(f"Processing {product_url=}")
            product_id = _slc_url_to_chunk_id(product_url, str(download['revision_id']))

            product_download_dir = self.downloads_dir / product_id
            product_download_dir.mkdir(exist_ok=True)

            # download product
            if args.dry_run:
                logger.debug(f"{args.dry_run=}. Skipping download.")
                continue

            if product_url.startswith("s3"):
                product = product_filepath = self.download_product_using_s3(
                    product_url,
                    token,
                    target_dirpath=product_download_dir.resolve(),
                    args=args
                )
            else:
                product = product_filepath = self.download_asf_product(
                    product_url, token, product_download_dir
                )

            logger.info(f"{product_filepath=}")

            logger.info(f"Marking as downloaded. {download['id']=}")
            es_conn.mark_product_as_downloaded(download['id'], job_id)

            logger.info(f"product_url_downloaded={product_url}")

            additional_metadata = {}
            try:
                additional_metadata['processing_mode'] = download['processing_mode']
            except:
                logger.warning("processing_mode not found in the slc_catalog ES index")

            if download.get("intersects_north_america"):
                logger.info("adding additional dataset metadata (intersects_north_america)")
                additional_metadata["intersects_north_america"] = True

            dataset_dir = self.extract_one_to_one(product, self.cfg, working_dir=Path.cwd(),
                                             extra_metadata=additional_metadata,
                                             name_postscript='-r'+str(download['revision_id']))

            self.update_pending_dataset_with_index_name(dataset_dir, '-r'+str(download['revision_id']))

            # Rename the dataset_dir to match the pattern w revision_id
            new_dataset_dir = dataset_dir.parent / form_batch_id(dataset_dir.name, str(download['revision_id']))
            logger.info(f"{new_dataset_dir}")
            os.rename(str(dataset_dir), str(new_dataset_dir))

            self.download_orbit_file(new_dataset_dir, product_filepath, additional_metadata)

            if (additional_metadata['processing_mode'] in ("historical", "reprocessing")):
                logger.info(
                    f"Processing mode is {additional_metadata['processing_mode']}. "
                    f"Attempting to download ionosphere correction file."
                )
                self.download_ionosphere_file(new_dataset_dir, product_filepath)

            logger.info(f"Removing {product_filepath}")
            product_filepath.unlink(missing_ok=True)

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

    def download_asf_product(self, product_url, token: str, target_dirpath: Path):
        logger.info(f"Requesting from {product_url}")

        asf_response = self._handle_url_redirect(product_url, token)
        asf_response.raise_for_status()

        product_filename = PurePath(product_url).name
        product_download_path = target_dirpath / product_filename
        with open(product_download_path, "wb") as file:
            file.write(asf_response.content)
        return product_download_path.resolve()

    def update_pending_dataset_metadata_with_ionosphere_metadata(self, dataset_dir: PurePath, ionosphere_metadata: dict):
        logger.info("Updating dataset's met.json with ionosphere metadata")

        with Path(dataset_dir / f"{dataset_dir.name}.met.json").open("r") as fp:
            met_json: dict = json.load(fp)

        met_json.update(ionosphere_metadata)

        with Path(dataset_dir / f"{dataset_dir.name}.met.json").open("w") as fp:
            json.dump(met_json, fp)

    def update_pending_dataset_with_index_name(self, dataset_dir: PurePath, postscript):
        logger.info("Updating dataset's dataset.json with index name")

        with Path(dataset_dir / f"{dataset_dir.name}{postscript}.dataset.json").open("r") as fp:
            dataset_json: dict = json.load(fp)

        with Path(dataset_dir / f"{dataset_dir.name}{postscript}.met.json").open("r") as fp:
            met_dict: dict = json.load(fp)

        dataset_json.update({
            "index": {
                "suffix": ("{version}_{dataset}-{date}".format(
                    version=dataset_json["version"],
                    dataset=met_dict["ProductType"],
                    date=datetime.utcnow().strftime("%Y.%m")
                )).lower()  # suffix index name with `-YYYY.MM
            }
        })

        with Path(dataset_dir / f"{dataset_dir.name}.dataset.json").open("w") as fp:
            json.dump(dataset_json, fp)

    def _get_aws_creds(self, token):
        logger.info("entry")

        with requests.get("https://sentinel1.asf.alaska.edu/s3credentials",
                          headers={'Authorization': f'Bearer {token}'}) as r:
            r.raise_for_status()

            return r.json()
