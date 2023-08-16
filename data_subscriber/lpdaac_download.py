import itertools
import logging
import json
import extractor.extract
from pathlib import PurePath, Path
from collections import defaultdict
import shutil
import requests
import requests.utils
from datetime import datetime
from product2dataset import product2dataset
from data_subscriber.url import _to_batch_id, _to_orbit_number, _has_url, _to_url, _to_https_url, form_batch_id

from data_subscriber.download import DaacDownload

logger = logging.getLogger(__name__)

class HLSDownload:
    def __init__(self):
        self.granule_id = None
        self.revision_id = None
        self.es_ids_urls = []

class DaacDownloadLpdaac(DaacDownload):

    def perform_download(self,
            session: requests.Session,
            es_conn,
            downloads: list[dict],
            args,
            token,
            job_id
    ):

        download_map = defaultdict(HLSDownload)
        for download in downloads:
            granule_id = download['granule_id']
            revision_id = str(download['revision_id'])
            key = form_batch_id(granule_id, revision_id)
            download_url = _to_url(download)
            es_id = download['_id']

            download_map[key].granule_id = granule_id
            download_map[key].revision_id = revision_id
            download_map[key].es_ids_urls.append((es_id, download_url))

        if args.smoke_run:
            download_map = dict(itertools.islice(download_map.items(), 1))

        # One HLSDownload object contains multiple es_id and url pairs
        for key, downloads in download_map.items():
            logger.info(f"Processing {key=}")

            granule_download_dir = self.downloads_dir / key
            granule_download_dir.mkdir(exist_ok=True)

            # download products in granule
            products = []
            product_urls_downloaded = []
            for download in downloads.es_ids_urls:
                product_url = download[1]
                if args.dry_run:
                    logger.debug(f"{args.dry_run=}. Skipping download.")
                    break
                product_filepath = self.download_product(product_url, session, token, args, granule_download_dir)
                products.append(product_filepath)
                product_urls_downloaded.append(product_url)

                # Mark as downloaded
                es_id = download[0]
                es_conn.mark_product_as_downloaded(es_id, job_id)
            logger.info(f"{products=}")


            logger.info(f"{len(product_urls_downloaded)=}, {product_urls_downloaded=}")

            self.extract_many_to_one(products, key, self.cfg)

            logger.info(f"Removing directory {granule_download_dir}")
            shutil.rmtree(granule_download_dir)


    def download_product(self, product_url, session, token: str, args, target_dirpath: Path):
        if args.transfer_protocol.lower() == "https":
            product_filepath = self.download_product_using_https(
                product_url,
                session,
                token,
                target_dirpath=target_dirpath.resolve()
            )
        elif args.transfer_protocol.lower() == "s3":
            product_filepath = self.download_product_using_s3(
                product_url,
                token,
                target_dirpath=target_dirpath.resolve(),
                args=args
            )
        elif args.transfer_protocol.lower() == "auto":
            if product_url.startswith("s3"):
                product_filepath = self.download_product_using_s3(
                    product_url,
                    token,
                    target_dirpath=target_dirpath.resolve(),
                    args=args
                )
            else:
                product_filepath =self.download_product_using_https(
                    product_url,
                    session,
                    token,
                    target_dirpath=target_dirpath.resolve()
                )

        return product_filepath

    def extract_many_to_one(self, products: list[Path], group_dataset_id, settings_cfg: dict):
        """Creates a dataset for each of the given products, merging them into 1 final dataset.

        :param products: the products to create datasets for.
        :param group_dataset_id: a unique identifier for the group of products.
        :param settings_cfg: the settings.yaml config as a dict.
        """
        # house all datasets / extracted metadata
        extracts_dir = Path("extracts")
        extracts_dir.mkdir(exist_ok=True)

        # create individual dataset dir for each product in the granule
        # (this also extracts the metadata to *.met.json files)
        product_extracts_dir = extracts_dir / group_dataset_id
        product_extracts_dir.mkdir(exist_ok=True)
        dataset_dirs = [
            self.extract_one_to_one(product, settings_cfg, working_dir=product_extracts_dir)
            for product in products
        ]
        logger.info(f"{dataset_dirs=}")

        # generate merge metadata from single-product datasets
        shared_met_entries_dict = {}  # this is updated, when merging, with metadata common to multiple input files
        total_product_file_sizes, merged_met_dict = \
            product2dataset.merge_dataset_met_json(
                str(product_extracts_dir.resolve()),
                extra_met=shared_met_entries_dict  # copy some common metadata from each product.
            )
        logger.debug(f"{merged_met_dict=}")

        logger.info("Creating target dataset directory")
        target_dataset_dir = Path(group_dataset_id)
        target_dataset_dir.mkdir(exist_ok=True)
        for product in products:
            shutil.copy(product, target_dataset_dir.resolve())
        logger.info("Copied input products to dataset directory")

        # group_dataset_id coming in is the ES _id which contains the revision-id from CMR as
        # the last .# So we split that out
        #TODO: Make this a function in url
        granule_id = group_dataset_id.split('-')[0]

        logger.info("update merged *.met.json with additional, top-level metadata")
        merged_met_dict.update(shared_met_entries_dict)
        merged_met_dict["FileSize"] = total_product_file_sizes
        merged_met_dict["FileName"] = granule_id
        merged_met_dict["id"] = granule_id
        logger.debug(f"{merged_met_dict=}")

        # write out merged *.met.json
        merged_met_json_filepath = target_dataset_dir.resolve() / f"{group_dataset_id}.met.json"
        with open(merged_met_json_filepath, mode="w") as output_file:
            json.dump(merged_met_dict, output_file)
        logger.info(f"Wrote {merged_met_json_filepath=!s}")

        # write out basic *.dataset.json file (version + created_timestamp)
        dataset_json_dict = extractor.extract.create_dataset_json(
            product_metadata={"dataset_version": merged_met_dict["dataset_version"]},
            ds_met={},
            alt_ds_met={}
        )
        dataset_json_dict.update({
            "index": {
                "suffix": ("{version}_{dataset}-{date}".format(
                    version=dataset_json_dict["version"],
                    dataset=merged_met_dict["ProductType"],
                    date=datetime.utcnow().strftime("%Y.%m")
                )).lower()  # suffix index name with `-YYYY.MM
            }
        })
        granule_dataset_json_filepath = target_dataset_dir.resolve() / f"{group_dataset_id}.dataset.json"
        with open(granule_dataset_json_filepath, mode="w") as output_file:
            json.dump(dataset_json_dict, output_file)
        logger.info(f"Wrote {granule_dataset_json_filepath=!s}")

        shutil.rmtree(extracts_dir)

    def download_product_using_https(self, url, session: requests.Session, token, target_dirpath: Path,
                                     chunk_size=25600) -> Path:
        headers = {"Echo-Token": token}
        with session.get(url, headers=headers) as r:
            r.raise_for_status()

            file_name = PurePath(url).name
            product_download_path = target_dirpath / file_name
            with open(product_download_path, "wb") as output_file:
                output_file.write(r.content)
            return product_download_path.resolve()

    def _get_aws_creds(self, token):
        logger.info("entry")

        with requests.get("https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials",
                          headers={'Authorization': f'Bearer {token}'}) as r:
            r.raise_for_status()

            return r.json()