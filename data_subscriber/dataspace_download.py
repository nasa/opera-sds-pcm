import os

import requests
import backoff
from util.backoff_util import fatal_code, backoff_logger
from util.dataspace_util import DEFAULT_DOWNLOAD_ENDPOINT, DataspaceSession

from data_subscriber.download import BaseDownload
from data_subscriber.asf_slc_download import AsfDaacSlcDownload

from data_subscriber.url import _has_https_url
from sys import stderr
from datetime import datetime
from pathlib import Path
from data_subscriber.url import form_batch_id


class DataspaceDownload(AsfDaacSlcDownload):
    def __init__(self, provider):
        super().__init__(provider)

    def perform_download(self, session: requests.Session, es_conn, downloads: list[dict], args, token, job_id):
        username, password = self.get_dataspace_login()

        with DataspaceSession(username, password) as session:
            for download in downloads:
                if not _has_https_url(download):
                    continue

                self.logger.info(f'Processing product {download["granule_id"]}')
                product_id = download['granule_id'][:-4]

                product_download_dir = self.downloads_dir / product_id
                product_download_dir.mkdir(exist_ok=True)

                if args.dry_run:
                    self.logger.info("args.dry_run=%s. Skipping download.", args.dry_run)
                    continue

                base_url, packed_url = self._get_download_endpoint_urls(download['https_url'])

                product = product_filepath = Path(os.path.join(product_download_dir, product_id + '.zip')).resolve()

                success, err = self.try_download_https(packed_url, product_filepath, session)

                if not success:
                    self.logger.warning(f'Failed to download compressed product from {packed_url}, '
                                        f'falling back to base product')
                    if err is not None:
                        self.logger.error(err)

                    success, err = self.try_download_https(base_url, product_filepath, session)

                    if not success:
                        self.logger.error(f'Failed to download product from backup URL {base_url}')

                        if err is not None:
                            self.logger.error(err)
                            raise err
                        else:
                            raise RuntimeError(f'Failed to download product from backup URL {base_url}')
                    else:
                        downloaded_url = base_url
                else:
                    downloaded_url = packed_url

                self.logger.info("Marking %s as downloaded.", product_filepath)
                self.logger.debug("download['id']=%s", download['id'])

                es_conn.mark_product_as_downloaded(download['id'], job_id)

                self.logger.debug(f"product_url_downloaded={downloaded_url}")

                additional_metadata = {}

                try:
                    additional_metadata['processing_mode'] = download['processing_mode']
                except KeyError:
                    self.logger.warning("processing_mode not found in the slc_catalog ES index")

                if download.get("intersects_north_america"):
                    self.logger.info("Adding intersects_north_america to dataset metadata")
                    additional_metadata["intersects_north_america"] = True

                dataset_dir = self.extract_one_to_one(product, self.cfg, working_dir=Path.cwd(),
                                                      extra_metadata=additional_metadata,
                                                      name_postscript='-r' + str(download['revision_id']))

                self.update_pending_dataset_with_index_name(dataset_dir, '-r' + str(download['revision_id']))

                # Rename the dataset_dir to match the pattern w revision_id
                new_dataset_dir = dataset_dir.parent / form_batch_id(dataset_dir.name, str(download['revision_id']))
                self.logger.debug("new_dataset_dir=%s", str(new_dataset_dir))

                os.rename(str(dataset_dir), str(new_dataset_dir))

                self.download_orbit_file(new_dataset_dir, product_filepath)

                # We've observed cases where the orbit file download seems to complete
                # successfully, but the resulting files are empty, causing the PGE/SAS to crash.
                # Check for any empty files now, so we can fail during this download job
                # rather than during the SCIFLO job.
                self.check_for_empty_orbit_files(new_dataset_dir)

                if additional_metadata['processing_mode'] in ("historical", "reprocessing"):
                    self.logger.info(
                        "Processing mode is %s. Attempting to download ionosphere correction file.",
                        additional_metadata['processing_mode']
                    )
                    self.download_ionosphere_file(new_dataset_dir, product_filepath)

                self.logger.info("Removing %s", product_filepath)
                product_filepath.unlink(missing_ok=True)

    def try_download_https(self, product_url, dst, session: DataspaceSession):
        start_t = datetime.now()
        try:
            headers = {"Authorization": f"Bearer {session.token}"}

            response = requests.get(product_url, headers=headers, stream=True)
            self.logger.info(f'Download request {response.url}: {response.status_code}')

            if response.status_code >= 400:
                self.logger.error(f'Failed to download {product_url}: {response.status_code}')
                print(response.text, file=stderr)
                return False, None

            size = 0

            with open(dst, 'wb') as fp:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        size += len(chunk)
                        fp.write(chunk)

            self.logger.info(f'Completed download to {dst} ({size:,} bytes in {datetime.now() - start_t})')
            return True, None
        except Exception as e:
            self.logger.warning(f'Failed to download {product_url}: {e}')
            return False, e

    def _get_download_endpoint_urls(self, doc_url: str):
        if doc_url.endswith('$value'):
            base_url = doc_url
            packed_url = doc_url.removesuffix('$value') + '$zip'
        elif doc_url.endswith('$zip'):
            base_url = doc_url.removesuffix('$zip') + '$value'
            packed_url = doc_url
        else:
            raise ValueError(f'Unrecognized download endpoint url format: {doc_url}')

        return base_url, packed_url
