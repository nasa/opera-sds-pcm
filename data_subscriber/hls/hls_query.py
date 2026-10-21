from pathlib import Path

from data_subscriber.query import BaseQuery
from data_subscriber.hls.hls_catalog import HLSSpatialProductCatalog
from data_subscriber.url import form_batch_id


class HlsCmrQuery(BaseQuery):
    """Class used to query the Common Metadata Repository (CMR) for Harmonized Landsat and Sentinel-1 (HLS) products."""
    _spatial_catalog_conn = None

    def spatial_catalog_conn(self):
        if self._spatial_catalog_conn is None:
            self._spatial_catalog_conn = HLSSpatialProductCatalog(self.logger)
        return self._spatial_catalog_conn

    def catalog_granules(self, granules, query_dt, force_es_conn=None, use_bulk=False, parallelize=False):
        # Look up which catalog docs already exist in a few batched searches up front, instead of one
        # sorted search per file and per granule while cataloging
        es_conn = force_es_conn if force_es_conn else self.es_conn
        es_conn.prefetch_existing([
            form_batch_id(Path(url).name, granule.get("revision_id"))
            for granule in granules
            for url in granule.get("filtered_urls") or []
        ])
        self.spatial_catalog_conn().prefetch_existing([granule["granule_id"] for granule in granules])

        return super().catalog_granules(granules, query_dt, force_es_conn=force_es_conn,
                                        use_bulk=use_bulk, parallelize=parallelize)

    def update_granule_index(self, granule, bulk=None):
        self.spatial_catalog_conn().process_granule(granule, bulk=bulk)

    def determine_download_granules(self, granules):
        if not self.args.granule_dedupe or self.args.native_id:
            self.logger.info('Skipping granule dedupe check')
            return granules

        filtered_granules = []

        flag_unsubmitted = False

        cataloged = self.es_conn.get_cataloged_granules_by_granule_ids([granule['granule_id'] for granule in granules])

        for granule in granules:
            catalog_entries = cataloged.get(granule['granule_id'], [])

            if len(catalog_entries) == 0:
                self.logger.info(f'Found new granule {granule["granule_id"]}')
                filtered_granules.append(granule)
            else:
                download_job_ids = set([
                    r['_source']['download_job_id'] for r in catalog_entries if 'download_job_id' in r['_source']
                ])

                if len(download_job_ids) == 0:
                    self.logger.info(f'Found unsubmitted granule {granule["granule_id"]}')
                    filtered_granules.append(granule)
                    flag_unsubmitted = True
                else:
                    if len(download_job_ids) > 1:
                        self.logger.warning(f'Granule {granule["granule_id"]} has multiple associated '
                                            f'download_job_ids. This should not happen and may be indicative of a '
                                            f'duplicate submission.')

                    self.logger.info(f'Dropping granule {granule["granule_id"]} as it has already been cataloged')

        if flag_unsubmitted:
            self.logger.warning('Query job has detected granules that were previously cataloged but don\'t appear to '
                                'have been submitted. There is a very small chance for duplicate creation.')

        return filtered_granules
