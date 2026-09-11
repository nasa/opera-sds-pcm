import re
from abc import ABC
from os.path import basename
from typing import List, Literal
from urllib.parse import urlparse

from dateutil.parser import parse

from util.conf_util import PGEOutputsConf


class File:
    def __init__(self, s3_url: str, primary: bool, match: re.Match, granule_id: str):
        self.file_name = basename(s3_url)

        parsed = urlparse(s3_url)
        self.s3_url = s3_url
        self.https_url = f'https://{parsed.netloc}.us-west-2.amazonaws.com/{parsed.path}'

        self.is_primary = primary
        self._match = match
        self._granule_id = granule_id

    def to_dict(self):
        d = {
            'FileLocation': f'/datasets/{self._granule_id}',
            'FileSize': 0,
            'FileName': self.file_name,
        }

        if self._match:
            d.update(self._match.groupdict())

        return d


class Granule(ABC):
    _CollectionName = None
    _ProductType = None
    _Dataset = None
    _IPath = None
    _Level = None
    _DAACCollection = None

    _IndexPrefix = None

    def __init__(self, granule_id):
        self.id = granule_id

        self.files = None
        self.creation_timestamp = None
        self.gcid = None
        self.geometry = None

        self.pge_version = None
        self.sas_version = None

        self.input_granules = None

        self.extra_ds_metadata = {}
        self.extra_met_metadata = {}

        self.product_version = None

    @staticmethod
    def get_additional_attribute_by_name(cmr_dict, name):
        additional_attributes = cmr_dict['umm'].get('AdditionalAttributes', [])

        v = None

        for attribute in additional_attributes:
            if attribute['Name'] == name:
                v = attribute['Values']
                break

        if v is not None and len(v) == 1:
            v = v[0]

        return v

    @staticmethod
    def _get_files_by_schema(
            urls_list: List[dict],
            schema: Literal['s3', 'https'],
            primary_pattern: re.Pattern,
            granule_id: str,
    ) -> List['File']:
        files = []

        for url_dict in urls_list:
            if not url_dict['URL'].startswith(schema):
                continue

            url = url_dict['URL']
            primary_match = primary_pattern.fullmatch(basename(url))

            files.append(File(
                url,
                primary_match is not None,
                primary_match,
                granule_id
            ))

        return files

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        return granule

    @classmethod
    def from_cmr_dict(cls, cmr_dict):
        if any(
            [f is None for f in (cls._CollectionName, cls._ProductType, cls._Dataset, cls._IPath,
                                 cls._Level, cls._DAACCollection, cls._IndexPrefix)]
        ):
            raise TypeError(f'type {type(cls)} is not fully implemented')

        outputs = PGEOutputsConf().cfg

        output_conf = outputs.get(cls._Dataset)

        if output_conf is None:
            raise ValueError(f'could not find pge output config for dataset {cls._Dataset}')

        granule_id = cmr_dict['meta']['native-id']
        creation_timestamp = cmr_dict['meta']['revision-date']
        # acquisition_timestamp = cmr_dict['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime']
        gcid = cmr_dict['meta']['concept-id']

        granule = cls(granule_id)

        granule.creation_timestamp = creation_timestamp
        # granule.acquisition_timestamp = acquisition_timestamp
        granule.gcid = gcid

        files = cls._get_files_by_schema(
            cmr_dict['umm']['RelatedUrls'],
            's3',
            output_conf['Outputs']['Primary'][0]['regex'],
            granule_id
        )

        granule.files = files

        granule.pge_version = cmr_dict['umm'].get('PGEVersionClass', {}).get('PGEVersion')

        for ident in cmr_dict['umm'].get('DataGranule', {}).get('Identifiers', []):
            if ident.get('IdentifierName') == 'SASVersionId':
                granule.sas_version = ident['Identifier']
            elif ident.get('IdentifierName') == 'PGEVersionId' and granule.pge_version is None:
                granule.pge_version = ident['Identifier']

        granule.input_granules = cmr_dict['umm'].get('InputGranules', [])

        return cls._decorate_from_cmr_dict(granule, cmr_dict)

    def _decorate_grq_doc(self, grq_doc: dict) -> dict:
        grq_doc.update(self.extra_ds_metadata)
        grq_doc['metadata'].update(self.extra_met_metadata)

        return grq_doc

    def _to_basic_grq_doc(self):
        doc = {
            'id': self.id,
            'objectid': self.id,
            'metadata': {
                'Files': [f.to_dict() for f in self.files if f.is_primary],
                'FileSize': 0,
                'id': self.id,
                'product_urls': [f.https_url for f in self.files if f.is_primary],
                'product_s3_paths': [f.s3_url for f in self.files if f.is_primary],
                'InputProductReceivedTime': self.creation_timestamp,
                'pge_version': self.pge_version or 'UNKNOWN',
                'sas_version': self.sas_version or 'UNKNOWN',
                'pcm_version': 'UNKNOWN',
                'collection_name': self._CollectionName,
                'ProductVersion': self.product_version or '1.0',
                'lineage': self.input_granules,
                'tags': ['PGE', 'daac_delivered'],
                'ProductReceivedTime': self.creation_timestamp,
                'ProductReceivedYear': self.creation_timestamp[:4],
                'ProductReceivedMonth': self.creation_timestamp[5:7],
                'ProductReceivedDay': self.creation_timestamp[8:10],
                'ProductType': self._ProductType,
                'dataset_version': f"v{self.product_version or '1.0'}",
                'accountability': {}
            },
            'dataset': self._Dataset,
            'ipath': self._IPath,
            'system_version': f"v{self.product_version or '1.0'}",
            'dataset_level': self._Level,
            'dataset_type': self._Dataset,
            'urls': [],
            'browse_urls': [],
            'images': [],
            'prov': {},
            'version': f"v{self.product_version or '1.0'}",
            'creation_timestamp': self.creation_timestamp.removesuffix("Z"),
            'grq_index_result': {
                'index': None
            },
            '@timestamp': self.creation_timestamp,
            'daac_CNM_S_status': 'SUCCESS',
            'daac_CNM_S_timestamp': self.creation_timestamp,
            'daac_received_timestamp': self.creation_timestamp,
            'daac_submission_timestamp': self.creation_timestamp,
            'daac_catalog_url': f'https://cmr.earthdata.nasa.gov/search/concepts/{self.gcid}.umm_json',
            'daac_collection': self._DAACCollection,
            'daac_process_complete_timestamp': '2026-09-06 00:25:20Z',
            'daac_catalog_id': self.gcid,
            'daac_identifier': self.id,
            'daac_delivery_error_message': None,
            'daac_product_file_urls': [f.s3_url for f in self.files],
            'daac_delivery_status': 'SUCCESS',
            'archive_product_urls': [f.s3_url for f in self.files if f.is_primary],
            'is_backfilled': True
        }

        return doc

    def to_grq_doc(self):
        index = self._IndexPrefix.removesuffix('-') + '-' + parse(self.creation_timestamp).strftime('%Y.%m')
        return self.id, index, self._decorate_grq_doc(self._to_basic_grq_doc())


class DSWx_HLS_Granule(Granule):
    _CollectionName = "OPERA_L3_DSWX-HLS_V1"
    _ProductType = "L3_DSWx_HLS"
    _Dataset = "L3_DSWx_HLS"
    _IPath = "hysds::data/L3_DSWx_HLS"
    _Level = "L3"
    _DAACCollection = "OPERA_L3_DSWX-HLS_V1"

    _IndexPrefix = "grq_v1.1_l3_dswx_hls"

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        input_hls = cls.get_additional_attribute_by_name(cmr_dict, 'HlsDataset')
        prod_version = cls.get_additional_attribute_by_name(cmr_dict, 'ProductVersion')

        granule.product_version = prod_version
        granule.extra_met_metadata['input_granule_id'] = input_hls

        return granule


class DSWx_S1_Granule(Granule):
    _CollectionName = 'OPERA_L3_DSWX-S1_V1'  # metadata.collection_name
    _ProductType = 'L3_DSWx_S1'  # metadata.ProductType
    _Dataset = 'L3_DSWx_S1'  # dataset_type
    _IPath = 'hysds::data/L3_DSWx_S1'  # ipath
    _Level = 'L3'  # dataset_level
    _DAACCollection = 'OPERA_L3_DSWX-S1_V1'  # daac_collection

    _IndexPrefix = 'grq_v1.0_l3_dswx_s1'

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        prod_version = cmr_dict['umm'].get('CollectionReference', {}).get('Version', '1.0')
        mgrs_tile_id = cls.get_additional_attribute_by_name(cmr_dict, 'MGRS_TILE_ID')
        rtc_input_list = granule.input_granules
        rtc_sensing_start_time = cmr_dict['umm'].get('TemporalExtent', {}).get('RangeDateTime', {}).get('BeginningDateTime')
        rtc_sensing_end_time = cmr_dict['umm'].get('TemporalExtent', {}).get('RangeDateTime', {}).get('EndingDateTime')

        granule.product_version = prod_version
        granule.extra_met_metadata = {
            'tile_id': mgrs_tile_id,
            'rtc_sensing_start_time': rtc_sensing_start_time,
            'rtc_sensing_end_time': rtc_sensing_end_time,
            'rtc_input_list': rtc_input_list,
        }

        return granule


class CSLC_S1_Granule(Granule):
    _CollectionName = 'OPERA_L2_CSLC-S1_V1'  # metadata.collection_name
    _ProductType = 'L2_CSLC_S1'  # metadata.ProductType
    _Dataset = 'L2_CSLC_S1'  # dataset_type
    _IPath = 'hysds::data/L2_CSLC_S1'  # ipath
    _Level = 'L2'  # dataset_level
    _DAACCollection = 'OPERA_L2_CSLC-S1_V1'  # daac_collection

    _IndexPrefix = 'grq_v1.1_l2_cslc_s1'

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        prod_version = cls.get_additional_attribute_by_name(cmr_dict, 'PRODUCT_VERSION')

        granule.product_version = prod_version
        granule.extra_met_metadata['input_granule_id'] = granule.input_granules[0]

        return granule


class CSLC_S1_STATIC_Granule(Granule):
    _CollectionName = 'OPERA_L2_CSLC-S1-STATIC_V1'  # metadata.collection_name
    _ProductType = 'L2_CSLC_S1_STATIC'  # metadata.ProductType
    _Dataset = 'L2_CSLC_S1_STATIC'  # dataset_type
    _IPath = 'hysds::data/L2_CSLC_S1_STATIC'  # ipath
    _Level = 'L2'  # dataset_level
    _DAACCollection = 'OPERA_L2_CSLC-S1-STATIC_V1'  # daac_collection

    _IndexPrefix = 'grq_v1.0_l2_cslc_s1_static'

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        prod_version = cls.get_additional_attribute_by_name(cmr_dict, 'PRODUCT_VERSION')

        granule.product_version = prod_version
        granule.extra_met_metadata['input_granule_id'] = granule.input_granules[0]

        return granule


class RTC_S1_Granule(Granule):
    _CollectionName = 'OPERA_L2_RTC-S1_V1'  # metadata.collection_name
    _ProductType = 'L2_RTC_S1'  # metadata.ProductType
    _Dataset = 'L2_RTC_S1'  # dataset_type
    _IPath = 'hysds::data/L2_RTC_S1'  # ipath
    _Level = 'L2'  # dataset_level
    _DAACCollection = 'OPERA_L2_RTC-S1_V1'  # daac_collection

    _IndexPrefix = 'grq_v1.0_l2_rtc_s1'

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        prod_version = cls.get_additional_attribute_by_name(cmr_dict, 'PRODUCT_VERSION')

        granule.product_version = prod_version
        granule.extra_met_metadata['input_granule_id'] = granule.input_granules[0]

        return granule


class RTC_S1_STATIC_Granule(Granule):
    _CollectionName = 'OPERA_L2_RTC-S1-STATIC_V1'  # metadata.collection_name
    _ProductType = 'L2_RTC_S1_STATIC'  # metadata.ProductType
    _Dataset = 'L2_RTC_S1_STATIC'  # dataset_type
    _IPath = 'hysds::data/L2_RTC_S1_STATIC'  # ipath
    _Level = 'L2'  # dataset_level
    _DAACCollection = 'OPERA_L2_RTC-S1-STATIC_V1'  # daac_collection

    _IndexPrefix = 'grq_v1.0_l2_rtc_s1_static'

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        prod_version = cls.get_additional_attribute_by_name(cmr_dict, 'PRODUCT_VERSION')

        granule.product_version = prod_version
        granule.extra_met_metadata['input_granule_id'] = granule.input_granules[0]

        return granule


class DISP_S1_Granule(Granule):
    _CollectionName = 'OPERA_L3_DISP-S1_V1'  # metadata.collection_name
    _ProductType = 'L3_DISP_S1'  # metadata.ProductType
    _Dataset = 'L3_DISP_S1'  # dataset_type
    _IPath = 'hysds::data/L3_DISP_S1'  # ipath
    _Level = 'L3'  # dataset_level
    _DAACCollection = 'OPERA_L3_DISP-S1_V1'  # daac_collection

    _IndexPrefix = 'grq_v1.0_l3_disp_s1'

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        prod_version = cls.get_additional_attribute_by_name(cmr_dict, 'ProductVersion')
        frame_id = cls.get_additional_attribute_by_name(cmr_dict, 'FRAME_NUMBER')

        granule.product_version = prod_version
        granule.extra_met_metadata['frame_id'] = int(frame_id)

        return granule


class DISP_S1_STATIC_Granule(Granule):
    _CollectionName = 'OPERA_L3_DISP-S1-STATIC_V1'  # metadata.collection_name
    _ProductType = 'L3_DISP_S1_STATIC'  # metadata.ProductType
    _Dataset = 'L3_DISP_S1_STATIC'  # dataset_type
    _IPath = 'hysds::data/L3_DISP_S1_STATIC'  # ipath
    _Level = 'L3'  # dataset_level
    _DAACCollection = 'OPERA_L3_DISP-S1-STATIC_V1'  # daac_collection

    _IndexPrefix = 'grq_v1.0_l3_disp_s1_static'

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        prod_version = cls.get_additional_attribute_by_name(cmr_dict, 'PRODUCT_VERSION')
        frame_number = cls.get_additional_attribute_by_name(cmr_dict, 'FRAME_NUMBER')

        granule.product_version = prod_version
        granule.extra_met_metadata['input_granule_id'] = str(frame_number)

        return granule


class DIST_S1_Granule(Granule):
    _CollectionName = 'OPERA_L3_DIST-ALERT-S1_V1'  # metadata.collection_name
    _ProductType = 'L3_DIST_S1'  # metadata.ProductType
    _Dataset = 'L3_DIST_S1'  # dataset_type
    _IPath = 'hysds::data/L3_DIST_S1'  # ipath
    _Level = 'L3'  # dataset_level
    _DAACCollection = 'OPERA_L3_DIST-ALERT-S1_V1'  # daac_collection

    _IndexPrefix = 'grq_v1.0_l3_dist_s1'

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        prod_version = cls.get_additional_attribute_by_name(cmr_dict, 'PRODUCT_VERSION')
        mgrs_tile = cls.get_additional_attribute_by_name(cmr_dict, 'MGRS_TILE_ID')

        granule.product_version = prod_version
        granule.extra_met_metadata['mgrs_tile_id'] = mgrs_tile

        return granule


class TROPO_Granule(Granule):
    _CollectionName = 'OPERA_L4_TROPO-ZENITH_V1'  # metadata.collection_name
    _ProductType = 'L4_TROPO'  # metadata.ProductType
    _Dataset = 'L4_TROPO'  # dataset_type
    _IPath = 'hysds::data/L4_TROPO'  # ipath
    _Level = 'L4'  # dataset_level
    _DAACCollection = 'OPERA_L4_TROPO-ZENITH_V1'  # daac_collection

    _IndexPrefix = 'grq_v1.0_l4_tropo'

    @classmethod
    def _decorate_from_cmr_dict(cls, granule: 'Granule', cmr_dict: dict) -> 'Granule':
        prod_version = cls.get_additional_attribute_by_name(cmr_dict, 'PRODUCT_VERSION')

        granule.product_version = prod_version
        granule.extra_met_metadata['input_granule_id'] = f'{granule.id[22:30]}/{granule.input_granules[0]}.nc'

        return granule
