from .granule import *
from .collection import Collection


_CLS_MAP  = {
        Collection.DSWx_HLS: DSWx_HLS_Granule,
        Collection.DSWx_S1: DSWx_S1_Granule,
        Collection.CSLC_S1: CSLC_S1_Granule,
        Collection.RTC_S1: RTC_S1_Granule,
        Collection.CSLC_S1_STATIC: CSLC_S1_STATIC_Granule,
        Collection.RTC_S1_STATIC: RTC_S1_STATIC_Granule,
        Collection.DISP_S1: DISP_S1_Granule,
        Collection.DISP_S1_STATIC: DISP_S1_STATIC_Granule,
        Collection.DIST_S1: DIST_S1_Granule,
        Collection.TROPO: TROPO_Granule,
    }


def get_granule_for_collection(coll: Collection, cmr_item: dict, **kwargs) -> Granule:
    cls = _CLS_MAP.get(coll)

    if cls is None:
        raise ValueError(f'{coll} is not implemented')

    return cls.from_cmr_dict(cmr_item, **kwargs)


def does_collection_use_iso_xml(coll: Collection) -> bool:
    cls = _CLS_MAP.get(coll)

    if cls is None:
        return False

    return getattr(cls, '_ReadIsoXml', False)
