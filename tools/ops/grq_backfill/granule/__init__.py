from .granule import *
from .collection import Collection


def get_granule_for_collection(coll: Collection, cmr_item: dict) -> Granule:
    cls = {
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
    }.get(coll)

    if cls is None:
        raise ValueError(f'{coll} is not implemented')

    return cls.from_cmr_dict(cmr_item)
