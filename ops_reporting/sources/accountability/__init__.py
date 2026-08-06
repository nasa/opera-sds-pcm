from typing import Literal, Tuple
from datetime import datetime

from .accountability import Accountability
from .dswx_hls_accountability import DSWxHLSAccountability
from .dswx_s1_accountability import DSWxS1Accountability
from .dswx_ni_accountability import DSWxNIAccountability
from .slc_accountability import SLCAccountability
from .disp_s1_accountability import DISPS1Accountability
from .dist_s1_accountability import DISTS1Accountability
from .tropo_accountability import TropoAccountability


_ACCOUNTABILITY_CLASSES = {
    'dswx-hls': DSWxHLSAccountability,
    'rtc-s1': SLCAccountability,
    'cslc-s1': SLCAccountability,
    'dswx-s1': DSWxS1Accountability,
    'disp-s1': DISPS1Accountability,
    'dist-s1': DISTS1Accountability,
    'dswx-ni': DSWxNIAccountability,
    'tropo': TropoAccountability,
}


def get_accountability_cls_for_product(
        product: str,
        venue: Literal["PROD", "UAT", "GRQ"],
        window: Tuple[datetime, datetime] | None = None,
        **kwargs
) -> Accountability:
    product = product.lower().replace('_', '-')

    if product not in _ACCOUNTABILITY_CLASSES:
        raise ValueError(f'Accountability class {product} not recognized. '
                         f'Supported: {list(_ACCOUNTABILITY_CLASSES.keys())} (case-insensitive; '
                         f'_ and - interchangeable)')

    cls = _ACCOUNTABILITY_CLASSES[product]

    if cls == SLCAccountability:
        return cls(product, venue, window, **kwargs)
    else:
        return cls(venue, window, **kwargs)


def get_accountability_products():
    return [
        p.upper().replace('-', '_').replace('DSWX', 'DSWx') for p in _ACCOUNTABILITY_CLASSES.keys()
    ]
