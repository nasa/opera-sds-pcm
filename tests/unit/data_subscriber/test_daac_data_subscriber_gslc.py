"""NISAR GSLC routing in daac_data_subscriber.py.

GSLCs can now be queried for DISP-NI historical processing, so the collection needs its own
provider, product type and catalog. GSLC download is OPERA-2673, so every download path must
still refuse rather than fall through to a downloader built for another product. GUNW stays
survey-only.
"""

import argparse
import sys
from unittest.mock import MagicMock, patch

import pytest

# Test modules that run earlier may have stubbed hysds with a MagicMock, which is not a package, and
# the downloaders imported here reach hysds.utils through product2dataset. Stub that only in that case.
if isinstance(sys.modules.get("hysds"), MagicMock):
    sys.modules.setdefault("hysds.utils", MagicMock())

from data_subscriber import daac_data_subscriber  # noqa: E402
from data_subscriber.cmr import (COLLECTION_TO_PRODUCT_TYPE_MAP, COLLECTION_TO_PROVIDER_TYPE_MAP, Collection,  # noqa: E402
                                 ProductType, Provider)
from data_subscriber.gslc.gslc_catalog import GSLCProductCatalog  # noqa: E402


def test_gslc_has_its_own_provider_and_product_type():
    assert COLLECTION_TO_PROVIDER_TYPE_MAP[Collection.NISAR_GSLC] == Provider.ASF_NISAR_GSLC
    assert COLLECTION_TO_PRODUCT_TYPE_MAP[Collection.NISAR_GSLC] == ProductType.NISAR_GSLC


def test_gunw_keeps_its_survey_only_placeholders():
    assert COLLECTION_TO_PROVIDER_TYPE_MAP[Collection.NISAR_GUNW] == Provider.ASF
    assert COLLECTION_TO_PRODUCT_TYPE_MAP[Collection.NISAR_GUNW] == ProductType.SLC


def test_gslc_query_dispatches_to_the_gslc_query_class():
    args = argparse.Namespace(collection=Collection.NISAR_GSLC)

    with patch.object(daac_data_subscriber, "GslcCmrQuery") as query_class:
        daac_data_subscriber.run_query(args, None, MagicMock(), "cmr", "job-1", {})

    query_class.assert_called_once()
    query_class.return_value.run_query.assert_called_once()


def test_gslc_query_catalogs_into_the_gslc_catalog():
    es_conn = daac_data_subscriber.supply_es_conn(argparse.Namespace(collection=Collection.NISAR_GSLC))

    assert isinstance(es_conn, GSLCProductCatalog)
    assert es_conn.ES_INDEX_PATTERNS == "gslc_catalog*"


def test_gslc_download_by_provider_is_not_implemented_yet():
    args = argparse.Namespace(provider=Provider.ASF_NISAR_GSLC)

    with pytest.raises(NotImplementedError, match="OPERA-2673"):
        daac_data_subscriber.run_download(args, None, MagicMock(), None, None, None, "cmr", "job-1")


@pytest.mark.parametrize("argv", [
    ["full", "-c", Collection.NISAR_GSLC],
    ["query", "-c", Collection.NISAR_GUNW],
    ["full", "-c", Collection.NISAR_GUNW],
], ids=["gslc_full", "gunw_query", "gunw_full"])
def test_unsupported_subcommands_are_refused_before_anything_runs(argv):
    with patch.object(daac_data_subscriber, "supply_job_id") as supply_job_id, \
            pytest.raises(NotImplementedError, match="is not supported for"):
        daac_data_subscriber.run(["daac_data_subscriber.py", *argv])

    supply_job_id.assert_not_called()
