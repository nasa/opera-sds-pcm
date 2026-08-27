"""Collection choices accepted by the batch proc CLI."""
import argparse
import sys
from unittest.mock import MagicMock, patch

import pytest

# pcm_batch reaches packages that only exist on-cluster.
for _name in ("tabulate", "hysds", "hysds.celery", "hysds_commons",
              "pcm_commons", "pcm_commons.query", "pcm_commons.query.ancillary_utility"):
    sys.modules.setdefault(_name, MagicMock())

# It also reads the deployed .sds config and the DISP-S1 burst database while
# being imported. Both are stubbed for the duration of the import only, so the
# real implementations stay in place for every other test in the session.
with patch("util.conf_util.SettingsConf", MagicMock()), \
        patch("data_subscriber.cslc_utils.localize_disp_frame_burst_hist",
              MagicMock(return_value=({}, {}, {}))):
    from tools import pcm_batch


def _collection_choices():
    """The --collection-shortname choices, which live on the 'create' subcommand."""
    for action in pcm_batch.create_parser()._actions:
        if isinstance(action, argparse._SubParsersAction):
            for subparser in action.choices.values():
                for sub_action in subparser._actions:
                    if sub_action.dest == "collection":
                        return sub_action.choices
    raise AssertionError("no --collection-shortname argument found")


@pytest.mark.parametrize("collection", ["SENTINEL-1C_SLC", "SENTINEL-1D_SLC"])
def test_batch_procs_can_be_created_for_the_newer_s1_platforms(collection):
    """Without these, no batch proc -- and so no on-demand or historical SLC
    processing -- can be created for an S1C or S1D date range."""
    assert collection in _collection_choices()


def test_the_original_collections_are_still_accepted():
    choices = _collection_choices()
    for collection in ("HLSL30", "HLSS30", "SENTINEL-1A_SLC", "SENTINEL-1B_SLC"):
        assert collection in choices
