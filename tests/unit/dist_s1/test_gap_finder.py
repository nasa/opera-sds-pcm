from itertools import pairwise

import pytest

from dist_s1.gap_finder import is_pair_disjoint, StateConfigTD
from tests.unit.dist_s1.gap_finder_test_input import lookup_unique


@pytest.mark.parametrize("tile_id, agns", [(k, sorted(lookup_unique[k]["agns"])) for k in lookup_unique])
def test_dist_lookup_unique_cases(tile_id, agns):
    aci_a = 300
    aci_b = 301
    aci_c = 302
    agn_first = agns[0]

    lookup = lookup_unique

    # check same AGN across an ACI gap
    for agn in agns:
        assert is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn, aci=aci_c)), lookup)

    if len(agns) == 1:
        assert not is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn_first, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn_first, aci=aci_b)), lookup)

        # aci gap
        assert is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn_first, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn_first, aci=aci_c)), lookup)  # EDGE CASE: skips full cycle
        return

    # check same AGN across adjacent ACIs (exclude lasts)
    for agn in agns[:-1]:
        assert is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn, aci=aci_b)), lookup)

    agn_last = agns[-1]

    assert not is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn_last, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn_first, aci=aci_b)), lookup)

    # aci gap
    assert is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn_last, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn_first, aci=aci_c)), lookup)  # EDGE CASE: skips full cycle

    # check adjacent AGNs in the same ACI
    for agn_a, agn_b in pairwise(agns):
        assert not is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn_a, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn_b, aci=aci_a)), lookup)

    # check non-adjacent state-configs
    for agn_a, agn_b in pairwise(agns[0::2]):
        assert is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn_a, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn_b, aci=aci_a)), lookup)
    for agn_a, agn_b in pairwise(agns[1::2]):
        assert is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn_a, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn_b, aci=aci_a)), lookup)
    for agn_a, agn_b in pairwise(agns[0::3]):
        assert is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn_a, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn_b, aci=aci_a)), lookup)
    for agn_a, agn_b in pairwise(agns[1::3]):
        assert is_pair_disjoint((StateConfigTD(tile_id=tile_id, agn=agn_a, aci=aci_a), StateConfigTD(tile_id=tile_id, agn=agn_b, aci=aci_a)), lookup)
