from dist_s1.gap_finder import is_pair_disjoint, StateConfigTD, lookup


def test_is_pair_disjoint__when_skips_middle_1():
    assert is_pair_disjoint((StateConfigTD("A", 3, 361), StateConfigTD("A", 7, 361)))


def test_is_pair_disjoint__when_skips_middle_2():
    assert is_pair_disjoint((StateConfigTD("A", 0, 362), StateConfigTD("A", 7, 362)))


def test_is_pair_disjoint__when_skips_end_1():
    assert is_pair_disjoint((StateConfigTD("A", 5, 364), StateConfigTD("A", 0, 365)))


def test_is_pair_disjoint__when_skips_end_2():
    assert is_pair_disjoint((StateConfigTD("A", 3, 364), StateConfigTD("A", 0, 375)))


def test_is_pair_disjoint__when_skips_strip():
    assert is_pair_disjoint((StateConfigTD("A", 5, 364), StateConfigTD("A", 3, 365)))


def test_is_pair_disjoint__when_skips_full_cycle():
    # skips everything in between AGNs across at least 1 ACI
    assert is_pair_disjoint((StateConfigTD("A", 3, 364), StateConfigTD("A", 3, 365)))


def test_0_56KQB():
    assert not is_pair_disjoint((StateConfigTD(tile_id="56KQB", agn=0, aci=300), StateConfigTD(tile_id="56KQB", agn=0, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="56KQB", agn=0, aci=300), StateConfigTD(tile_id="56KQB", agn=0, aci=302)), lookup)  # EDGE CASE: skips full cycle


def test_1_56XNL():
    assert not is_pair_disjoint((StateConfigTD(tile_id="56XNL", agn=1, aci=300), StateConfigTD(tile_id="56XNL", agn=1, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="56XNL", agn=1, aci=300), StateConfigTD(tile_id="56XNL", agn=1, aci=302)), lookup)  # EDGE CASE: skips full cycle


def test_2_17PNT():
    assert not is_pair_disjoint((StateConfigTD(tile_id="17PNT", agn=2, aci=300), StateConfigTD(tile_id="17PNT", agn=2, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="17PNT", agn=2, aci=300), StateConfigTD(tile_id="17PNT", agn=2, aci=302)), lookup)  # EDGE CASE: skips full cycle


def test_01_60MVU():
    assert not is_pair_disjoint((StateConfigTD(tile_id="60MVU", agn=0, aci=300), StateConfigTD(tile_id="60MVU", agn=1, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="60MVU", agn=1, aci=300), StateConfigTD(tile_id="60MVU", agn=0, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="60MVU", agn=1, aci=300), StateConfigTD(tile_id="60MVU", agn=0, aci=302)), lookup)  # EDGE CASE: skips full cycle
    assert is_pair_disjoint((StateConfigTD(tile_id="60MVU", agn=0, aci=300), StateConfigTD(tile_id="60MVU", agn=0, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="60MVU", agn=1, aci=300), StateConfigTD(tile_id="60MVU", agn=1, aci=301)), lookup)


def test_12_59NMH():
    assert not is_pair_disjoint((StateConfigTD(tile_id="59NMH", agn=1, aci=300), StateConfigTD(tile_id="59NMH", agn=2, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="59NMH", agn=2, aci=300), StateConfigTD(tile_id="59NMH", agn=1, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="59NMH", agn=2, aci=300), StateConfigTD(tile_id="59NMH", agn=1, aci=302)), lookup)  # EDGE CASE: skips full cycle
    assert is_pair_disjoint((StateConfigTD(tile_id="59NMH", agn=1, aci=300), StateConfigTD(tile_id="59NMH", agn=1, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="59NMH", agn=2, aci=300), StateConfigTD(tile_id="59NMH", agn=2, aci=301)), lookup)


def test_02_60KWF():
    assert not is_pair_disjoint((StateConfigTD(tile_id="60KWF", agn=0, aci=300), StateConfigTD(tile_id="60KWF", agn=2, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="60KWF", agn=2, aci=300), StateConfigTD(tile_id="60KWF", agn=0, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="60KWF", agn=2, aci=300), StateConfigTD(tile_id="60KWF", agn=0, aci=302)), lookup)  # EDGE CASE: skips full cycle
    assert is_pair_disjoint((StateConfigTD(tile_id="60KWF", agn=0, aci=300), StateConfigTD(tile_id="60KWF", agn=0, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="60KWF", agn=2, aci=300), StateConfigTD(tile_id="60KWF", agn=2, aci=301)), lookup)


def test_012_60MXS():
    assert not is_pair_disjoint((StateConfigTD(tile_id="60MXS", agn=0, aci=300), StateConfigTD(tile_id="60MXS", agn=1, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="60MXS", agn=1, aci=300), StateConfigTD(tile_id="60MXS", agn=2, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="60MXS", agn=2, aci=300), StateConfigTD(tile_id="60MXS", agn=0, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="60MXS", agn=2, aci=300), StateConfigTD(tile_id="60MXS", agn=0, aci=302)), lookup)  # EDGE CASE: skips full cycle
    assert is_pair_disjoint((StateConfigTD(tile_id="60MXS", agn=0, aci=300), StateConfigTD(tile_id="60MXS", agn=0, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="60MXS", agn=1, aci=300), StateConfigTD(tile_id="60MXS", agn=1, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="60MXS", agn=2, aci=300), StateConfigTD(tile_id="60MXS", agn=2, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="60MXS", agn=0, aci=300), StateConfigTD(tile_id="60MXS", agn=2, aci=300)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="60MXS", agn=0, aci=300), StateConfigTD(tile_id="60MXS", agn=2, aci=301)), lookup)


def test_123_59HQV():
    assert not is_pair_disjoint((StateConfigTD(tile_id="59HQV", agn=1, aci=300), StateConfigTD(tile_id="59HQV", agn=2, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="59HQV", agn=2, aci=300), StateConfigTD(tile_id="59HQV", agn=3, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="59HQV", agn=3, aci=300), StateConfigTD(tile_id="59HQV", agn=1, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="59HQV", agn=3, aci=300), StateConfigTD(tile_id="59HQV", agn=1, aci=302)), lookup)  # EDGE CASE: skips full cycle
    assert is_pair_disjoint((StateConfigTD(tile_id="59HQV", agn=1, aci=300), StateConfigTD(tile_id="59HQV", agn=1, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="59HQV", agn=2, aci=300), StateConfigTD(tile_id="59HQV", agn=2, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="59HQV", agn=3, aci=300), StateConfigTD(tile_id="59HQV", agn=3, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="59HQV", agn=1, aci=300), StateConfigTD(tile_id="59HQV", agn=3, aci=300)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="59HQV", agn=1, aci=300), StateConfigTD(tile_id="59HQV", agn=3, aci=301)), lookup)


def test_013_58PFS():
    assert not is_pair_disjoint((StateConfigTD(tile_id="58PFS", agn=0, aci=300), StateConfigTD(tile_id="58PFS", agn=1, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="58PFS", agn=1, aci=300), StateConfigTD(tile_id="58PFS", agn=3, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="58PFS", agn=3, aci=300), StateConfigTD(tile_id="58PFS", agn=0, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="58PFS", agn=3, aci=300), StateConfigTD(tile_id="58PFS", agn=0, aci=302)), lookup)  # EDGE CASE: skips full cycle
    assert is_pair_disjoint((StateConfigTD(tile_id="58PFS", agn=0, aci=300), StateConfigTD(tile_id="58PFS", agn=0, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="58PFS", agn=1, aci=300), StateConfigTD(tile_id="58PFS", agn=1, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="58PFS", agn=3, aci=300), StateConfigTD(tile_id="58PFS", agn=3, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="58PFS", agn=0, aci=300), StateConfigTD(tile_id="58PFS", agn=3, aci=300)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="58PFS", agn=0, aci=300), StateConfigTD(tile_id="58PFS", agn=3, aci=301)), lookup)


def test_145_11SKU():
    assert not is_pair_disjoint((StateConfigTD(tile_id="11SKU", agn=1, aci=300), StateConfigTD(tile_id="11SKU", agn=4, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="11SKU", agn=4, aci=300), StateConfigTD(tile_id="11SKU", agn=5, aci=300)), lookup)
    assert not is_pair_disjoint((StateConfigTD(tile_id="11SKU", agn=5, aci=300), StateConfigTD(tile_id="11SKU", agn=1, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="11SKU", agn=5, aci=300), StateConfigTD(tile_id="11SKU", agn=1, aci=302)), lookup)  # EDGE CASE: skips full cycle
    assert is_pair_disjoint((StateConfigTD(tile_id="11SKU", agn=1, aci=300), StateConfigTD(tile_id="11SKU", agn=1, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="11SKU", agn=4, aci=300), StateConfigTD(tile_id="11SKU", agn=4, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="11SKU", agn=5, aci=300), StateConfigTD(tile_id="11SKU", agn=5, aci=301)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="11SKU", agn=1, aci=300), StateConfigTD(tile_id="11SKU", agn=5, aci=300)), lookup)
    assert is_pair_disjoint((StateConfigTD(tile_id="11SKU", agn=1, aci=300), StateConfigTD(tile_id="11SKU", agn=5, aci=301)), lookup)
