from geo.geo_util import is_in_north_america


def test_costa_rican_border_point():
    assert is_in_north_america(-83.64435787741405, 10.925930079609543)


def test_salvadoran_border_point():
    assert is_in_north_america(-89.36162085002218, 14.41547780375771)
