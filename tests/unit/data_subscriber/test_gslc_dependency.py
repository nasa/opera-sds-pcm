"""Compressed-GSLC prerequisites of a DISP-NI historical k-set.

A k-set consumes the compressed GSLCs of up to m - 1 preceding k-sets, so submitting it before
they are cataloged would run the PGE without them. Each compressed GSLC is found by its frame and
the last sensing date of the k-set that produced it; the extractor stores that date as an ISO
datetime at midnight, so the lookup has to match the whole day rather than a YYYYMMDD string.
"""

from datetime import datetime, timedelta

import pytest

from data_subscriber.gslc.gslc_dependency import COMPRESSED_GSLC_ES_INDEX_PATTERNS, GSLCDependency
from data_subscriber.gslc.gslc_utils import NisarFrame

K = 15
M = 6
FRAME = 26410
FIRST = datetime(2025, 11, 8, 15, 50, 41)
DATES = [FIRST + timedelta(days=12 * i) for i in range(120)]


class FakeEs:
    """Answers compressed-GSLC lookups from the (frame_id, last date) pairs that were published."""

    def __init__(self, published=()):
        self.published = set(published)
        self.queries = []

    def query(self, index=None, body=None):
        self.queries.append((index, body))
        frame_term, date_range = body["query"]["bool"]["must"]
        frame_id = frame_term["term"]["metadata.frame_id"]
        day = date_range["range"]["metadata.last_date_time"]["gte"][:10]
        return [{"_id": f"{frame_id}-{day}"}] if (frame_id, day) in self.published else []


def published_for(*positions):
    return [(FRAME, DATES[p].strftime("%Y-%m-%d")) for p in positions]


@pytest.fixture
def dependency():
    return GSLCDependency(K, M, {FRAME: NisarFrame(frame_number=FRAME, nisar_frames={"151_?_011"},
                                                   sensing_datetimes=DATES)})


@pytest.mark.parametrize("position, m", [(0, 1), (14, 1), (15, 2), (30, 3), (74, 5), (75, 6), (90, 6), (105, 6)])
def test_m_ramps_up_by_one_per_k_set_until_it_reaches_m(dependency, position, m):
    assert dependency.m_for_position(position) == m


def test_the_first_k_set_needs_no_compressed_gslc(dependency):
    assert dependency.required_compressed_gslc_last_dates(FRAME, 0) == []
    assert dependency.compressed_gslc_satisfied(FRAME, 0, FakeEs()) is True


def test_the_second_k_set_needs_the_compressed_gslc_ending_on_the_first_k_sets_last_date(dependency):
    assert dependency.required_compressed_gslc_last_dates(FRAME, K) == [DATES[K - 1]]


def test_later_k_sets_need_the_last_m_minus_one_compressed_gslcs_newest_first(dependency):
    position = 6 * K

    assert dependency.required_compressed_gslc_last_dates(FRAME, position) == \
        [DATES[position - 1 - j * K] for j in range(M - 1)]
    assert DATES[K - 1] not in dependency.required_compressed_gslc_last_dates(FRAME, position)


def test_k_set_is_blocked_until_its_compressed_gslc_is_cataloged(dependency):
    assert dependency.compressed_gslc_satisfied(FRAME, K, FakeEs()) is False
    assert dependency.compressed_gslc_satisfied(FRAME, K, FakeEs(published_for(K - 1))) is True


def test_every_required_compressed_gslc_must_be_cataloged(dependency):
    position = 2 * K

    assert dependency.compressed_gslc_satisfied(FRAME, position, FakeEs(published_for(position - 1))) is False
    assert dependency.compressed_gslc_satisfied(FRAME, position, FakeEs(published_for(position - 1, K - 1))) is True


def test_another_frames_compressed_gslc_does_not_count(dependency):
    es = FakeEs([(FRAME + 1, DATES[K - 1].strftime("%Y-%m-%d"))])

    assert dependency.compressed_gslc_satisfied(FRAME, K, es) is False


def test_lookup_matches_the_whole_last_day_in_the_compressed_gslc_index(dependency):
    es = FakeEs()
    dependency.compressed_gslc_satisfied(FRAME, K, es)

    (index, body), = es.queries
    date_range = body["query"]["bool"]["must"][1]["range"]["metadata.last_date_time"]
    last_day = DATES[K - 1].date()
    assert index == COMPRESSED_GSLC_ES_INDEX_PATTERNS
    assert date_range == {"gte": f"{last_day}T00:00:00", "lt": f"{last_day + timedelta(days=1)}T00:00:00"}
    # A compressed GSLC's last_date_time as FilenameRegexMetExtractor writes it falls inside the range
    assert date_range["gte"] <= f"{last_day}T00:00:00.000000Z" < date_range["lt"]
