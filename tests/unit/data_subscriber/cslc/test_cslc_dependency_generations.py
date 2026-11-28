"""The historical path takes one compressed CSLC per burst and boundary: the newest.

GRQ can hold more than one generation of a compressed CSLC. A frame whose ministacks were
processed again has every one of them twice, with the same burst, reference, first and last
date and a later creation time. The case observed: frames 24726 to 24728, 27 bursts, every
compressed CSLC made in March 2025 and again in April 2025. The lookup for a ministack's
download job took whichever document GRQ returned first for each burst, so one boundary could
be handed to the SAS as a mix of the two generations.

Frame 24726 of the fixture has five ministacks of k=15 in its first historical phase. The
download job of the fifth needs the compressed CSLCs of the four boundaries before it.
"""

from functools import lru_cache
from pathlib import Path
from unittest.mock import MagicMock

from data_subscriber import cslc_utils
from data_subscriber.cslc.cslc_dependency import CSLCDependency

TEST_DATA = Path(__file__).parents[1] / "test_data"
EXCERPT_DB = str(TEST_DATA / "disp_s1_with_processing_mode_excerpt.json")

FRAME = 24726
K = 15
M = 6
BOUNDARY_POSITIONS = (14, 29, 44, 59)  # last date of ministacks one to four
FIFTH_MINISTACK_END = 74
MARCH = "20250312T101500"
APRIL = "20250418T220000"


def march_first(hits):
    return sorted(hits, key=lambda hit: hit["_id"])


def april_first(hits):
    return sorted(hits, key=lambda hit: hit["_id"], reverse=True)


def mixed(hits):
    """March first for every other burst: the order GRQ returns is not the order made."""
    burst_number = int(hits[0]["_source"]["metadata"]["ccslc_m_index"].split("_")[1])
    return march_first(hits) if burst_number % 2 else april_first(hits)


class GrqWithCompressedCslcs:
    """Stand-in for the GRQ ES utility: answers the lookup of one burst and boundary."""

    def __init__(self, hits, order=march_first):
        self.hits = hits
        self.order = order

    def query(self, index=None, body=None):
        terms = {key: value for clause in body["query"]["bool"]["must"] for key, value in clause["term"].items()}
        found = [hit for hit in self.hits
                 if hit["_source"]["metadata"]["ccslc_m_index"] == terms["metadata.ccslc_m_index.keyword"]
                 and hit["_source"]["metadata"]["frame_id"] == terms["metadata.frame_id"]]
        return self.order(found) if found else []


@lru_cache(maxsize=None)
def burst_database():
    return cslc_utils.process_disp_frame_burst_hist(EXCERPT_DB, use_processing_modes=True)[0]


class Frame:
    def __init__(self):
        frame_to_bursts = burst_database()
        self.dependency = CSLCDependency(K, M, frame_to_bursts, None, None, None, None, None)
        self.dependency.logger = MagicMock()
        frame = frame_to_bursts[FRAME]
        self.bursts = sorted(frame.burst_ids)
        self.indices = frame.sensing_datetime_days_index
        self.dates = [f"{dt:%Y%m%d}" for dt in frame.sensing_datetimes]

    def hit(self, burst, position, made, version="v1.0"):
        first, last = self.dates[position - K + 1], self.dates[position]
        ccslc_id = (f"OPERA_L2_COMPRESSED-CSLC-S1_F{FRAME}_{burst}_{self.dates[0]}T000000Z_"
                    f"{first}T000000Z_{last}T000000Z_{made}Z_VV_{version}")
        return {"_id": ccslc_id, "_source": {"metadata": {
            "frame_id": FRAME,
            "ccslc_m_index": cslc_utils.build_ccslc_m_index(burst, self.indices[position]),
            "product_s3_paths": [f"s3://lts/products/CSLC_S1_COMPRESSED/{ccslc_id}/{ccslc_id}.h5"]}}}

    def generation(self, made, positions=BOUNDARY_POSITIONS, bursts=None):
        return [self.hit(burst, position, made) for position in positions for burst in (bursts or self.bursts)]

    def lookup(self, hits, order=march_first):
        return self.dependency.get_dependent_compressed_cslcs(
            FRAME, self.indices[FIFTH_MINISTACK_END], GrqWithCompressedCslcs(hits, order))


def ids(hits):
    return sorted(hit["_id"] for hit in hits)


def test_the_fixture_is_the_case_observed():
    frame = Frame()

    assert len(frame.bursts) == 27
    assert len(frame.generation(MARCH) + frame.generation(APRIL)) == 216  # 4 boundaries x 27 bursts x 2


def test_the_newest_generation_of_every_compressed_cslc_is_taken():
    frame = Frame()

    found = frame.lookup(frame.generation(MARCH) + frame.generation(APRIL))

    assert len(found) == 108
    assert ids(found) == ids(frame.generation(APRIL))


def test_the_download_job_gets_one_file_per_burst_and_boundary():
    frame = Frame()

    found = frame.lookup(frame.generation(MARCH) + frame.generation(APRIL), order=mixed)
    paths = [path for hit in found for path in hit["_source"]["metadata"]["product_s3_paths"]]

    assert len(paths) == len(set(paths)) == 108
    assert all(APRIL in path for path in paths)


def test_the_choice_does_not_depend_on_the_order_of_the_hits():
    frame = Frame()
    both = frame.generation(MARCH) + frame.generation(APRIL)

    chosen = [ids(frame.lookup(both, order)) for order in (march_first, april_first, mixed)]

    assert chosen[0] == chosen[1] == chosen[2] == ids(frame.generation(APRIL))


def test_a_generation_that_covers_only_some_bursts_wins_for_those_alone():
    frame = Frame()
    redone, newest_boundary = frame.bursts[:9], BOUNDARY_POSITIONS[-1:]
    hits = frame.generation(MARCH) + frame.generation(APRIL, newest_boundary, redone)

    found = frame.lookup(hits)

    expected = (frame.generation(MARCH, BOUNDARY_POSITIONS[:-1])
                + frame.generation(MARCH, newest_boundary, frame.bursts[9:])
                + frame.generation(APRIL, newest_boundary, redone))
    assert ids(found) == ids(expected)


def test_the_newest_of_three_generations_is_taken():
    frame = Frame()
    june = "20250601T000000"
    hits = frame.generation(APRIL) + frame.generation(june) + frame.generation(MARCH)

    assert ids(frame.lookup(hits, order=mixed)) == ids(frame.generation(june))


def test_two_made_on_the_same_day_are_told_apart_by_the_time():
    frame = Frame()
    morning, evening = "20250418T060000", "20250418T220000"
    hits = frame.generation(evening) + frame.generation(morning)

    assert ids(frame.lookup(hits, order=march_first)) == ids(frame.generation(evening))


def test_a_tie_in_creation_time_is_broken_by_the_id():
    frame = Frame()
    burst, position = frame.bursts[0], BOUNDARY_POSITIONS[-1]
    twins = [frame.hit(burst, position, APRIL, "v1.0"), frame.hit(burst, position, APRIL, "v1.1")]

    for order in (march_first, april_first):
        assert cslc_utils.newest_ccslc(order(twins))["_id"].endswith("_v1.1")


def test_an_id_that_cannot_be_read_loses_to_one_that_can():
    frame = Frame()
    readable = frame.hit(frame.bursts[0], BOUNDARY_POSITIONS[-1], MARCH)
    unreadable = {"_id": "zzz-not-a-compressed-cslc-id"}

    for hits in ([readable, unreadable], [unreadable, readable]):
        assert cslc_utils.newest_ccslc(hits) is readable


def test_a_single_document_is_returned_as_it_is():
    only = {"_id": "ccslc"}

    assert cslc_utils.newest_ccslc([only]) is only


def test_a_single_generation_is_taken_as_before_without_a_warning():
    frame = Frame()

    found = frame.lookup(frame.generation(MARCH))

    assert ids(found) == ids(frame.generation(MARCH))
    frame.dependency.logger.warning.assert_not_called()


def test_a_warning_says_how_many_had_more_than_one_generation():
    frame = Frame()
    hits = frame.generation(APRIL) + frame.generation(MARCH, BOUNDARY_POSITIONS[-1:])

    frame.lookup(hits)

    frame.dependency.logger.warning.assert_called_once()
    message = frame.dependency.logger.warning.call_args[0][0] % frame.dependency.logger.warning.call_args[0][1:]
    assert "more than one generation" in message
    assert "27 of the 108" in message


def test_two_generations_of_one_burst_do_not_stand_in_for_a_missing_burst():
    frame = Frame()
    newest_boundary = BOUNDARY_POSITIONS[-1:]
    hits = (frame.generation(APRIL, BOUNDARY_POSITIONS[:-1])
            + frame.generation(APRIL, newest_boundary, frame.bursts[:-1])
            + frame.generation(MARCH, newest_boundary, frame.bursts[:1]))

    assert frame.lookup(hits) is False
    assert frame.dependency.compressed_cslc_satisfied(
        FRAME, frame.indices[FIFTH_MINISTACK_END], GrqWithCompressedCslcs(hits)) is False
