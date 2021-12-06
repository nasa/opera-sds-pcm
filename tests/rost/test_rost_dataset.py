import json
import os
from util.common_util import lower_keys
from rost import rost_pge


def test_rost_parser_output():
    ROST_FILE = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "test-files/orost",
        "id_00-0a-0100_orost-2023001-c001-d01-v01.xml",
    )
    EXPECTED_OUTPUT_JSON = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "test-files/orost",
        "id_00-0a-0100_orost-2023001-c032-d01-v01.json",
    )

    with open(EXPECTED_OUTPUT_JSON) as json_file:
        expected = json.load(json_file)

    out = rost_pge.parse(ROST_FILE)
    expected = lower_keys(expected)

    assert (
        expected["table_record"][0]["start_time"]
        == out["table_record"][0]["start_time"]
    )
    assert (
        expected["table_record"][0]["command_length"]
        == out["table_record"][0]["command_length"]
    )
    assert expected["table_record"][0]["opcode"] == out["table_record"][0]["opcode"]
    assert expected["table_record"][0]["rc_id"] == out["table_record"][0]["rc_id"]
    assert (
        expected["table_record"][0]["number_of_pulses"]
        == out["table_record"][0]["number_of_pulses"]
    )
    assert (
        expected["table_record"][0]["playback_urgency"]
        == out["table_record"][0]["playback_urgency"]
    )

    assert (
        expected["table_record"][1]["number_of_pulses"]
        == out["table_record"][1]["number_of_pulses"]
    )
    assert (
        expected["table_record"][1]["playback_urgency"]
        == out["table_record"][1]["playback_urgency"]
    )
    assert (
        expected["table_record"][2]["number_of_pulses"]
        == out["table_record"][2]["number_of_pulses"]
    )
    assert (
        expected["table_record"][2]["playback_urgency"]
        == out["table_record"][2]["playback_urgency"]
    )


def main():
    test_rost_parser_output()


if __name__ == "__main__":
    main()
