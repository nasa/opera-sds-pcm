import json
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from dateutil.parser import parse

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_subscriber_rtc_trigger_logic():
    mgrs_set_ids_dt = [
        ("MS_20_29", '20231101T013115Z'),
        ("MS_33_26", '20231101T225251Z'),
        ("MS_135_25", '20231108T224433Z'),
        ("MS_4_8", '20231111T230027Z'),
        ("MS_4_15", '20231111T230348Z'),
        ("MS_33_13", '20231101T224618Z'),
        ("MS_74_46", '20231023T183051Z'),
        ("MS_1_59", '20231111T183217Z'),
        ("MS_26_48", '20231101T113548Z')
    ]
    for mgrs_set_id, acq_dts in mgrs_set_ids_dt:
        dt = parse(acq_dts)  #.strftime("%Y%m%dT%H%M%SZ")
        if mgrs_set_id == "MS_74_46":
            start_dt: datetime = dt - timedelta(minutes=2)
            end_dt: datetime = dt + timedelta(minutes=2)
        else:
            start_dt: datetime = dt - timedelta(minutes=1)
            end_dt: datetime = dt + timedelta(minutes=1)

        start_dts = start_dt.strftime("%Y%m%dT%H%M%SZ")
        end_dts = end_dt.strftime("%Y%m%dT%H%M%SZ")
        print(start_dts, end_dts)

        logger.info("Running DAAC data subscriber")

        result = subprocess.run([
                "python data_subscriber/daac_data_subscriber.py query "
                "--endpoint=OPS "
                "--collection-shortname=OPERA_L2_RTC-S1_V1 "
                f'--start-date={start_dt.isoformat(timespec="seconds").replace("+00:00", "Z")} '
                f'--end-date={end_dt.isoformat(timespec="seconds").replace("+00:00", "Z")} '
                "--transfer-protocol=https "
                "--chunk-size=1 "
                "--use-temporal "
                "--job-queue=opera-job_worker-rtc_data_download "
                ""
            ],
            cwd=Path.cwd(),
            shell=True,
            text=True,
            check=True,
            capture_output=True
        )
        print(result.stdout)
        print(result.stderr)
        logger.info("Ran DAAC data subscriber")

    logger.info("Run evaluator")
    result = subprocess.run(
        ["python data_subscriber/rtc/evaluator.py --coverage-target=0 --main"],
        cwd=Path.cwd(),
        shell=True,
        text=True,
        check=True,
        capture_output=True
    )
    logger.info("Ran evaluator")
    print(result.stdout)
    print(result.stderr)

    result = json.loads(result.stdout)
    print(result)

    assert result["mgrs_sets"]["MS_20_29"][0]["coverage_actual"] == 2
    assert result["mgrs_sets"]["MS_33_26"][0]["coverage_actual"] == 80
    assert result["mgrs_sets"]["MS_135_25"][0]["coverage_actual"] == 77
    assert not result["mgrs_sets"].get("MS_4_8")
    assert not result["mgrs_sets"].get("MS_4_15")
    assert not result["mgrs_sets"].get("MS_33_13")
    assert result["mgrs_sets"]["MS_74_46"][0]["coverage_actual"] == 29
    assert result["mgrs_sets"].get("MS_1_59")[0]["coverage_actual"] == 39
    assert result["mgrs_sets"]["MS_26_48"][0]["coverage_actual"] == 60

    with Path(__file__).parent.joinpath("results").open("w") as fp:
        fp.write("PASS")
