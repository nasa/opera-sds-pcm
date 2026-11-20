"""state-config audit tool (DIST-S1). Forward mode only."""
import argparse
import logging
from datetime import UTC, datetime

import pandas as pd
from dateutil.parser import isoparse
from more_itertools import partition

from dist_s1.tools.validation.state_config_inspection.models.forward import DetailedRow, SummaryRow
from dist_s1.tools.validation.state_config_inspection.services.audit import \
    state_configs_group_by_tile, state_config_comparator
from dist_s1.tools.validation.state_config_inspection.services.dao import get_client, \
    fetch_state_config_docs_forward, ENVIRONMENTS

logger = logging.getLogger(__name__)


def main():
    args = create_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03dZ %(levelname)-8s %(name)s:%(lineno)d - %(message)s")

    client = get_client(args.env)
    state_configs = [d["metadata"] for d in fetch_state_config_docs_forward(client)]
    tile_to_state_configs = state_configs_group_by_tile(state_configs)
    for tile in tile_to_state_configs:
        tile_to_state_configs[tile] = sorted(tile_to_state_configs[tile], key=state_config_comparator)

    summary_rows = []
    detailed_rows = []
    for tile in tile_to_state_configs:
        scs = tile_to_state_configs[tile]

        for sc in scs:
            # move on if state configs are reasonably processed or processing
            if sc.get("dist_s1_id"):
                continue
            if sc["status"] == "PENDING":
                logger.info("state-config queued. skipping.")
                continue

            row = DetailedRow(
                name=sc["batch_id"],
                status=sc["status"],
                bursts_expected=sc["expected_burst_count"],
                bursts_actual=sc["actual_burst_count"]
            )
            detailed_rows.append(row)

        num_sc = len(tile_to_state_configs[tile])
        scs_incomplete, scs_complete = partition(lambda sc: sc.get("dist_s1_id"), tile_to_state_configs[tile])
        scs_incomplete, scs_complete = list(scs_incomplete), list(scs_complete)
        num_sc_queued = sum(1 for sc in scs_incomplete if sc["status"] == "PENDING")
        if num_sc_queued:
            logger.info(f"{num_sc_queued=}")
        num_dist_s1 = len(scs_complete)
        num_dist_s1_missing = len(scs_incomplete)
        row = SummaryRow(
            tile_id=tile,
            num_sc=num_sc,  # expected
            num_dist_s1=num_dist_s1,  # actual
            num_dist_s1_missing=num_dist_s1_missing,
        )
        summary_rows.append(row)

    start_datetime_str = args.start_datetime.isoformat(timespec="seconds")
    end_datetime_str = args.end_datetime.isoformat(timespec="seconds")
    now_str = datetime.now(UTC).isoformat(timespec="seconds")

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(f"summary - {start_datetime_str} to {end_datetime_str} - {now_str}.csv")

    detailed_df = pd.DataFrame(detailed_rows)
    detailed_df.to_csv(f"detailed - {start_datetime_str} to {end_datetime_str} - {now_str}.csv")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env",
        choices=ENVIRONMENTS.keys(),
        default="local",
        required=True,
        help="Environment whose GRQ to query against. (default: %(default)s)",
    )
    parser.add_argument(
        "--start-datetime",
        type=isoparse,
        required=True,
        help="Inclusive start of the query window (UTC)",
    )
    parser.add_argument(
        "--end-datetime",
        type=isoparse,
        required=True,
        help="Exclusive end of the query window (UTC)",
    )
    return parser


if __name__ == '__main__':
    main()
