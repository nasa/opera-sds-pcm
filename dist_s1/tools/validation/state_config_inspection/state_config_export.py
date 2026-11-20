"""state-config (DIST-S1) CSV export tool."""
import argparse
import logging
from pathlib import PurePath

import pandas as pd

from dist_s1.tools.validation.state_config_inspection.services.dao import get_local_client, scan_all

logger = logging.getLogger(__name__)


def main():
    args = create_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03dZ %(levelname)-8s %(name)s:%(lineno)d - %(message)s")

    # fetch
    if args.mode == "forward":
        state_config_docs = scan_all(get_local_client(), "grq_1.0_dist_s1-fwd-state-config")
    else:
        state_config_docs = scan_all(get_local_client(), "grq_1.0_dist_s1-state-config")

    # transform
    state_configs = (d["metadata"] for d in state_config_docs)

    # create dataframe
    df = pd.json_normalize(list(state_configs))
    df.index = pd.RangeIndex(start=1, stop=len(df) + 1)

    # export
    if args.mode == "forward":
        df.to_csv(PurePath("./dist_s1_fwd_state_config_raw.csv"), index=True)
    else:
        df.to_csv(PurePath("./dist_s1_state_config_raw.csv"), index=True)

def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["forward", "historical"], default="forward")
    return parser

if __name__ == "__main__":
    main()
