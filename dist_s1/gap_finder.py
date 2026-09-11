import logging
from collections import defaultdict
from functools import cache
from operator import indexOf
from typing import TypedDict

import pandas as pd

logger = logging.getLogger(__name__)

class StateConfigTD(TypedDict):
    tile_id: str
    agn: int
    aci: int


lookup = {}

def init_lookup():
    global lookup
    d = process_dist_burst_db_tile_to_agn()
    d = dict(d)
    for k in d:
        d[k] = sorted(d[k])
    for k in d:
        d[k] = {"agns": sorted(d[k])}

    lookup = d


def main():
    init_lookup()


def is_pair_disjoint(pair: tuple[StateConfigTD, StateConfigTD], lookup):
    a, b = pair
    tile_id_a = a["tile_id"]
    agn_a = a["agn"]
    agn_b = b["agn"]
    aci_a = a["aci"]
    aci_b = b["aci"]

    agns = lookup[tile_id_a]["agns"]

    if agns_too_far_apart := indexOf(agns, agn_b) - indexOf(agns, agn_a) > 1:
        if adjacent_end_to_beginning := agn_a == agns[-1] and agn_b == agns[0]:
            return False
        else:
            return True
    elif agns_same := indexOf(agns, agn_b) - indexOf(agns, agn_a) == 0:  # for len(agns)==1 / same AGN
        if adjacent_cycles := abs(aci_b - aci_a) == 1:
            if adjacent_end_to_beginning := agn_a == agns[-1] and agn_b == agns[0]:
                return False
            return True
        return True  # non-adjacent cycles
    else:  # adjacent AGN. check ACI.
        if same_cycle := aci_b == aci_a:
            return False
        else:
            if adjacent_cycles := abs(aci_b - aci_a) == 1:
                return False
            # TODO chrisjrd: may need to add special case for specific tile_id and ACNs that cut across ACI boundary
            # if a.tile_id == ... and a.agn == ... and b.tile_id == ... and b.agn = ...: pass
            return True


@cache
def process_dist_burst_db_tile_to_agn():
    df = cached_read_dist_burst_db("mgrs_burst_lookup_table_2025-11-19.parquet")
    d = defaultdict(set)
    for row in df.itertuples():
        #print(row['mgrs_tile_id'], row['acq_group_id_within_mgrs_tile'])
        d[row.mgrs_tile_id].add(row.acq_group_id_within_mgrs_tile)
    return d

@cache
def cached_read_dist_burst_db(file):
    return pd.read_parquet(file)
