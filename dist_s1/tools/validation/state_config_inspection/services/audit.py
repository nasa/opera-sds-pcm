from collections import defaultdict, namedtuple
from typing import Iterable

BatchIdSortable = namedtuple(
    "BatchIdSortable", ["satellite", "tile", "aci", "agn"] )

def state_configs_group_by_tile(state_configs: Iterable):
    tile_to_state_configs = defaultdict(list[dict])
    for state_config in state_configs:
        tile_to_state_configs[state_config["tile_id"]].append(state_config)
    return tile_to_state_configs


def state_config_comparator(state_config):
    batch_id_split = state_config["batch_id"].split("_")
    return BatchIdSortable(satellite=batch_id_split[2], tile=batch_id_split[0], aci=int(batch_id_split[3]), agn=int(batch_id_split[1]))
