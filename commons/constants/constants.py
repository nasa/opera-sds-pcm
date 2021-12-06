"""
Add field names from PGE config files, names of functions,
match patterns or key names that can be referenced throughout code base

Note: To add new keys, please follow an alphabetical order

e.g.
LOCALIZE_KEY = "localize" # name of key found in input preprocessor output
GET_PGE_NAME = "pge_name" # name of key found in PGE config file
GET_ICE_SCLK = "getIceSclk" # name of function
"""

ORBIT_EPHEMERIS_PRECOND = "get_orbit_ephemeris"

PRODUCT_MOST_RECENT_PRECOND = "get_product_most_recent_version"

PRODUCT_COUNTER_PRECOND = "get_product_counter"

PRODUCT_MET_PRECOND = "get_product_metadata"

PRODUCT_METADATA_KEY = "product_metadata"
DATASET_TYPE_KEY = "dataset_type"

PADDING = "padding"
FILTERS = "filters"

LATENCY = "LATENCY"
RRST_EVALUATOR = "RRST_EVALUATOR"
TRACK_FRAME_EVALUATOR = "TRACK_FRAME_EVALUATOR"
DATATAKE_EVALUATOR = "DATATAKE_EVALUATOR"
NOMINAL_LATENCY = "NOMINAL_LATENCY"
URGENT_RESPONSE_LATENCY = "URGENT_RESPONSE_LATENCY"
NETWORK_PAIR_EVALUATOR = "NETWORK_PAIR_EVALUATOR"
