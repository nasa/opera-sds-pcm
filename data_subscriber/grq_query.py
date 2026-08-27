from datetime import datetime


async def async_query_grq(args, index_pattern, settings, timerange, now: datetime, verbose=True) -> list:
    if index_pattern is None:
        raise ValueError("index_pattern cannot be None")



