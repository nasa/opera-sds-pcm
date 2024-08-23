import io
import json
from concurrent.futures import Future
from pathlib import Path
from typing import Optional

import dateutil.parser


def result_or_err(future: Future):
    try:
        result = future.result()
    except Exception as err:
        result = err
    return result


def to_dates(*, dates=list[str], dates_file=Optional[io.TextIOWrapper]):
    if dates:
        dates = [dateutil.parser.isoparse(date).date().isoformat() for date in dates]
    elif dates_file:
        with dates_file:
            try:
                dates = json.load(dates_file)
            except Exception:
                dates_file.seek(0)
                dates = [line.strip() for line in dates_file.readlines() if line.strip()]
        dates = [dateutil.parser.isoparse(date).date().isoformat() for date in dates]
    else:
        raise AssertionError()
    return dates


def with_inserted_suffix(path: Path, suffix):
    """
    Add a suffix to the path (before the last suffix).
    E.g. add_suffix("example.gz", ".tar") => "example.tar.gz"

    :param path: the path to modify.
    :param suffix: the desired suffix to insert.
    """
    return path.with_name(path.name.removesuffix("".join(path.suffixes)) + "".join(path.suffixes[:-1]) + suffix + path.suffixes[-1])


def with_replaced_suffix(path: Path, suffix):
    """
    Add a suffix to the path (before the last suffix), removing any leading suffixes.
    E.g. add_suffix("example.gz", ".tar") => "example.tar.gz"

    :param path: the path to modify.
    :param suffix: the desired suffix to insert.
    """
    return path.with_name(path.name.removesuffix("".join(path.suffixes)) + suffix + path.suffixes[-1])
