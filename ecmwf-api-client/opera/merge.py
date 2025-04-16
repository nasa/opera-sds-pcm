import logging
from pathlib import Path

import xarray

logger = logging.getLogger(__file__)


def merge_netcdf_pairs(a2_a3_pair_paths: list[tuple[Path, Path]],
                       target: Path):
    combined = opera_merge_netcdf_pairs_with_target(a2_a3_pair_paths, target)

    #combined = opera_merge_netcdf_pairs(a2_a3_pair_paths)
    #combined.to_netcdf(target.resolve(), format="NETCDF4")
    #combined.close()
    return target


def opera_merge_netcdf_pairs_with_target(a2_a3_pairs: list[tuple[Path, Path]], 
                             target: Path):
    """
    Concatenate A2 and A3
    :param a2: A2
    :param a3: A3
    :param combined_file_name: The combined file name
    :return:
    """
    #objs = []
    for a2_a3_pair in a2_a3_pairs:
        a2_path = a2_a3_pair[0]
        a3_path = a2_a3_pair[1]

        ds_xr_a2 = xarray.open_dataset(str(a2_path.resolve()), chunks="auto")
        ds_xr_a3 = xarray.open_dataset(str(a3_path.resolve()), chunks="auto")

        ds_a2_longitude = ds_xr_a2.longitude.values
        ds_a2_longitude[ds_a2_longitude < 0] = ds_a2_longitude[ds_a2_longitude < 0] + 360
        ds_xr_a2["longitude"] = ds_a2_longitude

        ds_a3_longitude = ds_xr_a3.longitude.values
        ds_a3_longitude[ds_a3_longitude < 0] = ds_a3_longitude[ds_a3_longitude < 0] + 360
        ds_xr_a3["longitude"] = ds_a3_longitude

        # modify this with NISAR version
        # https://github.jpl.nasa.gov/IEMS-SDS/nisar-pcm/blob/develop/util/ecmwf_util.py#L301-L310
        ds_merge = xarray.concat(
            [
                ds_xr_a2.sortby("level").sortby("longitude"),
                ds_xr_a3.sortby("level").sortby("longitude").isel(level=slice(1, None)),
            ],
            dim="level",
        )
        
        ds_merge.to_netcdf(target.resolve(), format="NETCDF4")



def opera_merge_netcdf_pairs(a2_a3_pairs: list[tuple[Path, Path]]):
    """
    Concatenate A2 and A3
    :param a2: A2
    :param a3: A3
    :param combined_file_name: The combined file name
    :return:
    """
    objs = []
    for a2_a3_pair in a2_a3_pairs:
        a2_path = a2_a3_pair[0]
        a3_path = a2_a3_pair[1]

        ds_xr_a2 = xarray.open_dataset(str(a2_path.resolve()), chunks="auto")
        ds_xr_a3 = xarray.open_dataset(str(a3_path.resolve()), chunks="auto")

        ds_a2_longitude = ds_xr_a2.longitude.values
        ds_a2_longitude[ds_a2_longitude < 0] = ds_a2_longitude[ds_a2_longitude < 0] + 360
        ds_xr_a2["longitude"] = ds_a2_longitude

        ds_a3_longitude = ds_xr_a3.longitude.values
        ds_a3_longitude[ds_a3_longitude < 0] = ds_a3_longitude[ds_a3_longitude < 0] + 360
        ds_xr_a3["longitude"] = ds_a3_longitude

        objs.extend([
                xarray.merge([
                    ds_xr_a2.sortby("level").sortby("longitude"),
                    ds_xr_a3.sortby("level").sortby("longitude").isel(level=slice(1, None)),
                ])
        ])

    return xarray.concat(objs, dim="time")


