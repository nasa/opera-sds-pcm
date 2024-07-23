import logging
from pathlib import Path
from typing import Optional, Literal

import cfgrib
import xarray

logger = logging.getLogger(__file__)


def nisar_open_and_concat_and_save_netcdf_pairs(a2_a3_pair_paths: list[tuple[Path, Path]],
                                                target: Path):
    # combined = load_and_concat_grib_pair(a2_path, a3_path)
    combined = nisar_open_and_concat_netcdf_pairs(a2_a3_pair_paths)
    combined.to_netcdf(target.expanduser().resolve(), format="NETCDF4")
    combined.close()
    return target


def nisar_open_and_concat_netcdf_pairs(a2_a3_pairs: list[tuple[Path, Path]]):
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

        ds_xr_a2 = xarray.open_dataset(str(a2_path.expanduser().resolve()), chunks="auto")
        ds_xr_a3 = xarray.open_dataset(str(a3_path.expanduser().resolve()), chunks="auto")

        ds_a2_longitude = ds_xr_a2.longitude.values
        ds_a2_longitude[ds_a2_longitude < 0] = ds_a2_longitude[ds_a2_longitude < 0] + 360
        ds_xr_a2["longitude"] = ds_a2_longitude

        ds_a3_longitude = ds_xr_a3.longitude.values
        ds_a3_longitude[ds_a3_longitude < 0] = ds_a3_longitude[ds_a3_longitude < 0] + 360
        ds_xr_a3["longitude"] = ds_a3_longitude

        objs.extend([
            ds_xr_a3.sortby("level").sortby("longitude"),
            ds_xr_a3.sortby("level").sortby("longitude").isel(level=slice(1, None)),
        ])

    return xarray.concat(objs, dim="level")


def load_and_concat_grib_pair(a2_path: Path, a3_path: Path):
    logger.info("load_dataset + concat")

    # ds_xr_a2_surf = xarray.load_dataset(filename_or_obj=str(a2_path.expanduser()))
    # ds_xr_a3_surf = xarray.load_dataset(filename_or_obj=str(a3_path.expanduser()))  # surf has "hybrid" dimension
    ds_xr_a2_3d, ds_xr_a2_surf = cfgrib.open_datasets(str(a2_path.expanduser()))
    ds_xr_a3_3d, ds_xr_a3_surf = cfgrib.open_datasets(str(a3_path.expanduser()))  # surf has "hybrid" dimension
    ds_xr_a3_surf = ds_xr_a3_surf.sel(hybrid=slice(69, 137))

    return xarray.concat([ds_xr_a2_surf, ds_xr_a3_surf], dim="time")


def _mfopen_mfcombine_and_save_grib_pair(a2_path: Path, a3_path: Path, target: Path):
    combined = _mfopen_mfcombine_grib_pair(a2_path, a3_path)
    combined.to_netcdf(target.expanduser().resolve(), format="NETCDF4")
    combined.close()
    return target


def _mfopen_mfcombine_grib_pair(a2_path: Path, a3_path: Path):
    logger.info("open_mfdataset")

    return xarray.open_mfdataset([
            a2_path.expanduser().resolve(),
            a3_path.expanduser().resolve()
    ], engine="cfgrib", concat_dim="time", combine='nested')  # memory efficient


def _load_and_merge_and_save_grib_pair(a2_path: Path, a3_path: Path, target: Path):
    combined = load_and_merge_grib_pair(a2_path, a3_path)
    combined.to_netcdf(target.expanduser().resolve(), format="NETCDF4")
    return combined


def _load_and_merge_grib_pair(a2_path: Path, a3_path: Path):
    logger.info("load_dataset + merge")

    # ds_xr_a2_surf = xarray.load_dataset(filename_or_obj=str(a2_path.expanduser()))
    # ds_xr_a3_surf = xarray.load_dataset(filename_or_obj=str(a3_path.expanduser()))  # has "hybrid" dimension
    ds_xr_a2_3d, ds_xr_a2_surf = cfgrib.open_datasets(str(a2_path.expanduser().resolve()))
    ds_xr_a3_3d, ds_xr_a3_surf = cfgrib.open_datasets(str(a3_path.expanduser().resolve()))  # has "hybrid" dimension
    ds_xr_a3_surf = ds_xr_a3_surf.sel(hybrid=slice(69, 137))
    return xarray.merge([ds_xr_a2_surf, ds_xr_a3_surf])
