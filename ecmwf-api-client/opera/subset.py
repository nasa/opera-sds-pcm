import sys
import time
from pathlib import Path

import netCDF4


def subset_netcdf_file(in_: Path, out_: Path, bbox: tuple[int, int, int, int] = None):
    """subsetting a netcdf file and storing in netcdf format

    Parameters
    ----------
    in_
    The path to the large file

    out_
    output file in netcdf format

    bbox
    bounding box in (west, south, east, north)
    """
    if not bbox:
        bbox = (-180, -90, 180, 90)

    west, south, east, north = bbox
    if west < 0 or east < 0:
        west += 180
        east += 180

    nc = netCDF4.Dataset(str(in_.expanduser().resolve()), 'r')
    lons = nc['longitude'][:]
    lats = nc['latitude'][:]

    lon_idx = (lons >= west) & (lons <= east)
    lat_idx = (lats >= south) & (lats <= north)

    # Extract subset for latitude and longitude, time and level
    subset_lons = lons[lon_idx]
    subset_lats = lats[lat_idx]
    subset_time = nc['time'][:]
    subset_level = nc['level'][:]

    # Create a new NetCDF file to store the subset
    subset_nc = netCDF4.Dataset(str(out_.expanduser().resolve()), 'w')
    if hasattr(nc, "Conventions"):  # unconfirmed if this would be empty when coming from formal sources
        setattr(subset_nc, "Conventions", getattr(nc, "Conventions"))
    # if hasattr(nc, "history"):  # unconfirmed if this would be empty when coming from formal sources
        # setattr(subset_nc, "history", getattr(nc, "history"))
    subset_nc.history = f"{time.ctime(time.time())} by OPERA SDS PCM: {' '.join(sys.argv)}"

    # Create dimensions for latitude and longitude
    subset_nc.createDimension('longitude', len(subset_lons))
    subset_nc.createDimension('latitude', len(subset_lats))
    subset_nc.createDimension('time', len(subset_time))
    subset_nc.createDimension('level', len(subset_level))

    # Create latitude and longitude variables
    subset_nc.createVariable('longitude', 'f4', ('longitude',))
    subset_nc.createVariable('latitude', 'f4', ('latitude',))
    subset_nc.createVariable('time', 'i4', ('time',))
    subset_nc.createVariable('level', 'f4', ('level',))

    # Write latitude and longitude data
    subset_nc.variables['longitude'][:] = subset_lons
    subset_nc.variables['latitude'][:] = subset_lats
    subset_nc.variables['time'][:] = subset_time
    subset_nc.variables['level'][:] = subset_level

    # Iterate through other variables and write subset data
    for var_name, var in nc.variables.items():
        if var_name not in ['longitude', 'latitude', 'level', 'time']:  # Skip latitude,longitude, time and level variables
            subset_data = var[:, :, lat_idx, lon_idx]  # Subset along latitude and longitude dimensions
            subset_nc.createVariable(var_name, var.dtype, ('time', 'level', 'latitude', 'longitude'))
            subset_nc.variables[var_name][:] = subset_data

    # copy Variable attributes over
    #  Copies common attributes like "long_name", "units", "standard_name", "calendar"
    #  ignores protected attributes -- in effect, "_FillValue".
    #  Notably, copy "units" (units="hours since 1900-01-01 00:00:0.0") for "time".
    for var_ in ("time", "z", "t", "q", "lnsp", "longitude", "latitude", "level"):
        src_variable = nc.variables[var_]
        target_variable = subset_nc.variables[var_]
        attrs_ = set(filter(lambda a: not a.startswith("_"), set(dir(src_variable)) - set(dir(target_variable))))
        for attr_ in attrs_:
            setattr(target_variable, attr_, getattr(src_variable, attr_))

    # Close both NetCDF files
    nc.close()
    subset_nc.close()

    return out_
