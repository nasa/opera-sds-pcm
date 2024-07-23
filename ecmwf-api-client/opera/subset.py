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

    # Create dimensions for latitude and longitude
    subset_nc.createDimension('longitude', len(subset_lons))
    subset_nc.createDimension('latitude', len(subset_lats))
    subset_nc.createDimension('time', len(subset_time))
    subset_nc.createDimension('level', len(subset_level))

    # Create latitude and longitude variables
    subset_nc.createVariable('longitude', 'f4', ('longitude',))
    subset_nc.createVariable('latitude', 'f4', ('latitude',))
    subset_nc.createVariable('time', 'f4', ('time',))
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

    # Close both NetCDF files
    nc.close()
    subset_nc.close()

    return out_
