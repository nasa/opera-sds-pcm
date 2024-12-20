import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def run_grib_to_netcdf(grib_file: Path, nc_file: Path):
    """Convert a grib file to netcdf. If the file is already a netcdf file, ignore."""
    logger.info(f"{grib_file=!s}, {nc_file=!s}")
    if grib_file.suffix == ".nc":
        logger.info(f"src({grib_file=!s}) is already a netcdf file. Nothing to do.")
        return grib_file

    import subprocess
    subprocess.run(
        [
            "grib_to_netcdf",
            "-D", "NC_FLOAT",
            "-o", str(nc_file.resolve()),
            str(grib_file)
        ],
        shell=False,
        check=False,
    )

    logger.info(f"Finished running grib_to_netcdf. Check output at {nc_file=!s}")
    return nc_file
