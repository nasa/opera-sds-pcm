import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def run_grib_to_netcdf(src: Path, target: Path):
    """Convert a grib file to netcdf. If the file is already a netcdf file, ignore."""
    logger.info(f"{src=!s}, {target=!s}")
    if src.suffix == ".nc":
        logger.info(f"src({src=!s}) is already a netcdf file. Nothing to do.")
        return src

    import subprocess
    subprocess.run(
        [
            "grib_to_netcdf",
            "-D", "NC_FLOAT",
            "-o", str(target.expanduser().resolve()),
            str(src.expanduser())
        ],
        shell=False,
        check=False,
    )

    logger.info(f"Finished running grib_to_netcdf. Check output at {target=!s}")
    return target
