import os
import json

from commons.logger import logger

from opera_chimera.constants.opera_chimera_const import OperaChimeraConstants as nc_const


def simulate_output(metadata, base_name, output_dir, extensions):
    logger.info("Simulating PGE output generation....")
    # Create the expected outputs
    for extension in extensions:
        if extension.endswith("met"):
            # Create the met.json
            met_file = os.path.join(output_dir, "{}.{}".format(base_name, extension))
            with open(met_file, "w") as outfile:
                json.dump(metadata, outfile, indent=2)
        else:
            file_name = "{}.{}".format(base_name, extension)
            output_file = os.path.join(output_dir, file_name)
            logger.info("Simulating output {}".format(output_file))
            with open(output_file, "wb") as f:
                f.write(os.urandom(1024))


def simulate_run_pge(runconfig, output_dir, pge_config, context):
    # simulate output
    output_types = pge_config.get(nc_const.OUTPUT_TYPES)
    for type in output_types.keys():
        # Need to formulate a base name for the output files
        base_names = runconfig.get(nc_const.BASE_NAME, {})
        if len(base_names) == 0:
            raise RuntimeError("Missing {} field in the run_config parameter".format(nc_const.BASE_NAME))
        else:
            base_name = base_names.get(type, None)
            if not base_name:
                raise RuntimeError("{} is not defined in the {} area of the run_config parameter".format(
                    type, nc_const.BASE_NAME))
            else:
                if not isinstance(base_name, list):
                    base_name = [base_name]

        mock_metadata = runconfig.get(nc_const.MOCK_METADATA, {})
        if len(mock_metadata) == 0:
            raise RuntimeError("Missing {} field in the run_config parameter".format(nc_const.MOCK_METADATA))
        else:
            metadata = mock_metadata.get(type, None)
            if not metadata:
                raise RuntimeError("{} is not defined in the {} area of the run_config parameter".format(
                    type, nc_const.MOCK_METADATA))
            else:
                if not isinstance(metadata, list):
                    metadata = [metadata]

        if len(base_name) != len(metadata):
            raise RuntimeError("Length of base_name is not equal to length of metadata:"
                               "\nbase_name={}\n\nmetadata={}".format(base_name, metadata))

        for i in range(len(base_name)):
            simulate_output(metadata[i], base_name[i], output_dir, output_types[type])
