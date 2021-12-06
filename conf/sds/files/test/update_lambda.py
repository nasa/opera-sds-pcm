import boto3
import json
import argparse

from pcm_commons.logger import logger


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("function_name", help="Lambda function name to update")
    parser.add_argument("environment", help="JSON of the environment variables to update")

    args = parser.parse_args()

    function_name = args.function_name

    environment_update = json.loads(args.environment)

    client = boto3.client("lambda")
    response = client.get_function(FunctionName=function_name)
    environment = response.get("Configuration", {}).get("Environment", {})
    environment["Variables"].update(environment_update)
    logger.info("Updating Lambda, {}, with the following environment: {}".format(function_name,
                                                                                 json.dumps(environment)))
    response = client.update_function_configuration(FunctionName=function_name, Environment=environment)
    logger.debug("Lambda response for {}: {}".format(function_name, json.dumps(response)))
