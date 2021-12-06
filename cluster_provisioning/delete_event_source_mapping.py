import boto3
import logging
import sys

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("")

if __name__ == "__main__":
    prefix = sys.argv[1]
    client = boto3.client('lambda')
    response = client.list_event_source_mappings()
    LOGGER.info("list_event_source_mappings Response: {}".format(response))
    for event_source_map in response['EventSourceMappings']:
        if prefix in event_source_map['EventSourceArn']:
            uuid = event_source_map['UUID']
            response = client.delete_event_source_mapping(UUID=uuid)
            LOGGER.info("delete_event_source_mapping Response: {}".format(
                response))
