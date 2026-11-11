from opensearchpy import OpenSearch
from opera_commons.logger import logger


def pit_scan(client: OpenSearch, index, query, keep_alive=None, size=None):
    pit_id = client.create_pit(
        index=index,
        params={"keep_alive": keep_alive or '10m'},
    )

    logger.info(f'Opened point-in-time for {index}: {pit_id}')

    if size is not None:
        if 'size' in query:
            logger.warning('Replacing size in provided query with size in kwarg')
        query['size'] = size
    elif 'size' not in query:
        query['size'] = 2_500

    if 'pit' in query:
        logger.warning('pit field already in query')

    query['pit'] = {
        'id': pit_id,
        'keep_alive': keep_alive or '10m',
    }

    try:
        while True:
            response = client.search(body=query)
            hits = response['hits']['hits']

            if not hits:
                logger.info('Reached end of search, breaking...')
                break

            for hit in hits:
                yield hit

            query['search_after'] = hits[-1]['sort']
    finally:
        client.delete_pit(body={'pit_id': pit_id})
