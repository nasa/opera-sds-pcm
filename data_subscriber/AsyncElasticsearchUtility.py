import elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError, ElasticsearchException


class AsyncElasticsearchUtility:
    def __init__(self, es_url, logger=None, **kwargs):
        self.es = elasticsearch.AsyncElasticsearch(hosts=[es_url], **kwargs)
        self.es_url = es_url
        self.logger = logger

    async def index_document(self, **kwargs):
        """
        indexing (adding) document to Elasticsearch
        https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.index
            index – (required) The name of the index
            body – The document
            id – (optional) Document ID, will use ES generated id if not specified
            refresh – If true then refresh the affected shards to make this operation visible to search
            ignore - will not raise error if status code is specified (ex. 404, [400, 404])
        """
        try:
            result = await self.es.index(**kwargs)
            return result
        except RequestError as e:
            if self.logger:
                self.logger.exception(e.info)
            else:
                print(e.info)
            raise e
        except (ElasticsearchException, Exception) as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e

    async def get_by_id(self, **kwargs):
        """
        retrieving document from Elasticsearch based on _id
        https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.get
            index (required) – A comma-separated list of index names
            allow_no_indices – Ignore if a wildcard expression resolves to no concrete indices (default: false)
            expand_wildcards – Whether wildcard expressions should get expanded to open or closed indices
                (default: open) Valid choices: open, closed, hidden, none, all Default: open
            ignore - will not raise error if status code is specified (ex. 404, [400, 404])
        """
        try:
            data = await self.es.get(**kwargs)
            return data
        except NotFoundError as e:
            if self.logger:
                self.logger.error(e)
            else:
                print(e)
            raise e
        except (ElasticsearchException, Exception) as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e

    async def query(self, **kwargs):
        """
        returns all records returned from a query, through the scroll API

        https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
            body – The search definition using the Query DSL
            index – (required) A comma-separated list of index names to search (or aliases)
            _source – True or false to return the _source field or not, or a list of fields to return
            _source_excludes – A list of fields to exclude from the returned _source field
            _source_includes – A list of fields to extract and return from the _source field
            q – Query in the Lucene query string syntax
            scroll – Specify how long a consistent view of the index should be maintained for scrolled search
            size – Number of hits to return (default: 10)
            sort – A comma-separated list of <field>:<direction> pairs

        https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.clear_scroll
            body – A comma-separated list of scroll IDs to clear if none was specified via the scroll_id parameter
            scroll_id – A comma-separated list of scroll IDs to clear
        """
        if 'scroll' not in kwargs:
            kwargs['scroll'] = '2m'
        scroll = kwargs['scroll']  # re-use in each subsequent scroll

        if 'size' not in kwargs:
            kwargs['size'] = 100

        documents = []
        scroll_ids = set()  # unique set of scroll_ids to clear

        try:
            if self.logger:
                self.logger.info('query **kwargs: {}'.format(dict(**kwargs)))
            page = await self.es.search(**kwargs)
            sid = page['_scroll_id']
            scroll_ids.add(sid)
            documents.extend(page['hits']['hits'])
            page_size = page['hits']['total']['value']
        except RequestError as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e
        except (ElasticsearchException, Exception) as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e

        if page_size <= len(documents):  # avoid scrolling if we get all data in initial query
            for scroll_id in scroll_ids:
                await self.es.clear_scroll(scroll_id=scroll_id)
            return documents

        while page_size > 0:
            page = await self.es.scroll(scroll_id=sid, scroll=scroll)
            scroll_document = page['hits']['hits']
            sid = page['_scroll_id']
            scroll_ids.add(sid)

            page_size = len(scroll_document)  # Get the number of results that we returned in the last scroll
            documents.extend(scroll_document)

        # clearing the _scroll_id, Elasticsearch can only keep a finite number of concurrent scroll's (default 500)
        for scroll_id in scroll_ids:
            await self.es.clear_scroll(scroll_id=scroll_id)

        return documents

    async def search(self, **kwargs):
        """
        similar to query method but does not scroll, used if user doesnt want to scroll
        https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
            index – (required) A comma-separated list of index names to search (or aliases)
            body – The search definition using the Query DSL
            _source – True or false to return the _source field or not, or a list of fields to return
            q – Query in the Lucene query string syntax
            scroll – Specify how long a consistent view of the index should be maintained for scrolled search
            size – Number of hits to return (default: 10)
            sort – A comma-separated list of <field>:<direction> pairs
        """
        try:
            if self.logger:
                self.logger.info('search **kwargs: {}'.format(dict(**kwargs)))
            result = await self.es.search(**kwargs)
            return result
        except RequestError as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e
        except (ElasticsearchException, Exception) as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e

    async def get_count(self, **kwargs):
        """
        returning the count for a given query (warning: ES7 returns max of 10000)
        # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.count
            body – A query to restrict the results specified with the Query DSL (optional)
            index – (required) A comma-separated list of indices to restrict the results
            q – Query in the Lucene query string syntax
            ignore - will not raise error if status code is specified (ex. 404, [400, 404])
        """
        try:
            result = await self.es.count(**kwargs)
            return result['count']
        except (ElasticsearchException, Exception) as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e

    async def delete_by_id(self, **kwargs):
        """
        Removes a document from the index
        https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-delete.html
            index – (required) The name of the index
            id – The document ID
            refresh – If true then refresh the affected shards to make this operation visible to search
            ignore - will not raise error if status code is specified (ex. 404, [400, 404])
        """
        try:
            if self.logger:
                self.logger.info('query **kwargs: {}'.format(dict(**kwargs)))
            result = await self.es.delete(**kwargs)
            return result
        except NotFoundError as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e
        except (ElasticsearchException, Exception) as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e

    async def update_document(self, **kwargs):
        """
        updates Elasticsearch document using the update API
        https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.update
            index – (required) The name of the index
            id – Document ID
            body – The request definition requires either script or partial doc:
                ex. {
                    "doc_as_upsert": true,
                    "doc": <ES document>
                }
            _source – True or false to return the _source field or not, or a list of fields to return
            refresh – If true then refresh the affected shards to make this operation visible to search
            ignore - will not raise error if status code is specified (ex. 404, [400, 404])
        """
        try:
            if self.logger:
                self.logger.info('update_document **kwargs'.format(dict(**kwargs)))
            result = await self.es.update(**kwargs)
            return result
        except RequestError as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e
        except (ElasticsearchException, Exception) as e:
            if self.logger:
                self.logger.exception(e)
            else:
                print(e)
            raise e
