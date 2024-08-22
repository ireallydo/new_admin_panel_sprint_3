from elasticsearch import Elasticsearch, BadRequestError, NotFoundError, RequestError
from elasticsearch.helpers import parallel_bulk, bulk
from .es_connector import es
from typing import Union, List


class ElasticsearchDAO:

    def __init__(self, index: str):
        self._con: Elasticsearch = es
        self._idx = index

    def bulk_create_generator(self, movies_data: List[dict]):
        for movie in movies_data:
            yield {
                "_index": self._idx,
                "_op_type": 'create',
                '_id': movie['id'],
                "_source": movie
            }

    def bulk_update_generator(self, movies_data: List[dict]):
        for movie in movies_data:
            yield {
                "_index": self._idx,
                "_op_type": 'update',
                '_id': movie['id'],
                "doc": movie
            }

    def parallel_create_bulk(self, movies_data: List[dict]):
        for success, info in parallel_bulk(self._con,
                                           self.bulk_create_generator(movies_data)):
            if not success:
                print('A document failed: ', info)
            else:
                print('Hooray!: ', info)

    def parallel_update_bulk(self, movies_data: List[dict]):
        for success, info in parallel_bulk(self._con,
                                           self.bulk_update_generator(movies_data)):
            if not success:
                print('A document failed: ', info)
            else:
                print('Hooray!: ', info)

    def exists(self, id: str):
        return self._con.exists(index=self._idx, id=id)
