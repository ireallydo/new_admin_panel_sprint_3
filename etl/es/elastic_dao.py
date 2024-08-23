from elasticsearch import Elasticsearch, BadRequestError, NotFoundError, RequestError
from elasticsearch.helpers import parallel_bulk, bulk
from .es_connector import es
from fault_tolerance_sys.backoff import ESBackoff
from typing import List


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

    @ESBackoff.connection_backoff
    @ESBackoff.server_backoff
    def parallel_create_bulk(self, movies_data: List[dict]):
        response = list()
        for success, info in parallel_bulk(self._con,
                                           self.bulk_create_generator(movies_data)):
            if not success:
                print('A document failed: ', info)
                return success
            else:
                print('Hooray!: ', info)
                response.append(info)
        return response

    @ESBackoff.connection_backoff
    @ESBackoff.server_backoff
    def parallel_update_bulk(self, movies_data: List[dict]):
        response = list()
        for success, info in parallel_bulk(self._con,
                                           self.bulk_update_generator(movies_data)):
            if not success:
                print('A document failed: ', info)
                return success
            else:
                print('Hooray!: ', info)
                response.append(info)
        return response

    @ESBackoff.connection_backoff
    @ESBackoff.server_backoff
    def exists(self, id: str):
        return self._con.exists(index=self._idx, id=id)
