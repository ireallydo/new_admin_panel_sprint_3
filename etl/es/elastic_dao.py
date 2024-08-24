import logging
from typing import List

from elasticsearch import (BadRequestError, Elasticsearch, NotFoundError,
                           RequestError)
from elasticsearch.helpers import bulk, parallel_bulk

from fault_tolerance_sys.backoff import ESBackoff

from .es_connector import es

logger = logging.getLogger()


class ElasticsearchDAO:

    def __init__(self, index: str):
        self._con: Elasticsearch = es
        self._idx = index

    def bulk_create_generator(self, movies_data: List[dict]):
        logger.info(f"Creating generator of data for bulk create in index {self._idx}")
        for movie in movies_data:
            yield {
                "_index": self._idx,
                "_op_type": 'create',
                '_id': movie['id'],
                "_source": movie
            }

    def bulk_update_generator(self, movies_data: List[dict]):
        logger.info(f"Creating generator of data for bulk update in index {self._idx}")
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
        logger.info("Make bulk request for create documents in Elasticsearch")
        response = list()
        for success, info in parallel_bulk(self._con,
                                           self.bulk_create_generator(movies_data)):
            if not success:
                logger.debug('A document failed: ', info)
                return success

            logger.debug('Document was created: ', info)
            response.append(info)

        return response

    @ESBackoff.connection_backoff
    @ESBackoff.server_backoff
    def parallel_update_bulk(self, movies_data: List[dict]):
        logger.info("Make bulk request for update documents in Elasticsearch")
        response = list()
        for success, info in parallel_bulk(self._con,
                                           self.bulk_update_generator(movies_data)):
            if not success:
                logger.debug('A document failed: ', info)
                return success

            logger.debug('Document was updated: ', info)
            response.append(info)

        return response

    @ESBackoff.connection_backoff
    @ESBackoff.server_backoff
    def exists(self, doc_id: str):
        logger.info(f"Checking if document with id {doc_id} exists in index {self._idx}")
        response = self._con.exists(index=self._idx, id=doc_id)
        logger.info(f"Response about document with id {doc_id} existing in index {self._idx}: {response}")
        return response
