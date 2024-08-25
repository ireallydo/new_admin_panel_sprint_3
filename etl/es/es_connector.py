import json
import logging

from elasticsearch import ApiError, Elasticsearch, NotFoundError

from fault_tolerance_sys.backoff import ESBackoff
from settings import Settings

settings = Settings()
logger = logging.getLogger()


es_connection_addr = f"{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}"
es = Elasticsearch(es_connection_addr)
logger.debug("Elasticsearch connection address: %s", es_connection_addr)


@ESBackoff.connection_backoff
def init_index(idx: str):
    logger.info("Initializing index '%s' in Elasticsearch", idx)
    try:
        logger.info("Check if index '%s' exists", idx)
        es.indices.get(index=idx)
        logger.info("Index '%s' exists and will be used", idx)
    except NotFoundError as e:
        logger.info("Index '%s' not found in Elasticsearch", idx)
        with open('es/movies_index_config.json', 'r') as config_file:
            config = json.load(config_file)
            logger.info("Create index '%s'", idx)
            index = es.indices.create(
                index=idx,
                mappings=config['mappings'],
                settings=config['settings'],
                pretty=True
            )
            logger.info("Index '%s' was successfully created", idx)
            logger.debug("Index info: %s", dict(index))
