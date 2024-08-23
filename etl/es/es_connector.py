from elasticsearch import Elasticsearch, NotFoundError, ApiError
from fault_tolerance_sys.backoff import ESBackoff
from settings import Settings
import logging
import json


settings = Settings()
logger = logging.getLogger()


es_connection_addr = f"{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}"
es = Elasticsearch(es_connection_addr)
logger.debug(f"Elasticsearch connection address: {es_connection_addr}")


@ESBackoff.connection_backoff
def init_index(idx: str):
    logger.info(f"Initializing index '{idx}' in Elasticsearch")
    try:
        logger.info(f"Check if index '{idx}' exists")
        es.indices.get(index=idx)
        logger.info(f"Index '{idx}' exists and will be used")
    except NotFoundError as e:
        logger.info(f"Index '{idx}' not found in Elasticsearch")
        with open('es/movies_index_config.json', 'r') as config_file:
            config = json.load(config_file)
            logger.info(f"create index '{idx}'")
            index = es.indices.create(
                index=idx,
                mappings=config['mappings'],
                settings=config['settings'],
                pretty=True
            )
            logger.info(f"Index '{idx}' was successfully created")
            logger.debug(f"Index info: {dict(index)}")
