from elasticsearch import Elasticsearch, NotFoundError
from fault_tolerance_sys.es_backoff import es_connection_backoff
import json


es = Elasticsearch("http://127.0.0.1:9200")


@es_connection_backoff
def init_index(idx: str):
    try:
        index = es.indices.get(index=idx)
    except NotFoundError as e:
        print('Index not found!')
        with open('es/movies_index_config.json', 'r') as config_file:
            config = json.load(config_file)
            index = es.indices.create(
                index=idx,
                mappings=config['mappings'],
                settings=config['settings'],
                pretty=True
            )
            print(dict(index))
