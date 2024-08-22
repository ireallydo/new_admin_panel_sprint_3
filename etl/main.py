from services import ExtractService, TransformService, LoadService
from settings.constants import MOVIES_IDX
from settings.configs import Config
from es.es_connector import init_index


def main():

    init_index(MOVIES_IDX)
    config = Config()

    extractor = ExtractService()
    transformer = TransformService()
    loader = LoadService(MOVIES_IDX)

    #  while True:

    extract_config = config.create_extract_config()
    persons_data_modified = extractor.extract_modified_data(extract_config)
    persons_ids = {str(person[0]) for person in persons_data_modified }

    enrich_config = config.create_enrich_config(persons_ids)
    movies_of_persons = extractor.enrich_modified_data(enrich_config)
    movies_ids = {str(movie[0]) for movie in movies_of_persons}

    merge_config = config.create_merge_config(movies_ids)
    pg_movies_data = extractor.merge_modified_data(merge_config)


    transformed_data = transformer.pg_to_es_transform_data(
        movies_ids=movies_ids,
        movies_data=pg_movies_data
    )


    resp = loader.load_data_to_es(transformed_data)
    # pp(resp, indent=2)


if __name__ == '__main__':
    main()
