from services.dto import ExtractSchema, EnrichSchema, MergeSchema
from services import ExtractService, TransformService
from settings.constants import SCHEMA


def main():

    extractor = ExtractService()

    extract_config = ExtractSchema(
        table=f'{SCHEMA}.person',
        modified='2021-06-16 20:14:09.268101',
        limit=100
    )
    persons_data_modified = extractor.extract_modified_data(extract_config)
    # print(persons_data_modified )
    persons_ids = {str(person[0]) for person in persons_data_modified }

    enrich_config = EnrichSchema(
        table=f'{SCHEMA}.film_work',
        join_tb_cols_value={
            f'{SCHEMA}.person_film_work': {
                'film_work_id': f'{SCHEMA}.film_work.id'
            }
        },
        filter_tb_col=(
            f'{SCHEMA}.person_film_work',
            'person_id'
        ),
        filter_values=persons_ids,
        limit=100
    )
    movies_of_persons = extractor.enrich_modified_data(enrich_config)
    # print(movies_of_persons)
    movies_ids = {str(movie[0]) for movie in movies_of_persons}

    merge_config = MergeSchema(
        table=f'{SCHEMA}.film_work',
        select_tb_cols={
            f'{SCHEMA}.film_work': {
                'id',
                'title',
                'description',
                'rating as imdb_rating',
                'type'
            },
            f'{SCHEMA}.person': {
                'id as person_id',
                'full_name as person_name'
            },
            f'{SCHEMA}.person_film_work': {
                'role'
            },
            f'{SCHEMA}.genre': {
                'name'
            }
        },
        join_tb_cols_value={
            f'{SCHEMA}.person_film_work': {
                'film_work_id': f'{SCHEMA}.film_work.id'
            },
            f'{SCHEMA}.person': {
                'id': f'{SCHEMA}.person_film_work.person_id'
            },
            f'{SCHEMA}.genre_film_work': {
                'film_work_id': f'{SCHEMA}.film_work.id'
            },
            f'{SCHEMA}.genre': {
                'id': f'{SCHEMA}.genre_film_work.genre_id'
            }
        },
        filter_tb_col=(
            f'{SCHEMA}.film_work',
            'id'
        ),
        filter_values=movies_ids
    )
    pg_movies_data = extractor.merge_modified_data(merge_config)


    transformer = TransformService()
    transformed_data = transformer.pg_to_es_transform_data(
        movies_ids=movies_ids,
        movies_data=pg_movies_data
    )
    print(transformed_data[0])


if __name__ == '__main__':
    main()
