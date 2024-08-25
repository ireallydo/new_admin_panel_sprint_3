import logging
from typing import List

from pydantic import parse_obj_as

from postgres_db.dao import EnricherDAO, ExtractorDAO, MergerDAO
from postgres_db.dto import (EnrichResponseSchema, EnrichSchema,
                             ExtractResponseSchema, ExtractSchema, MergeSchema)


logger = logging.getLogger()


class ExtractService:

    def __init__(self):
        self._extractor_dao = ExtractorDAO()
        self._enricher_dao = EnricherDAO()
        self._merger_dao = MergerDAO()


    def extract_modified_data(self, extract_config: ExtractSchema):
        logger.info("Extract modified data from database")
        response = self._extractor_dao.get_modified_data(extract_config)
        resp = parse_obj_as(List[ExtractResponseSchema], response)
        return resp

    def enrich_modified_data(self, enrich_config: EnrichSchema):
        logger.info("Enrich modified data from database")
        response = self._enricher_dao.get_related_data(enrich_config)
        resp = parse_obj_as(List[EnrichResponseSchema], response)
        return resp

    def merge_modified_data(self, merge_config: MergeSchema):
        logger.info("Get details of modified data from database and merge data")
        response = self._merger_dao.get_merged_data(merge_config)
        return response
