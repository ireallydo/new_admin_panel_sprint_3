from postgres_db.dto import ExtractSchema, EnrichSchema, MergeSchema
from postgres_db.dao import ExtractorDAO, EnricherDAO, MergerDAO
import logging


logger = logging.getLogger()


class ExtractService:

    def __init__(self):
        self._extractor_dao = ExtractorDAO()
        self._enricher_dao = EnricherDAO()
        self._merger_dao = MergerDAO()


    def extract_modified_data(self, extract_config: ExtractSchema):
        logger.info("Extract modified data from database")
        response = self._extractor_dao.get_modified_data(extract_config)
        return response

    def enrich_modified_data(self, enrich_config: EnrichSchema):
        logger.info("Enrich modified data from database")
        response = self._enricher_dao.get_related_data(enrich_config)
        return response


    def merge_modified_data(self, merge_config: MergeSchema):
        logger.info("Get details of modified data from database and merge data")
        response = self._merger_dao.get_merged_data(merge_config)
        return response
