from postgres_db.dto import ExtractSchema, EnrichSchema, MergeSchema
from postgres_db.dao import ExtractorDAO, EnricherDAO, MergerDAO


class ExtractService:

    def __init__(self):
        self._extractor_dao = ExtractorDAO()
        self._enricher_dao = EnricherDAO()
        self._merger_dao = MergerDAO()


    def extract_modified_data(self, extract_config: ExtractSchema):
        response = self._extractor_dao.get_modified_data(extract_config)
        return response

    def enrich_modified_data(self, enrich_config: EnrichSchema):
        response = self._enricher_dao.get_related_data(enrich_config)
        return response


    def merge_modified_data(self, merge_config: MergeSchema):
        response = self._merger_dao.get_merged_data(merge_config)
        return response

    def get_modified_bulk(self):
        pass