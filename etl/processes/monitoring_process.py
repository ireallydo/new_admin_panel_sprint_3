import logging

from services import ExtractService
from settings.configs import Config
from settings.dto import ExtractConfigDTO


logger = logging.getLogger()


class MonitoringProcess:

    def __init__(self):
        self._extractor = ExtractService()
        self._config = Config()

    def start_process(self, extract_settings: ExtractConfigDTO):
        extract_config = self._config.create_extract_config(extract_settings)
        data_modified = self._extractor.extract_modified_data(extract_config)
        return data_modified


monitoring_process = MonitoringProcess()
