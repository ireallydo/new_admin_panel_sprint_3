import logging
from typing import Any

from . import BaseStorage


logger = logging.getLogger()


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        logger.info(f"Set state for key/value pair: key : value")
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        logger.info(f"Get state by key: {key}")
        state = self.storage.retrieve_state(key)
        logger.info(f"Received state by key {key}: {state}")
        return state.decode("utf-8")
