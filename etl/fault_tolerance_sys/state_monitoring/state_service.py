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
        logger.info("Set state for key/value pair: %s / %s", key , value)
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        logger.info("Get state by key: %s", key)
        state = self.storage.retrieve_state(key)
        logger.info("Received state by key %s: %s", key, state)
        if state is not None:
            return state.decode("utf-8")
