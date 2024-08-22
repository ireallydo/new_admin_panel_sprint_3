from typing import Any
from . import BaseStorage


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        state_data = self.storage.retrieve_state()
        if key in state_data.keys():
            return state_data[key]
        else:
            return None