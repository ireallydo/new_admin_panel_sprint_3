from redis import Redis
from . import BaseStorage
from . import redis_client
from typing import Dict, Any, Union


class RedisStorage(BaseStorage):
    """Реализация хранилища, использующего Redis."""

    def __init__(self):
        self._redis: Redis = redis_client.get_connection()

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        self._redis.mset(state)

    def retrieve_state(self, key: Union[str, Any]) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        return self._redis.get(key)
