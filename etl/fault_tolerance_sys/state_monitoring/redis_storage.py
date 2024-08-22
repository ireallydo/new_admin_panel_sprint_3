from redis import Redis
from . import BaseStorage
from typing import Dict, Any


class RedisStorage(BaseStorage):
    """Реализация хранилища, использующего Redis."""

    def __init__(self, redis: Redis):
        self._redis = redis

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        self._redis.mset(state)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        return self._redis
