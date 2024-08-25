import logging
from typing import Any, Dict, Union

from redis import Redis

from . import BaseStorage, redis_client


logger = logging.getLogger()


class RedisStorage(BaseStorage):
    """Реализация хранилища, использующего Redis."""

    def __init__(self):
        self._redis: Redis = redis_client.get_connection()

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        logger.info("Save state to Redis storage: %s", state)
        self._redis.mset(state)

    def retrieve_state(self, key: Union[str, Any]) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        logger.info("Get state from Redis storage by key: %s", key)
        state = self._redis.get(key)
        logger.info("Got state from Redis storage by key %s: %s", key, state)
        return state
