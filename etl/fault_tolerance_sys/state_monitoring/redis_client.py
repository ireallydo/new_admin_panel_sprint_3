import logging

import redis

from settings import Settings


settings = Settings()
logger = logging.getLogger()


class RedisClient:

    def get_connection(self):
        logger.info("Create Redis connection")
        connection = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            username=settings.REDIS_USER,
            password=settings.REDIS_USER_PASSWORD
        )
        try:
            info = connection.info()
            logger.debug("Redis version: %s", info['redis_version'])
            response = connection.ping()
            if response:
                logger.info("Successfully connected to Redis")
            else:
                logger.critical("Failed to connect to Redis")
        except redis.exceptions.RedisError as e:
            logger.critical(e)
        return connection


redis_client = RedisClient()
