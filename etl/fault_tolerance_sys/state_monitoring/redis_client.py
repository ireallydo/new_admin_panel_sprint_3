import redis
from settings import Settings


settings = Settings()


class Redis:

    def get_connection(self):
        connection = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            username=settings.REDIS_USER,
            password=settings.REDIS_USER_PASSWORD
        )
        try:
            info = connection.info()
            print(info['redis_version'])
            response = connection.ping()
            if response:
                print("Подключение успешно!")
            else:
                print("Не удалось подключиться к Redis.")
        except redis.exceptions.RedisError as e:
            print(f"Ошибка: {e}")
        return connection


redis_client = Redis()
