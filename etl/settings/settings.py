from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    DB_HOST: str = Field(..., env="DB_HOST")
    DB_PORT: str = Field(..., env="DB_PORT")
    DB_NAME: str = Field(..., env="DB_NAME")
    DB_USER: str = Field(..., env="DB_USER")
    DB_PASSWORD: str = Field(..., env="DB_PASSWORD")

    ELASTIC_HOST: str = Field(..., env="ELASTIC_HOST")
    ELASTIC_PORT: str = Field(..., env="ELASTIC_PORT")

    REDIS_HOST: str = Field(..., env="REDIS_HOST")
    REDIS_PORT: str = Field(..., env="REDIS_PORT")
    REDIS_USER: str = Field(..., env="REDIS_USER")
    REDIS_USER_PASSWORD: str = Field(..., env="REDIS_USER_PASSWORD")
    REDIS_DB: int = Field(..., env="REDIS_DB")

    LOG_FILEPATH: str = Field("logs/app_log.log", env="LOG_FILEPATH")
    LOG_ROTATION: int = Field(1, env="LOG_ROTATION")
    LOG_RETENTION: int = Field(30, env="LOG_RETENTION")

    class Config:
        env_file = Path(__file__).parents[1].joinpath("environment/.env")
        env_file_encoding = "utf-8"
