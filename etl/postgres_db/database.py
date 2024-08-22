from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from settings import Settings


settings = Settings()

db_con_str = (f"postgresql+psycopg2://"
              f"{settings.DB_USER}:{settings.DB_PASSWORD}"
              f"@{settings.DB_HOST}/{settings.DB_NAME}")
# logger.debug(f"DB Connection string: {db_con_str }")
engine = create_engine(db_con_str, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, autocommit=False, autoflush=False, class_=Session)
