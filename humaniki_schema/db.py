from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

engine = create_engine("mysql://{user}:{password}@{host}/{database}?charset=utf8".format(
    host=os.environ['HUMANIKI_MYSQL_HOST'],
    user=os.environ['HUMANIKI_MYSQL_USER'],
    password=os.environ['HUMANIKI_MYSQL_PASS'],
    database=os.environ['HUMANIKI_MYSQL_DB']),
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True
)

# Base.metadata.bind = db_engine
session_factory = sessionmaker(bind=engine)
