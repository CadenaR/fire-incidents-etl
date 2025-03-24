import os
from dotenv import load_dotenv
import logging

from shared.ddl import ddl
from sqlalchemy import create_engine, text, Engine

load_dotenv()

STG_PATH = os.getenv("STG_PATH")


def get_postgres_engine() -> Engine:
    logging.info("Creating DB engine")
    print("Creating DB engine")

    pg_user = os.getenv("POSTGRES_USER")
    pg_pass = os.getenv("POSTGRES_PASSWORD")
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB")

    engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )
    return engine


def create_stg_table(engine: Engine, entity: str):
    logging.info("Creating tables if don't exist")
    print("Creating tables if don't exist")
    create_sql = ddl.get(entity)
    copy_sql = f"""
    CREATE TABLE IF NOT EXISTS {entity}_staging AS
    SELECT * FROM {entity}
    WHERE 1=2;
    """
    if create_sql:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
            conn.execute(text(copy_sql))


def create_log_table(engine: Engine):
    logging.info("Creating logs table if not exist")
    print("Creating logs table if not exist")
    create_sql = ddl.get("etl_logs")

    if create_sql:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
