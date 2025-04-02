import os
import pandas as pd
from dotenv import load_dotenv
import logging

from shared.data_types import entity_config
from sqlalchemy import text, Engine

load_dotenv()

STG_PATH = os.getenv("STG_PATH")


def load_stg_data(engine: Engine, entity: str, todays_path: str):
    logging.info("Starting data loading to DB")
    print("Starting data loading to DB")
    parquet_path = f"{STG_PATH}/{entity}/{todays_path}/{entity}.parquet"
    df = pd.read_parquet(parquet_path)
    logging.info("Parquet file read")
    print("Parquet file read")

    staging_table = f"{entity}_staging"

    logging.info("Loading data to staging table")
    print("Loading data to staging table")
    df.to_sql(staging_table, engine, if_exists="replace", index=False)
    logging.info("Data loaded to staging table")
    print("Data loaded to staging table")