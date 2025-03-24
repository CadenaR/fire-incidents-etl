import os
import pandas as pd
from dotenv import load_dotenv
import logging

from shared.data_types import entity_config
from sqlalchemy import text, Engine

load_dotenv()

STG_PATH = os.getenv("STG_PATH")


def load_stg_data(
    engine: Engine, entity: str, todays_path: str, id_columns: str, date_column: str
):
    logging.info("Starting data loading to DB")
    print("Starting data loading to DB")
    parquet_path = f"{STG_PATH}/{entity}/{todays_path}/{entity}.parquet"
    df = pd.read_parquet(parquet_path)
    logging.info("Parquet file read")
    print("Parquet file read")

    staging_table = f"{entity}_staging"

    logging.info("Loading data to staging table")
    print("Loading data to staging table")
    # We use append to preserve DB data types
    df.to_sql(staging_table, engine, if_exists="append", index=False)
    logging.info("Data loaded to staging table")
    print("Data loaded to staging table")

    update_columns = ",".join(
        [
            f"{column} = EXCLUDED.{column}"
            for column in entity_config[entity]["schema"].keys()
        ]
    )

    logging.info("Starting data merge")
    print("Starting data merge")
    merge_sql = f"""
    INSERT INTO {entity} AS target
    SELECT * FROM {staging_table}
    ON CONFLICT ({id_columns}) DO UPDATE SET
        {update_columns}
    WHERE EXCLUDED.{date_column} > target.{date_column}
    RETURNING {id_columns};
    """

    with engine.begin() as conn:
        result = conn.execute(text(merge_sql))
        merged_ids = result.fetchall()  
        merged_count = len(merged_ids)

        logging.info(f"{merged_count} record(s) merged into fire_incidents")
        print(f"{merged_count} record(s) merged into fire_incidents")

        conn.execute(text(f"DELETE FROM {staging_table}"))

