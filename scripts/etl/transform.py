import os
import pandas as pd
from pathlib import Path
import logging
from dotenv import load_dotenv
from shared.data_types import entity_config

load_dotenv()

LZ_PATH = os.getenv("LZ_PATH")
STG_PATH = os.getenv("STG_PATH")


def enforce_pandas_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    df = df.copy()
    
    for col, dtype in schema.items():
        if col in df.columns:
            if dtype.startswith("datetime"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception:
                    df[col] = pd.NA
    return df


def transform_raw_data(entity: str, todays_path: str, date_col: str, schema: dict):
    logging.info("Starting extraction")
    print("Starting extraction")
    json_files_path = f"{LZ_PATH}/{entity}/{todays_path}/"
    final_path = f"{STG_PATH}/{entity}/{todays_path}/{entity}.parquet"
    folder = Path(json_files_path)
    files = folder.glob("*.json.gz")

    df = pd.concat(
        [pd.read_json(f, lines=False, compression="gzip") for f in files],
        ignore_index=True,
    )
    logging.info("Dataframe created")
    print("Dataframe created")

    if entity == "fire_incidents":
        df["longitude"] = df["point"].apply(
            lambda p: p["coordinates"][0] if isinstance(p, dict) else None
        )
        df["latitude"] = df["point"].apply(
            lambda p: p["coordinates"][1] if isinstance(p, dict) else None
        )
        df.drop(columns=["point"], inplace=True)
        logging.info("Point column exploded")
        print("Point column exploded")

    df[date_col] = pd.to_datetime(df[date_col])

    df.sort_values(by=date_col, inplace=True)
    logging.info("Data sorted on date column")
    print("Data sorted on date column")

    df_deduped = df.drop_duplicates(subset="id", keep="last")
    logging.info("Duplicates dropped")
    print("Duplicates dropped")

    df_deduped = enforce_pandas_schema(df_deduped, schema)
    logging.info("Schema enforced")
    print("Schema enforced")

    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    df_deduped.to_parquet(final_path, compression="snappy")
    logging.info(f"Parquet file /{todays_path}/{entity}.parquet has been created")
    print(f"Parquet file /{todays_path}/{entity}.parquet has been created")


    
