import os
import json
import gzip
import logging
from sodapy import Socrata
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text, Engine

load_dotenv()

API_DOMAIN = os.getenv("API_DOMAIN")
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = os.getenv("API_ENDPOINT")
LZ_PATH = os.getenv("LZ_PATH")


def get_latest_ts(engine: Engine, entity: str, search_column: str) -> str:
    query = text(f"SELECT MAX({search_column}) AS max_{entity} FROM {entity};")

    with engine.begin() as conn:
        result = conn.execute(query).fetchone()
        max_timestamp = result[0]

    if max_timestamp:
        formatted_ts = max_timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        logging.info(f"Max {entity}: {formatted_ts}")
        print(f"Max {entity}: {formatted_ts}")
        return formatted_ts
    else:
        logging.info(f"No data found in {entity}.")
        print(f"No data found in {entity}.")
        return ""


def get_incidents(
    client: Socrata, latest_ts: str, date_column: str, limit: int, offset: int
) -> list:
    if latest_ts:
        q_filter = f"{date_column} between '{latest_ts}' and '9999-01-10T00:00:00' order by {date_column} limit {limit} offset {offset}"
        return client.get(API_ENDPOINT, where=q_filter)

    return client.get(API_ENDPOINT, order=date_column, limit=limit, offset=offset)


def extract_api_incident_data(engine: Engine, entity: str, date_column: str, limit: int):
    offset = 0
    counter = 0

    client = Socrata(
        API_DOMAIN,
        API_KEY,
    )

    latest_ts = get_latest_ts(engine, entity, date_column)

    while True:
        new_incidents = []
        new_incidents = get_incidents(client, latest_ts, date_column, limit, offset)

        if not new_incidents:
            logging.info(f"{counter} records have been stored")
            print(f"{counter} records have been stored")
            break

        todays_path = datetime.now().strftime("%Y/%m/%d/%H%M%S")
        file_name = f"{todays_path}.json.gz"
        file_path = f"{LZ_PATH}/{entity}/{file_name}"

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if os.path.isfile(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    existing_incidents = json.load(f)
                    if not isinstance(existing_incidents, list):
                        existing_incidents = []
                except json.JSONDecodeError:
                    existing_incidents = []
        else:
            existing_incidents = []

        existing_incidents.extend(new_incidents)

        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            json.dump(existing_incidents, f)

        logging.info(f"File {file_name} has been stored")
        print(f"File {file_name} has been stored")

        counter += len(new_incidents)
        offset += limit