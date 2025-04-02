import os
import json
import gzip
import logging
from sodapy import Socrata
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_DOMAIN = os.getenv("API_DOMAIN")
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = os.getenv("API_ENDPOINT")
LZ_PATH = os.getenv("LZ_PATH")


def extract_api_incident_data(entity: str, date_column: str, limit: int):
    offset = 0
    counter = 0

    client = Socrata(
        API_DOMAIN,
        API_KEY,
    )

    while True:
        new_incidents = []
        new_incidents = client.get(
            API_ENDPOINT, order=date_column, limit=limit, offset=offset
        )

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
