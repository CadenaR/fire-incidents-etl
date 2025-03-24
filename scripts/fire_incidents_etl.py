import os
from datetime import datetime
from dotenv import load_dotenv
import logging
from pathlib import Path

# Create a logs directory
Path("logs").mkdir(exist_ok=True)

from shared.data_types import entity_config
from shared.utils import get_postgres_engine, create_stg_table
from etl.extract import extract_api_incident_data
from etl.transform import transform_raw_data
from etl.load import load_stg_data

load_dotenv()

STG_PATH = os.getenv("STG_PATH")

def main():
    log_filename = f"logs/fire_etl_{datetime.today().strftime('%Y-%m-%d')}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info("Starting ETL")
    print("Starting ETL")
    
    try:
        todays_path = datetime.now().strftime("%Y/%m/%d")
        entity = "fire_incidents"
        date_column = "data_as_of"
        id_column = "id"
        api_row_limit = 100000
        fire_incident_schema = entity_config[entity]["schema"]
        
        engine = get_postgres_engine()
        
        create_stg_table(engine, entity)
        
        extract_api_incident_data(engine, entity, date_column, api_row_limit)

        transform_raw_data(entity, todays_path, date_column, fire_incident_schema)

        load_stg_data(engine, entity, todays_path, id_column, date_column)
    
    except Exception as e:
        logging.error("ETL failed", exc_info=True)


if __name__ == "__main__":
    main()