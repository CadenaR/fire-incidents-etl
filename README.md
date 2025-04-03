# Fire Incidents ETL Pipeline

An ETL project that ingests, cleans, and models fire incident data using Python and PostgreSQL.

<p align="center">
  <img src="assets/fire_incidents.png" alt="Fire Incidents Logo"/>
</p>

This is a simple solution that works with fire incidents data from https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data
The intention of this ETL was to follow a medallion-alike architecture, having a Landing zone (bronze layer), Staging (silver layer) and Postgres DB containing merged data (golden layer)
The project works as follows:

## 1. Extraction 
Historical data is collected from the API and stored in raw format as a compressed json, this helps to have a track of all historical data in its original form in case something happens in the future in the other layers. Storing it in a compressed format, helps us to reduce storage space, which allows the project to be cost efficient.

## 2. Transformation
In this part, json data is read and an formatted into a defined structure, then it is deduplicated based on its `id` column and a schema enforcement is applied, to keep all data types consistent. Finally, it is loaded into the `staging` path as a parquet, which will allow us to also keep track of how data has been evolving.

## 3. Loading and DBT models
The last step, data loading, reads parquet file and stores that data into a `fire_incidents_staging` table. After that, dbt models get executed and data from `fire_incidents_staging` table goes into `fire_incidents_silver`, deduplicating data from `fire_incidents_staging` table, then data from `fire_incidents_silver` table goes into `fire_incidents`, deduplicating previous data (`fire_incidents`) with current data (`fire_incidents_silver`) and adding `is_active` column to handle deleted records.

*NOTE*: Either for `fire_incidents_silver` and `fire_incidents` have schema enforcement as dbt can't infer nor inherit schema from other existing tables with a defined schema.

## 4. Log System
You will see that each script has a log and a print toghether, this was designed so if you execute this project in the terminal, you can see the progress of each step, but also to keep track of all the executions that have been made in case this is orchestrated with a tool like Airflow, mage, dagster, etc.
For checking the logs, you can go to the `logs` folder and search for the date you want to review.

## Deduplication and Deletion Handling
Deduplication happens in three parts (step 3 also handles deletions):
1. When transforming data, deduplication happens at dataset level, by sorting data based on `date_as_of` table and taking `id` as the subset, taking only the last record (This will only preserve most recent records when they are duplicated).
2. When running dbt `fire_incidents_silver` model data gets another deduplication check by making a ROW_COUNT() over the whole data in `fire_incidents_staging`, sorting by `data_as_of` in descendant order so that only the most recent record gets into the silver table.
3. Finally, data from `fire_incidents_silver` table gets into `fire_incidents` table by making a full outer join, this helps us to avoid inserting duplicates, it merges data and also tracks records that have been deleted from the `fire_incidents` API DB by checking if incoming data (`fire_incidents_silver` aliased as `current`) is not anymore in the previous data (`fire_incidents` aliased as `previous`). When an existing record is not in the current data, a column called `is_active` will be set to `False`, which help us to soft delete records, hence we will have track of them in case it is needed.

## Sample Query
This query shows total fire incidents by year:

```sql
SELECT
  TO_CHAR(incident_date, 'YYYY') AS Year,
  COUNT(*) AS total_incidents
FROM fire_incidents
WHERE is_active = TRUE
GROUP BY 1
ORDER BY 1 DESC;
```

![image](https://github.com/user-attachments/assets/0ed0622c-dc5a-42d3-a3a3-696688748d55)


---

This project was built thinking on the feature, so hardcoding was avoided to let new etl pipelines for new datasets be included in an easy way, reusing the existing etl scripts.
Also, the structure of data folders was designed to work on a daily basis execution, so we can have track of the execution of each day, because of that, you will be able to see it like `data/landing/yyyy/mm/dd/` that way we allow the different scripts were to look each day, without harcoding any value.

---

## How to run the Pipeline

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/fire-incidents-etl.git
cd fire-incidents-etl
```

### 2. Create Virtual Environment

```bash
python -m venv fire_incidents_env
.\fire_incidents_env\Scripts\activate # On Mac: source fire_incidents_env/bin/activate 
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Create a `.env` file at the root based on the `.env.example` file

### 4. Start the PostgreSQL container
Make sure you update the `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB` to the credentials and DB name you want
```bash
docker-compose up -d
```

### 5. Set up DBT
Create profiles.yml file by running `~/.dbt/profiles.yml` in your bash and then write the following inside:

```
fire_incidents_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: <your-postgres-user>
      password: <your-postgres-password>
      port: 5432
      dbname: <your-postgres-database>
      schema: public
      threads: 1
```


### 6. Run the pipeline
```bash
python scripts/fire_incidents_etl.py
```
