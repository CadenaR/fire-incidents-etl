# Fire Incidents ETL Pipeline

An ETL project that ingests, cleans, and models fire incident data using Python and PostgreSQL.

<p align="center">
  <img src="assets/fire_incidents.png" alt="Fire Incidents Logo" width="300"/>
</p>


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

### 5. Run the pipeline
```bash
python scripts/fire_incidents_etl.py
```
