# 652-group6-finalproject
# NOTE - REFER TO THE PROJECT WRITEUP FOR INSTRUCTIONS FIRST
# AND REFER TO OVERVIEW OF DEPLOYMENT HERE AS REQUIRED

A containerized data pipeline using Apache Airflow, PostgreSQL, and a Flask API. Currently configured to collect music data from Spotify, Kaggle, and LastFM.

## Architecture

The project runs five services via Docker Compose:

1. **PostgreSQL**
   - Port: 5432
   - Serves dual purpose:
     - Main data storage for pipeline data
     - Airflow metadata database (tracks DAG states, variables, connections)
   - Initialized via mounted `init.sql` script
   - Contains two databases:
     - `airflow`: Manages Airflow's backend
     - `group6`: Stores pipeline data

2. **Redis**
   - Port: 6379
   - Message broker for Airflow's Celery executor
   - Manages task queue for Airflow workers

3. **Apache Airflow**
   - Consists of multiple components:
     - **airflow-init**: One-time initialization service
       - Creates admin user
       - Sets up connections and variables
       - Runs database migrations
     - **airflow**: Main Airflow webserver and scheduler
       - Web UI port: 8081
       - Manages DAG scheduling and execution
     - **airflow-worker**: Celery workers that execute tasks
   - Custom image built from `Dockerfile.af`
   - Uses PostgreSQL as backend database
   - Uses Redis as message broker

4. **Flask API**
   - Port: 5001
   - REST API for data access
   - Custom image built from `Dockerfile.flask`

## Configuration

### Docker Setup
- `docker-compose.yml`: Service definitions and environment variables
- `init.sql`: Database initialization (users, databases, permissions)
- `Dockerfile.af`: Airflow container setup and dependencies
- `Dockerfile.flask`: API container configuration

### Airflow Directory Structure
```
airflow/
├── dags/                   # DAG definition files
│   └── sql/               # SQL scripts used by DAGs
├── logs/                   # Airflow task logs (created on run)
├── plugins/               # Custom plugins (if needed)
└── variables.json         # Airflow variables configuration
```

The `dags` folder is automatically mounted to the Airflow container. Any Python files in this directory will be parsed as DAGs.

### Airflow Variables
Create `airflow/variables.json` with your configuration:
```json
{
    "LASTFM_API_KEY": "your_api_key",
    "LASTFM_BASE_URL": "http://ws.audioscrobbler.com/2.0/",
    "OTHER_VAR": "other_value"
}
```
These variables will be automatically imported during Airflow initialization.

## Deployment

1. Start all services:
```bash
docker-compose up --build -d
```

2. Access Airflow UI:
   - URL: http://localhost:8081
   - Username: `group6`
   - Password: `group6`

3. Connect to PostgreSQL:
   - Using pgAdmin:
     - Host: `localhost` (or `host.docker.internal` on macOS)
     - Port: 5432
     - Databases: 
       - `airflow`: Airflow metadata
       - `group6`: Pipeline data
     - Username: group6 or airflow
     - Password: group6 or airflow

4. Shutdown:
```bash
# Stop and remove everything including volumes
docker-compose down -v

# Stop containers but preserve data
docker-compose down
```
