# 652-group6-finalproject
# NOTE - REFER TO THE PROJECT WRITEUP FOR GENERAL INSTRUCTIONS FIRST
# AND REFER TO DETAILED INSTRUCTIONS HERE AS REQUIRED

# See the folder labeled documentation for writeup, ERD, and
# Example CSV's that you should expect to see generated from DAGs

# docker container notes
# If there are issues in docker desktop, then just make sure you're in project repo, and run
# docker-compose down -v
# docker-compose up --build -d
# Make sure your jhu_docker is off before you run the commands

# Recommended dag sequence - kaggle_dag, spotify_dag, and lasfm_brainz_dag in any order
# Then you can optionally run the matching_dag, but it's slow

A containerized data pipeline using Apache Airflow, PostgreSQL, and a Flask API. Currently configured to collect music data from LastFM's API.

## Architecture

The project runs three services via Docker Compose:

1. **PostgreSQL**
   - Port: 5432
   - Serves dual purpose:
     - Main data storage for pipeline data
     - Airflow metadata database (tracks DAG states, variables, connections)
   - Initialized via mounted `init.sql` script
   - Contains two databases:
     - `airflow`: Manages Airflow's backend
     - `group6`: Stores pipeline data

2. **Apache Airflow**
   - Web UI port: 8081
   - Manages data pipeline workflows
   - Custom image built from `Dockerfile.af`
   - Uses PostgreSQL as backend database

3. **Flask API**
   - Port: 5001
   - REST API for data access
   - Custom image built from `Dockerfile.flask`

## Configuration

### Docker Compose Quick Hits
Key components in `docker-compose.yml`:
```yaml
# Shared environment variables for Airflow services
x-common-env: &airflow-common-env
  AIRFLOW_HOME: /home/jhu/airflow
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow

services:
  postgres:
    # Healthcheck ensures database is ready before other services start
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
    volumes:
      # Persists database data
      - postgres_data:/var/lib/postgresql/data
      # Initializes database with users and permissions
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql

  airflow-init:
    # One-time setup service that:
    # - Initializes Airflow database
    # - Creates admin user
    # - Sets up connections
    depends_on:
      postgres:
        condition: service_healthy

  airflow:
    # Main Airflow service (webserver + scheduler)
    volumes:
      # Maps local DAGs to container
      - ./airflow:/home/jhu/airflow
    depends_on:
      # Ensures proper startup order
      postgres:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully
```

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
     - Password: group6 or airlow

4. Shutdown:
```bash
# Stop and remove everything including volumes
docker-compose down -v

# Stop containers but preserve data
docker-compose down
```

## Troubleshooting

If services fail to start properly:
1. Check container logs
2. Ensure ports aren't already in use
3. Try full reset:
```bash
docker-compose down -v
docker system prune -f
docker-compose up --build -d
```
