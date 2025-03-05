# 652-group6

- Flask API: `localhost:5001`
  - ./flask_api/app.py
  - Dockerfile.flask   
- Airflow: `localhost:8081` (user: `group6`, password: `group6`)
  - ./airflow
  - Dockerfile.af
- Postgres: `localhost:5433` (user: `group6`, password: `group6`, db: `group6`)

### To run 
- pull this repo
- run `docker-compose up --build -d`
- for airflow UI go to http://localhost:8081 and login using username and pw
- for flask api post or get request from http://localhost:5001/ 
