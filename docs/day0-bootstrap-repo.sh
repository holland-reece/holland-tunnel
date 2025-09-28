airflow/               # DAGs & plugins
  dags/
  docker-compose.yml
  .env.example
dbt/                   # dbt Core project
  models/{staging,silver,gold,bi}
  profiles-template.yml
superset/              # seed + permissions (optional)
mlflow/                # local tracking store
expectations/          # Great Expectations project
scripts/               # ingestion & utilities
docs/
  requirements.txt
README.md
