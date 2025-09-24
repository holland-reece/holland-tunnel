infra/                 # IaC lives here
  docker-compose.yml
  .env.example
airflow/               # DAGs & plugins
  dags/
  requirements.txt
dbt/                   # dbt Core project
  models/{staging,silver,gold,bi}
  profiles-template.yml
superset/              # seed + permissions (optional)
mlflow/                # local tracking store
expectations/          # Great Expectations project
scripts/               # ingestion & utilities
.github/workflows/ci.yml
README.md
