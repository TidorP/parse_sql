# Fluent Software Engineering Tech Test Tools

## `sql_runner.py`
Use this function to run a SQL snippet on BigQuery

### Setup
You will need to add a `.env` file with:
* `DEFAULT_DATASET` - the BigQuery dataset you want to run queries on
* `SERVICE_ACCOUNT_JSON` - the JSON key for the BigQuery service account that has permissions to run jobs

Install dependencies with poetry:
1. Run `poetry install`

### Usage

Either run with `poetry run python sql_runner.py`, or import into your code
