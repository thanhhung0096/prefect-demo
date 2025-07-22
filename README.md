# Moodle Learning Activities ETL with Prefect

This project runs multiple learning activity queries (Assignment, Quiz, H5P, etc.) from a Moodle database in parallel using **Prefect**.

## Requirements

- Python 3.11
- PostgreSQL or MySQL with access to Moodle schema
- Prefect 3.x

## How to Run

### Install dependencies
```bash
pip install -r requirements.txt
```

### Start Prefect API Server (locally)
```
prefect server start
```

This launches:

- Prefect UI: http://127.0.0.1:4200

- Prefect API: http://127.0.0.1:4200/api

### Start a Prefect Worker (new terminal)

```bash
prefect worker start --pool 'moodle'
```

### Deploy the Flow

```
prefect deploy flows/moodle_learning_activities_flow.py:moodle_learning_activities_flow -n moodle-demo
```
