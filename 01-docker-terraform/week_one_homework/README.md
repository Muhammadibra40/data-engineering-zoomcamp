# Week 1 Homework — Docker & SQL (Data Engineering Zoomcamp)

This directory contains my solution for **Week 1** of the **Data Engineering Zoomcamp**, focusing on Docker fundamentals, PostgreSQL, and analytical SQL queries using the NYC Taxi dataset.

The goal of this homework is to:
- Run PostgreSQL using Docker Compose
- Analyze taxi trip data using SQL
- Validate results and explore data using a Jupyter notebook
- Maintain a reproducible Python environment

---

## Structure

```

.
├── docker-compose.yaml
├── analytical_queries.sql
├── notebook.ipynb
├── pyproject.toml
├── uv.lock
├── .python-version
└── README.md

````

### File Descriptions

- **`docker-compose.yaml`**  
  Defines and runs the PostgreSQL service using Docker.

- **`analytical_queries.sql`**  
  Contains SQL queries answering the Week 1 analytical questions.

- **`notebook.ipynb`**  
  Used for data exploration, validation, and supporting analysis.

- **`pyproject.toml`**  
  Python project configuration and dependency definitions.

- **`uv.lock`**  
  Locked dependency versions to ensure reproducible environments.

- **`.python-version`**  
  Specifies the Python version used for this project.

---

## Prerequisites

- Docker & Docker Compose
- Python (version specified in `.python-version`)
- Optional: `uv` package manager or `pip`

---

## Running PostgreSQL with Docker

From this directory, start the database:

```bash
docker compose up -d
````

To stop the services:

```bash
docker compose down
```

To stop and remove volumes (full reset):

```bash
docker compose down -v
```

---

## Connecting to the Database

Typical connection details (as defined in `docker-compose.yaml`):

* **Host**: `localhost`
* **Port**: `5432`
* **Database / User / Password**: defined in the compose file

If connecting from another Docker container, use the Postgres **service name** as the host.

---

## SQL Analysis

All homework queries are located in:

```text
analytical_queries.sql
```

These queries perform analytical checks such as:

* Row counts
* Aggregations
* Time-based analysis
* Data validation

They can be executed using:

* `psql`
* pgAdmin
* Any SQL client connected to PostgreSQL

---

## Jupyter Notebook

The notebook `notebook.ipynb` is used to:

* Explore the dataset
* Cross-check SQL results
* Perform lightweight analysis in Python

To run locally:

### Using uv

```bash
uv sync
jupyter notebook
```





