# Week 5 — Downloads before class

[Week 5 practical](README.md)

Everything for this practical is in `weeks/05-workflow-orchestration`. No earlier week's running containers, database, `.env`, or Python installation is required. You need Docker Desktop (or Docker Engine with Compose).

## 1. Start Docker and get the course files

Update your course files, preserving your own changes. Open Docker and check:

```sh
docker version
docker compose version
```

Docker should report both a Client and a Server. From the repository root:

```sh
cd weeks/05-workflow-orchestration
```

All remaining commands run in this folder.

## 2. Download the images

```sh
docker pull kestra/kestra:v1.1
docker pull postgres:18.6-bookworm
docker pull dpage/pgadmin4:9.17
docker pull python:3.13.11-slim-bookworm
```

Docker can reuse image downloads from previous weeks, but this week's configuration does not depend on their folders. Use these versions so everyone has the same classroom environment.

## 3. Build the Python image

```sh
docker build -t deng-week5-ingest:local .
```

This downloads Python and its libraries and packages the local loader. It does not start the course services or load records. It requires no `.env` file. Rebuild if the instructor updates the loader.

## 4. Download the taxi files

Save these files in **this week's `data/` folder**, preserving their filenames:

- [January 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet): `yellow_tripdata_2024-01.parquet`
- [February 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet): `yellow_tripdata_2024-02.parquet`

If you already downloaded these files, you may copy them here instead. No zone lookup file is needed for the current exercises. Part 3's small SQL sample is already included in `sql/prepare-trip-sample.sql`; it requires no separate download.

## 5. Check the downloads

```sh
docker image inspect kestra/kestra:v1.1 --format '{{.Id}}'
docker image inspect postgres:18.6-bookworm --format '{{.Id}}'
docker image inspect dpage/pgadmin4:9.17 --format '{{.Id}}'
docker image inspect python:3.13.11-slim-bookworm --format '{{.Id}}'
docker image inspect deng-week5-ingest:local --format '{{.Id}}'
```

Each command should print an image identifier. Confirm that both Parquet files are in `data/`.

You can now close Docker Desktop; start it again before class. You do not need to start services, configure accounts, create flows or run ingestion beforehand.
