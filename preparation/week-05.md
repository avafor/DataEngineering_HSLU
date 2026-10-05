# Week 5 — Downloads before class

[All preparation checklists](README.md) · [Module homepage](../README.md)

Prepare Week 5 from its self-contained folder. Earlier weeks' environments are not required.

```sh
cd weeks/05-workflow-orchestration
cp .env.example .env
```

Keep an existing `.env` if you already created one. Open it and replace the example passwords and `TAXI_DATA_DIR` as explained in the [Week 5 overview](../weeks/05-workflow-orchestration/README.md).

Download the required Docker images and build the local Python image:

```sh
docker compose pull
docker pull python:3.13.11-slim-bookworm
docker build -t deng-week5-ingest:local .
```

Download the prepared taxi files listed in the [data folder instructions](../weeks/05-workflow-orchestration/data/README.md). Then check the configuration:

```sh
docker compose config --quiet
```

You do not need to start the containers before class.
