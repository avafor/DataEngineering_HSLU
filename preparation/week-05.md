# Week 5 — Downloads before class

[All preparation checklists](README.md) · [Module homepage](../README.md)

In Week 5, you will move from running individual Python and SQL commands to coordinating a complete data pipeline with Kestra. You will split the NYC Taxi pipeline into tasks, control their order, schedule monthly executions, recover missed months with backfills, retry temporary failures, and stop the pipeline when its source or output is invalid. Before class, think about these questions: Who should start a pipeline when nobody is watching? What should happen when one task fails after earlier tasks have succeeded? How can a rerun avoid loading duplicate records? How would you discover that a scheduled month was missed or produced no data? You do not need to prepare answers; bring your initial ideas to the session.

Prepare Week 5 from its self-contained folder. Earlier weeks' environments are not required. Complete every step below before class, but do not start the services yet.

## 1. Enter the Week 5 folder

```sh
cd weeks/05-workflow-orchestration
```

All remaining commands in this checklist run from this folder.

## 2. Create the local environment file

Copy the example file once:

```sh
cp .env.example .env
```

On Windows PowerShell, use:

```powershell
Copy-Item .env.example .env
```

If `.env` already exists, keep it and add any missing settings from `.env.example`.

Open `.env` and replace the four example passwords. The Kestra login password needs at least eight characters, including an uppercase letter, a lowercase letter and a number. `KESTRA_USER` is an email-format local login name.

| Settings | Used for |
|---|---|
| `KESTRA_USER`, `KESTRA_PASSWORD` | Signing into Kestra |
| `KESTRA_DB_PASSWORD` | Kestra connecting to its internal database |
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | Connecting to the taxi database |
| `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD` | Signing into pgAdmin |
| `TAXI_DATA_DIR` | The absolute path to this folder's `data/` directory |

Keep `.env` private. Git ignores it. Do not paste its contents into a question, commit it, or share its passwords.

## 3. Configure the data-folder path

Docker needs the absolute laptop path when Kestra starts a temporary task container and mounts the prepared files.

On macOS or Linux, find the path with:

```sh
cd data
pwd
cd ..
```

On Windows PowerShell, use:

```powershell
(Resolve-Path data).Path.Replace('\', '/')
```

Put the result in `.env`, for example:

```dotenv
TAXI_DATA_DIR=/Users/your-name/course-repo/weeks/05-workflow-orchestration/data
```

This is the only absolute path you need to configure.

## 4. Download images and build the Python image

Download the required Docker images and build the local Python image:

```sh
docker compose pull
docker pull python:3.13.11-slim-bookworm
docker build -t deng-week5-ingest:local .
```

The first two commands download images. The final command builds `deng-week5-ingest:local` from this folder's `Dockerfile`. That image contains the Python scripts and libraries that Kestra will run in temporary task containers.

## 5. Download the prepared taxi files

Follow the [data folder instructions](../weeks/05-workflow-orchestration/data/README.md). Confirm that the required files are inside `weeks/05-workflow-orchestration/data/`.

## 6. Validate the configuration

Run:

```sh
docker compose config --quiet
```

This checks that Compose can read `compose.yaml` and all required `.env` values. It does not start any services. No output means that the configuration is valid.

## If you used an earlier Week 5 setup

Keep the existing Week 5 `.env` and its Kestra passwords. Add any missing taxi-database and pgAdmin settings from `.env.example`. Set `PGADMIN_PORT=8086` and point `TAXI_DATA_DIR` to this week's `data` folder.

Download or copy the prepared Parquet files into that folder and rebuild the local image with:

```sh
docker build -t deng-week5-ingest:local .
```

The current Week 5 environment uses one self-contained Compose file. Do not start it together with an older Week 5 Compose configuration. Existing saved flows that reference another Python image must be updated during class from the current YAML files.

## Ready-to-attend checklist

- `.env` exists locally and is not tracked by Git.
- All example passwords in `.env` have been replaced.
- `TAXI_DATA_DIR` is the absolute path to this week's `data/` folder.
- The required Docker images have been downloaded.
- `deng-week5-ingest:local` has been built.
- The prepared taxi files are present in `data/`.
- `docker compose config --quiet` finishes without an error.
- You have not started the Week 5 services yet.

## If something fails

| Problem | What to check |
|---|---|
| A required setting is missing | Compare `.env` with `.env.example`. |
| Password validation fails | Check the Kestra password requirements above. |
| Docker cannot build or pull | Confirm Docker Desktop or your Docker engine is running. |
| File mount path is invalid | Recheck the absolute `TAXI_DATA_DIR` value. |
| Image is not found later | Rebuild it from this folder with `docker build -t deng-week5-ingest:local .`. |

When asking for help, include the command and complete error message. Never share passwords, tokens or the contents of `.env`.
