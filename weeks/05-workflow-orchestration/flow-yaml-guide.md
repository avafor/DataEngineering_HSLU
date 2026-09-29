# Reading the taxi ingestion flow

This guide explains the important parts of [04-load-taxi-month-starter.yaml](flows/04-load-taxi-month-starter.yaml). The flow tells Kestra how to start the Python loader for one selected month.

## The task and its command

```yaml
- id: ingest_month
  type: io.kestra.plugin.scripts.shell.Commands
  commands:
    - cd /app
    - python ingest_months.py --year {{ inputs.year }} --months {{ inputs.month }}
```

`id` names this step inside the flow. `type` selects Kestra's shell-command task. The commands are the work to perform. Kestra replaces `{{ inputs.year }}` and `{{ inputs.month }}` with the values selected for this execution.

The command runs `ingest_months.py`; it does not define the Python program. The program is already packaged in the Docker image described below.

## `containerImage`: what environment runs the command?

```yaml
containerImage: deng-week5-ingest:local
```

This is the local Docker image created from Week 5's `Dockerfile`:

```sh
docker build -t deng-week5-ingest:local .
```

The image contains Python, the required libraries and the course scripts. The name has an image name (`deng-week5-ingest`) and a tag (`local`). It is not a service, a network or a database.

## `taskRunner`: how does Kestra run the task?

```yaml
taskRunner:
  type: io.kestra.plugin.scripts.runner.docker.Docker
```

`taskRunner` chooses the mechanism used to execute the task. This Docker runner asks Docker to create a temporary container from `containerImage`.

The temporary task container is alongside the Kestra container. It is not a container inside Kestra:

```text
Docker on the student's computer
├── Kestra container
├── PostgreSQL container
└── temporary Python task container
    created from deng-week5-ingest:local
```

Kestra coordinates the task; the temporary container runs the Python command.

### Where is the container created?

The container is created when you click **Execute** in Kestra and the `ingest_month` task starts. It is not created when you build the image or when `docker compose up -d` starts the four permanent services.

Week 5's `compose.yaml` mounts Docker's API socket into the Kestra container:

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

The Docker task runner uses that socket to ask the Docker engine on the student's computer to create a container from `deng-week5-ingest:local`:

```text
Kestra task starts
      ↓
Kestra Docker runner uses the Docker socket
      ↓
Docker creates a container from deng-week5-ingest:local
      ↓
The command runs inside that container
```

There is no separate `docker run` line in our flow. The Kestra Docker runner makes the request through Docker's API. When the task finishes, the temporary container stops and is removed; the image and PostgreSQL data remain.

## `pullPolicy`: should Kestra download the image?

```yaml
pullPolicy: NEVER
```

`NEVER` means use the image already stored locally. It does not download an image from a registry. Students must build the image first. If the image is missing, the task fails with an image-not-found error.

Other policies can download images, but `NEVER` makes this local dependency explicit and avoids an accidental registry download.

## `networkMode`: how can Python reach PostgreSQL?

```yaml
networkMode: "{{ envs.taxi_network }}"
```

Containers are isolated unless they share a Docker network. Week 5's Compose file names its network `deng-week5` and gives Kestra this value through:

```yaml
ENV_TAXI_NETWORK: deng-week5
```

When the flow runs, the temporary Python container joins `deng-week5`, the same network as the Week 5 PostgreSQL container.

## Database connection settings

```yaml
env:
  POSTGRES_HOST: postgres
  POSTGRES_DB: "{{ envs.taxi_db }}"
  POSTGRES_USER: "{{ envs.taxi_user }}"
  POSTGRES_PASSWORD: "{{ envs.taxi_password }}"
```

These values become environment variables inside the temporary Python container. The Python script reads them to connect to PostgreSQL.

`postgres` is the Compose service name. Docker's internal DNS resolves it to the PostgreSQL container on the shared network. Port `5432` is PostgreSQL's default internal port; it does not need to be published on the student's computer for this container-to-container connection.

The values named `envs.taxi_db`, `envs.taxi_user` and so on are supplied to Kestra by the Week 5 Compose file. They are different from the flow inputs: inputs select the data period, while environment variables configure the connection.

## `volumes`: how can Python read the prepared file?

```yaml
volumes:
  - "{{ envs.taxi_data_dir }}:/app/data:ro"
```

This maps the student's local Week 5 `data/` folder to `/app/data` inside the temporary container.

```text
Laptop: weeks/05-workflow-orchestration/data
                 │ read-only mount
                 ▼
Task container: /app/data
```

`ro` means read-only. The loader can read a prepared Parquet file but cannot change or delete the student's local files. `TAXI_DATA_DIR` supplies the absolute laptop path used on the left side of the mount.

## `entryPoint`: why is it empty?

The image's Dockerfile contains:

```dockerfile
ENTRYPOINT ["python"]
```

The flow uses:

```yaml
entryPoint: []
```

This clears the image's default `python` entrypoint so Kestra's shell runner can launch its generated command script directly. Without this override, the image's default `python` command could be combined with the shell runner's command in the wrong way.

## The complete execution path

```text
1. Student builds deng-week5-ingest:local
2. Student starts the four Week 5 Compose services
3. Student executes the flow in Kestra
4. Kestra asks Docker for a temporary task container
5. Docker starts it from deng-week5-ingest:local
6. The container joins deng-week5 and receives the database settings
7. /app/data is mounted read-only
8. Python runs ingest_months.py
9. Python connects to postgres:5432 and loads the selected month
10. The task exits; PostgreSQL keeps the committed data
```

`docker compose up -d` starts the supporting services. It does not run this ingestion task. The Python task container is created only when the flow is executed.
