# Maintainer checks

These tests use tiny synthetic fixtures to check hashing, reruns, missing values,
validation, rollback, monthly replacement and repeatable sample setup. They require
an isolated database named `week5_review`; never point them at a teaching database.

From the Week 5 folder, with Docker running:

```sh
docker build -t deng-week5-review:local .
docker run -d --rm --name deng-week5-review-db --tmpfs /var/lib/postgresql -e POSTGRES_DB=week5_review -e POSTGRES_HOST_AUTH_METHOD=trust postgres:18.6-bookworm
docker exec deng-week5-review-db pg_isready -U postgres -d week5_review
```

Wait until the last command reports that connections are accepted, then run:

```sh
docker run --rm --network container:deng-week5-review-db -e TEST_DATABASE_URL=postgresql+psycopg://postgres@localhost:5432/week5_review --mount "type=bind,source=$PWD,target=/course,readonly" deng-week5-review:local /course/tests/test_etl.py
docker stop deng-week5-review-db
```

The review database has no published port and no persistent data volume. Stopping
it removes it. The tests do not validate Kestra's scheduler, restart UI or a full
monthly download; those need a separate execution check in Kestra.
