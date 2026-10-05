# Week 4 — Downloads before class

[All preparation checklists](README.md) · [Week 4 materials](../weeks/04-transformation-and-serving/README.md)

In Week 4, you will turn ingested records into data that a consumer can understand and use. You will inspect data-quality problems, define transformation rules, enrich taxi trips with zone names, create a daily report, and protect fictional customer identifiers through tokenization and database permissions. Before class, think about these questions: What makes a source record suitable for a report? When should a suspicious value be removed, corrected, or only flagged? Why might joining a lookup table accidentally hide records? Which identifiers should an analyst be allowed to see, and how could the system still recognize the same customer without exposing an email address? You do not need to prepare answers; bring your initial ideas to the session.

Prepare these files and the Python image before travelling to class. This checklist does not require loading data, writing SQL, or completing the week's exercises.

## 1. Keep the Week 2 environment

Keep Docker, the PostgreSQL and pgAdmin images, your `.env`, and the January taxi file from [Week 2 preparation](week-02.md). Keep the existing database volume too. Do not run `docker compose down -v`: it removes the saved lab database.

If you missed Week 2, complete its downloads checklist now. Week 4 uses at least one complete month from Week 2 Part 3, Step 7. Keep the downloaded January file so that loading it requires no additional download. The loading activity stays in the practical instructions; it is not part of this preparation checklist.

## 2. Get the current course files

Update your course copy using your usual Git workflow, preserving your own changes. Check that it contains:

- `examples/nyc-taxi/load_zones.py`
- `examples/nyc-taxi/sql/week4/01-zones.sql` through `06-check-access.sql`
- `weeks/04-transformation-and-serving/README.md`

The fictional customer and booking records are included in `05-tokenization.sql`; there is no separate customer dataset to download.

## 3. Download the taxi-zone lookup

Download the [official Taxi Zone Lookup CSV](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv), linked on the [TLC data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Save it as:

```text
examples/nyc-taxi/data/taxi_zone_lookup.csv
```

If your browser displays the CSV, use Save As and preserve the `.csv` extension. Check that the download has finished and the file is not empty. Its first line should contain `LocationID`, `Borough`, `Zone`, and `service_zone`. Do not rename these CSV columns or edit the values.

## 4. Build the updated Python image

Start Docker. Open a terminal in `examples/nyc-taxi`, then run:

```sh
docker compose build ingest
```

This prepares an image containing the new zone loader. It does not start PostgreSQL or load records. The existing dependencies are reused; no new Python package is needed for Week 4.

Check that the script is available without starting the database:

```sh
docker compose run --rm --no-deps ingest load_zones.py --help
```

Expect usage instructions mentioning a file path. `--no-deps` skips dependent services, and `--help` exits before connecting to PostgreSQL.

## Ready-to-attend checklist

- Docker starts and the Week 2 images are available.
- The Week 2 taxi file and the new zone CSV are in `examples/nyc-taxi/data`.
- The updated image built successfully and the help command worked.
- The six Week 4 SQL files are available locally.

No additional cloud account, service, or software installation is required. Recheck this file when a course update is announced.
