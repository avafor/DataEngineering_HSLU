# Week 5 taxi files

Save `yellow_tripdata_2024-01.parquet` and `yellow_tripdata_2024-02.parquet` here before class. Parquet files are ignored by Git.

- [January 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet)
- [February 2024](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet)

The loader reads these prepared files. If a requested file is absent, it downloads it temporarily for that execution.

The split-task ETL writes `source.parquet` and `transformed.csv` directly into this folder. These are ignored by Git. Run one split-task execution at a time and recover any failed Load before another run overwrites these files. `load_ready` and `retry_ready` are empty exercise markers, not data files.
