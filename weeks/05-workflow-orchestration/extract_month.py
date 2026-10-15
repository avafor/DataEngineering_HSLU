"""Download one monthly TLC file into the shared data directory."""
import argparse
from pathlib import Path
import shutil
from urllib.request import urlopen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, choices=range(1, 13), required=True)
    args = parser.parse_args()
    filename = f"yellow_tripdata_{args.year}-{args.month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    Path("/app/data").mkdir(parents=True, exist_ok=True)
    target = Path("/app/data") / "source.parquet"
    print(f"Downloading {url}")
    with urlopen(url, timeout=60) as response, target.open("wb") as output:
        shutil.copyfileobj(response, output)
    print(f"Saved {target}")


if __name__ == "__main__":
    main()
